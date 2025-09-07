# The export-shards architecture

The `export-shards` RPC command creates Arrow IPC files that are partitioned into neuroglancer shards, 
where each DVID block is written as an Arrow record while traversing the segmentation database in ZYX 
order. Since TensorStore may request blocks (chunks) in a non-ZYX Morton order, we need to use a shard file 
format optimized for fast, random-access record reads.

For a large dataset like the segmentation for male CNS, the result are tens of thousands of shard files and
accompanying JSON chunk index files. The total data will exceed 1TB after using zstd compression on the
already compressed DVID segmentation.

## Code Architecture

The functionality is contained in `datatype/labelmap/export.go`. It basically has three layers of goroutines: 

1. `readBlocksZYX` is a function that as quickly as possible scans the embedded key-value 
database in native ZYX key order for potentially multiple scales and sends data to a buffered 
`chunkCh` channel

2. a layer of 100 `chunkHandler` goroutines that all consume from that single buffered `chunkCh`, 
deserializes &  uncompresses the gzip DVID segmentation block data, computes a shard ID, and 
then either reuses or starts a `shardWriter` goroutine and sends the block data

3. a layer of shard-specific goroutines that are launched within a `shardWriter.start` function, 
reading from its buffered shardWriter channel, adding data for agglomerated labels from the 
in-memory versioned label mapping system, and then converting the block data to an Arrow 
record which is written to an Arrow IPC file as well as a sidecar JSON chunk index file  
that gives chunk coordinates and Arrow record numbers.

## Shard Arrow File Formats

Each shard consists of two files:

- **`{origin}.arrow`**: Standard Apache Arrow IPC format containing block records
- **`{origin}.json`**: JSON mapping of chunk coordinates to Arrow record indices

For example, a shard starting at origin (0,0,0) would have files:
- `0_0_0.arrow` - Arrow IPC data
- `0_0_0.json` - Chunk coordinate index

## Arrow IPC File Format

The implementation uses the **Arrow IPC File Format** (Feather V2) for the main data, which provides:
- Built-in random access capabilities
- Footer with byte offsets and lengths for every record batch
- Standard compatibility with Arrow libraries

## Record Structure

Each DVID block is stored as a single Arrow record. The number of records per shard depends on the 
shard size and chunk size configuration.

## Chunk Index Format

The chunk index is stored as a separate JSON file with the same base name as the Arrow file but with `.json` extension. It contains a JSON object mapping coordinate strings to Arrow record indices:

**Example: `0_0_0.json`**
```json
{
  "0_0_0": 0,
  "0_0_64": 1,
  "0_64_0": 2,
  "64_0_0": 3
}
```

Keys are formatted as `"{x}_{y}_{z}"` where x, y, z are chunk coordinates in voxel space.

## Arrow Schema

Each Arrow record contains the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `chunk_x` | int32 | X coordinate of the chunk |
| `chunk_y` | int32 | Y coordinate of the chunk |
| `chunk_z` | int32 | Z coordinate of the chunk |
| `labels` | list\<uint64\> | Agglomerated label IDs |
| `supervoxels` | list\<uint64\> | Original supervoxel IDs |
| `dvid_compressed_block` | binary | DVID compressed block data |

```python
import pyarrow as pa

SCHEMA = pa.schema([
    pa.field('chunk_x', pa.int32(), nullable=False),
    pa.field('chunk_y', pa.int32(), nullable=False),
    pa.field('chunk_z', pa.int32(), nullable=False),
    pa.field('labels', pa.list_(pa.uint64()), nullable=False),
    pa.field('supervoxels', pa.list_(pa.uint64()), nullable=False),
    pa.field('dvid_compressed_block', pa.binary(), nullable=False)
])
```

## Python Reading Utilities

### Basic Reader

```python
import json
import os
import pyarrow as pa

def read_shard_with_index(arrow_filename):
    """
    Read Arrow shard file with chunk index from separate JSON file.
    
    Args:
        arrow_filename: Path to the .arrow file
    
    Returns:
        tuple: (pyarrow.Table, dict) - Arrow table and chunk coordinate index
    """
    # Read the Arrow data
    with pa.ipc.open_file(arrow_filename) as reader:
        table = reader.read_all()
    
    # Read the chunk index from corresponding JSON file
    json_filename = os.path.splitext(arrow_filename)[0] + '.json'
    chunk_index = {}
    if os.path.exists(json_filename):
        with open(json_filename, 'r') as f:
            chunk_index = json.load(f)
    
    return table, chunk_index
```

### Usage Example

```python
# Read shard file and its index
table, chunk_index = read_shard_with_index('s0/0_0_0.arrow')

# The chunk index was loaded from s0/0_0_0.json
print(f"Shard contains {len(chunk_index)} chunks")

# Access specific chunk by coordinates
chunk_coord = "64_64_0"
if chunk_coord in chunk_index:
    record_idx = chunk_index[chunk_coord]
    
    # Get the specific record
    chunk_x = table['chunk_x'][record_idx].as_py()
    chunk_y = table['chunk_y'][record_idx].as_py()  
    chunk_z = table['chunk_z'][record_idx].as_py()
    labels = table['labels'][record_idx].as_py()
    supervoxels = table['supervoxels'][record_idx].as_py()
    compressed_data = table['dvid_compressed_block'][record_idx].as_py()
    
    print(f"Chunk ({chunk_x}, {chunk_y}, {chunk_z})")
    print(f"Labels: {labels}")
    print(f"Supervoxels: {supervoxels}")
```

### Advanced Reader Class

```python
class ArrowShardReader:
    def __init__(self, arrow_filename):
        self.arrow_filename = arrow_filename
        self.table, self.chunk_index = read_shard_with_index(arrow_filename)
    
    def get_chunk(self, x, y, z):
        """Get chunk data by coordinates."""
        coord_key = f"{x}_{y}_{z}"
        if coord_key not in self.chunk_index:
            return None
            
        record_idx = self.chunk_index[coord_key]
        return {
            'coordinates': (x, y, z),
            'labels': self.table['labels'][record_idx].as_py(),
            'supervoxels': self.table['supervoxels'][record_idx].as_py(),
            'compressed_data': self.table['dvid_compressed_block'][record_idx].as_py()
        }
    
    def list_chunks(self):
        """List all available chunk coordinates."""
        return [(int(x), int(y), int(z)) 
                for coord_str in self.chunk_index.keys()
                for x, y, z in [coord_str.split('_')]]
    
    def get_labels_for_chunk(self, x, y, z):
        """Get just the labels for a specific chunk."""
        chunk = self.get_chunk(x, y, z)
        return chunk['labels'] if chunk else None

# Usage
reader = ArrowShardReader('s0/0_0_0.arrow')
chunks = reader.list_chunks()
labels = reader.get_labels_for_chunk(64, 64, 0)
```

## Notes

- The Arrow IPC data can be read by any standard Arrow library
- The separate JSON chunk index provides O(1) lookup for specific spatial coordinates
- Multiple chunks can be efficiently accessed from a single shard file  
- The chunk index JSON file is human-readable for debugging and inspection
- Standard Arrow tools can process the .arrow files without needing to understand the indexing


# Arrow Shard File Format

The `export-shards` RPC command creates Arrow IPC files that are partitioned into neuroglancer shards, 
where each DVID block is written as an Arrow record while traversing the segmentation database in ZYX 
order. Since TensorStore may request blocks (chunks) in a non-ZYX Morton order, we need to use a shard file 
format optimized for fast, random-access record reads.

## File Format

Each shard file uses the following format:

```
[Standard Arrow IPC Data][JSON Chunk Index][8-byte Length][8-byte Magic]
```

- **Arrow IPC Data**: Standard Apache Arrow IPC format containing block records
- **JSON Chunk Index**: JSON mapping of chunk coordinates to Arrow record indices  
- **8-byte Length**: Little-endian uint64 indicating length of JSON data
- **8-byte Magic**: ASCII string "CHUNKIDX" identifying the index footer

## Arrow IPC File Format

The implementation uses the **Arrow IPC File Format** (Feather V2) for the main data, which provides:
- Built-in random access capabilities
- Footer with byte offsets and lengths for every record batch
- Standard compatibility with Arrow libraries

## Record Structure

Each DVID block is stored as a single Arrow record. The number of records per shard depends on the 
shard size and chunk size configuration.

## Chunk Index Format

The chunk index is a JSON object mapping coordinate strings to Arrow record indices:

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
import struct
import pyarrow as pa

def read_shard_with_index(filename):
    """
    Read Arrow shard file with chunk index.
    
    Returns:
        tuple: (pyarrow.Table, dict) - Arrow table and chunk coordinate index
    """
    # First, read the standard Arrow data
    with pa.ipc.open_file(filename) as reader:
        table = reader.read_all()
    
    # Then read the chunk index from custom footer
    with open(filename, 'rb') as f:
        f.seek(-16, 2)  # Last 16 bytes
        magic = f.read(8)
        if magic == b'CHUNKIDX':
            length_bytes = f.read(8)
            length = struct.unpack('<Q', length_bytes)[0]
            f.seek(-(16 + length), 2)
            index_json = f.read(length)
            chunk_index = json.loads(index_json)
            return table, chunk_index
    
    return table, {}
```

### Usage Example

```python
# Read shard file
table, chunk_index = read_shard_with_index('s0/0_0_0.arrow')

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
    def __init__(self, filename):
        self.filename = filename
        self.table, self.chunk_index = read_shard_with_index(filename)
    
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
- The chunk index provides O(1) lookup for specific spatial coordinates
- Multiple chunks can be efficiently accessed from a single shard file
- The format maintains backward compatibility with standard Arrow readers (they'll just ignore the footer)


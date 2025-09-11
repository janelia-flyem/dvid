# The export-shards architecture

The `export-shards` RPC command creates Arrow IPC files that are partitioned into neuroglancer shards, 
where each DVID block is written as an Arrow record while traversing the segmentation database in ZYX 
order. Since TensorStore may request blocks (chunks) in a non-ZYX Morton order, we need to use a shard file 
format optimized for fast, random-access record reads.

For a large dataset like the segmentation for male CNS, the result are tens of thousands of shard files and
accompanying CSV chunk index files. The total data will exceed 1TB after using zstd compression on the
already compressed DVID segmentation.

## Code Architecture

The functionality is contained in `datatype/labelmap/export.go`. It basically has three layers of goroutines: 

1. `readBlocksZYX` is a function that as quickly as possible scans the embedded key-value 
database in native ZYX key order (potentially across multiple scales) and sends data to a buffered 
`chunkCh` channel

2. a layer of 50 `chunkHandler` goroutines that all consume from that single buffered `chunkCh`, 
deserializes &  uncompresses the gzip DVID segmentation block data, computes a shard ID, and 
then either reuses or starts a `shardWriter` goroutine and sends the block data

3. a layer of shard-specific goroutines that are launched within a `shardWriter.start` function, 
reading from its buffered shardWriter channel, adding data for agglomerated labels from the 
in-memory versioned label mapping system, and then converting the block data to an Arrow 
record which is written to an Arrow IPC file as well as a sidecar CSV chunk index file  
that gives chunk coordinates and Arrow record numbers.

## Shard Arrow File Formats

Each shard consists of two files:

- **`{origin}.arrow`**: Standard Apache Arrow IPC format containing block records
- **`{origin}.csv`**: CSV mapping of chunk coordinates to Arrow record indices

For example, a shard starting at origin (0,0,0) would have files:
- `0_0_0.arrow` - Arrow IPC data
- `0_0_0.csv` - Chunk coordinate index

## Arrow IPC File Format

The implementation uses the **Arrow IPC File Format** (Feather V2) for the main data, which provides:
- Built-in random access capabilities
- Footer with byte offsets and lengths for every record batch
- Standard compatibility with Arrow libraries

## Record Structure

Each DVID block is stored as a single Arrow record. The number of records per shard depends on the 
shard size and chunk size configuration.

## Chunk Index Format

The chunk index is stored as a separate CSV file with the same base name as the Arrow file but with `.csv` extension. It contains rows mapping chunk coordinates to Arrow record indices:

**Example: `0_0_0.csv`**
```csv
x,y,z,rec
1,1,0,1
2,1,0,2
2,2,1,3
```

## Arrow Schema

Each Arrow record contains the following fields:

| Field | Type | Description |
|-------|------|-------------|
| `chunk_x` | int32 | X coordinate of the chunk |
| `chunk_y` | int32 | Y coordinate of the chunk |
| `chunk_z` | int32 | Z coordinate of the chunk |
| `labels` | list\<uint64\> | Agglomerated label IDs |
| `supervoxels` | list\<uint64\> | Original supervoxel IDs |
| `dvid_compressed_block` | binary | DVID compressed block data (zstd) |
| `uncompressed_size` | uint32 | Size of block data before compression |

```python
import pyarrow as pa

SCHEMA = pa.schema([
    pa.field('chunk_x', pa.int32(), nullable=False),
    pa.field('chunk_y', pa.int32(), nullable=False),
    pa.field('chunk_z', pa.int32(), nullable=False),
    pa.field('labels', pa.list_(pa.uint64()), nullable=False),
    pa.field('supervoxels', pa.list_(pa.uint64()), nullable=False),
    pa.field('dvid_compressed_block', pa.binary(), nullable=False),
    pa.field('uncompressed_size', pa.uint32(), nullable=False)
])
```

## Python Reading Utilities

### Basic Reader

```python
import csv
import os
import pyarrow as pa

def read_shard_with_index(arrow_filename):
    """
    Read Arrow shard file with chunk index from separate CSV file.
    
    Args:
        arrow_filename: Path to the .arrow file
    
    Returns:
        tuple: (pyarrow.Table, dict) - Arrow table and chunk coordinate index
    """
    # Read the Arrow data
    with pa.ipc.open_file(arrow_filename) as reader:
        table = reader.read_all()
    
    # Read the chunk index from corresponding CSV file
    csv_filename = os.path.splitext(arrow_filename)[0] + '.csv'
    chunk_index = {}
    if os.path.exists(csv_filename):
        with open(csv_filename, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Use tuple of coordinates as key for efficient lookup by TensorStore
                coord_key = (int(row['x']), int(row['y']), int(row['z']))
                chunk_index[coord_key] = int(row['rec'])
    
    return table, chunk_index
```

### Usage Example

```python
# Read shard file and its index
table, chunk_index = read_shard_with_index('s0/0_0_0.arrow')

# The chunk index was loaded from s0/0_0_0.csv
print(f"Shard contains {len(chunk_index)} chunks")

# Access specific chunk by coordinates (using tuple for efficient lookup)
chunk_coord = (64, 64, 0)
if chunk_coord in chunk_index:
    record_idx = chunk_index[chunk_coord]
    
    # Get the specific record
    chunk_x = table['chunk_x'][record_idx].as_py()
    chunk_y = table['chunk_y'][record_idx].as_py()  
    chunk_z = table['chunk_z'][record_idx].as_py()
    labels = table['labels'][record_idx].as_py()
    supervoxels = table['supervoxels'][record_idx].as_py()
    compressed_data = table['dvid_compressed_block'][record_idx].as_py()
    uncompressed_size = table['uncompressed_size'][record_idx].as_py()
    
    print(f"Chunk ({chunk_x}, {chunk_y}, {chunk_z})")
    print(f"Labels: {labels}")
    print(f"Supervoxels: {supervoxels}")
    print(f"Compressed size: {len(compressed_data)} bytes, uncompressed: {uncompressed_size} bytes")
```

### Advanced Reader Class

```python
class ArrowShardReader:
    def __init__(self, arrow_filename):
        self.arrow_filename = arrow_filename
        self.table, self.chunk_index = read_shard_with_index(arrow_filename)
    
    def get_chunk(self, x, y, z):
        """Get chunk data by coordinates."""
        coord_key = (x, y, z)
        if coord_key not in self.chunk_index:
            return None
            
        record_idx = self.chunk_index[coord_key]
        return {
            'coordinates': (x, y, z),
            'labels': self.table['labels'][record_idx].as_py(),
            'supervoxels': self.table['supervoxels'][record_idx].as_py(),
            'compressed_data': self.table['dvid_compressed_block'][record_idx].as_py(),
            'uncompressed_size': self.table['uncompressed_size'][record_idx].as_py()
        }
    
    def list_chunks(self):
        """List all available chunk coordinates."""
        return list(self.chunk_index.keys())
    
    def get_labels_for_chunk(self, x, y, z):
        """Get just the labels for a specific chunk."""
        chunk = self.get_chunk(x, y, z)
        return chunk['labels'] if chunk else None

# Usage
reader = ArrowShardReader('s0/0_0_0.arrow')
chunks = reader.list_chunks()
labels = reader.get_labels_for_chunk(64, 64, 0)
```

### TensorStore Integration Example

The tuple-based coordinate indexing integrates seamlessly with TensorStore's virtual_chunked driver:

```python
import tensorstore as ts
import pandas as pd

def create_tensorstore_reader(base_path):
    """Create a TensorStore virtual chunked reader for the shard data."""
    
    # Load all CSV indices into a single mapping using pandas for efficiency
    chunk_mapping = {}
    arrow_files = {}
    
    # Scan for all shard files
    import glob
    for arrow_path in glob.glob(f"{base_path}/**/*.arrow", recursive=True):
        table, index = read_shard_with_index(arrow_path)
        arrow_files[arrow_path] = table
        chunk_mapping.update(index)
    
    def read_chunk(output_array, read_params):
        """Read function called by TensorStore for each chunk request."""
        domain = output_array.domain()
        # domain.origin() gives global coordinates as tuple - perfect for our index!
        global_coords = tuple(domain.origin())
        
        record_idx = chunk_mapping.get(global_coords)
        if record_idx is not None:
            # Find which shard file contains this record
            # (In practice, you'd optimize this lookup)
            for arrow_path, table in arrow_files.items():
                if record_idx < table.num_rows:
                    # Extract compressed block data and decode
                    compressed_data = table['dvid_compressed_block'][record_idx].as_py()
                    # Decode DVID block format and fill output_array
                    # ... (DVID decompression logic here)
                    break
        
        return ts.TimestampedStorageGeneration()
    
    # Create virtual chunked TensorStore
    return ts.VirtualChunked(
        read_function=read_chunk,
        dtype=ts.uint64,  # assuming label data
        domain=ts.IndexDomain(...),  # your volume domain
        chunk_layout=ts.ChunkLayout(chunk_shape=[64, 64, 64]),  # DVID chunk size
    )

# Usage
store = create_tensorstore_reader("exported_shards/")
# Now you can read data: store[100:164, 200:264, 300:364]
```

## Notes

- The Arrow IPC data can be read by any standard Arrow library
- The CSV chunk index provides O(1) tuple-based lookup perfect for TensorStore's coordinate system
- Multiple chunks can be efficiently accessed from a single shard file  
- The chunk index CSV file is human-readable for debugging and inspection
- Standard Arrow tools can process the .arrow files without needing to understand the indexing
- Global chunk coordinates in the CSV eliminate the need for coordinate translation in TensorStore


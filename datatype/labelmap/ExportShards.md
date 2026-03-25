# The export-shards architecture

The `export-shards` RPC command creates Arrow IPC streaming files that are partitioned into neuroglancer
shards, where DVID blocks are batched into Arrow record batches while traversing the segmentation
database in ZYX order. A companion CSV file provides chunk coordinates, byte offsets, and batch
positions for efficient random access via GCS range reads.

For a large dataset like the segmentation for male CNS, the result is tens of thousands of shard files
and accompanying CSV index files. The total data will exceed 1TB after using zstd compression on the
already compressed DVID segmentation.

## Code Architecture

The functionality is contained in `datatype/labelmap/export.go`. It has three layers of goroutines:

1. `readBlocksZYX` is a function that as quickly as possible scans the embedded key-value
database in native ZYX key order (potentially across multiple scales) and sends data to a buffered
`chunkCh` channel

2. a layer of 50 `chunkHandler` goroutines that all consume from that single buffered `chunkCh`,
decompress gzip DVID segmentation block data using direct header parsing (avoiding the
UnmarshalBinary/MarshalBinary copy overhead), compute a shard ID, and then either reuse or
start a `shardWriter` goroutine and send the block data

3. a layer of shard-specific goroutines that are launched within a `shardWriter.start` function,
reading from its buffered shardWriter channel, adding data for agglomerated labels from the
in-memory versioned label mapping system, zstd-compressing the block data, and accumulating
blocks into Arrow record batches which are written to an Arrow IPC streaming file along with
a sidecar CSV file for chunk indexing and byte offsets.

## Configurable Batch Size

The number of blocks per Arrow record batch is configurable via the `batch_size` field in the
export spec JSON (default: 1). Setting `batch_size=1` preserves per-block random access for
GCS range reads. Larger values (e.g., 256) reduce Arrow IPC framing overhead but make the
smallest fetchable unit a batch of that many blocks.

## Shard Output Files

Each shard produces two files:

- **`{origin}.arrow`**: Arrow IPC streaming format containing record batches
- **`{origin}.csv`**: CSV with chunk coordinates, byte offsets, and batch positions

For example, a shard starting at origin (0,0,0) would have files:
- `0_0_0.arrow` — Arrow IPC streaming data
- `0_0_0.csv` — Chunk index with byte offsets

## Arrow IPC Streaming Format

The implementation uses the **Arrow IPC Streaming Format**, which writes a schema message followed
by a sequence of record batches. This differs from the Arrow IPC File Format (Feather V2) in that
the streaming format does not include a footer with random-access metadata — instead, byte offsets
for each record batch are tracked by a `countingWriter` during export and written to the CSV file.

## Chunk Index Format (.csv)

The CSV has a comment header with the schema size (bytes written before the first record batch),
followed by column headers and one row per block. The format is the same regardless of batch size.

| Column | Type | Description |
|--------|------|-------------|
| `x` | int | Chunk X coordinate |
| `y` | int | Chunk Y coordinate |
| `z` | int | Chunk Z coordinate |
| `offset` | int | Byte offset of the record batch in the Arrow IPC stream |
| `size` | int | Byte size of the record batch |
| `batch_idx` | int | Row index within the record batch (0-based) |

**Example with batch_size=1: `0_0_0.csv`**
```csv
# schema_size=688
x,y,z,offset,size,batch_idx
1,1,0,688,792,0
2,1,0,1480,2624,0
2,2,1,4104,1192,0
```

Each block has its own record batch, so `batch_idx` is always 0 and each row has a unique
`offset`/`size`.

**Example with batch_size=256: `0_0_0.csv`**
```csv
# schema_size=688
x,y,z,offset,size,batch_idx
1,1,0,688,153600,0
2,1,0,688,153600,1
2,2,1,688,153600,2
...
```

Blocks within the same record batch share identical `offset` and `size` values but have
distinct `batch_idx` values.

## Arrow Schema

Each row in a record batch contains the following fields:

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

## Python Reading Examples

### Full table load

```python
import pyarrow as pa
from pyarrow import ipc

def read_shard(arrow_path):
    """Read an Arrow IPC streaming file into a table."""
    with open(arrow_path, "rb") as f:
        data = f.read()
    buf = pa.BufferReader(data)
    try:
        reader = ipc.open_file(buf)
    except pa.ArrowInvalid:
        buf = pa.BufferReader(data)
        reader = ipc.open_stream(buf)
    return reader.read_all()
```

### Range read of a single record batch

```python
import csv

def read_chunk_index(csv_path):
    """Load CSV index as (x,y,z) -> (offset, size, batch_idx)."""
    index = {}
    with open(csv_path) as f:
        # Skip comment line
        first = f.readline()
        if not first.startswith("#"):
            f.seek(0)
        for row in csv.DictReader(f):
            coord = (int(row["x"]), int(row["y"]), int(row["z"]))
            index[coord] = (int(row["offset"]), int(row["size"]), int(row["batch_idx"]))
    return index

def read_chunk(arrow_path, csv_path, x, y, z):
    """Read a single chunk via range read."""
    index = read_chunk_index(csv_path)
    offset, size, batch_idx = index[(x, y, z)]

    with open(arrow_path, "rb") as f:
        # Read schema (needed once, can be cached)
        schema_data = f.read(offset)  # or use schema_size from CSV header
        schema_reader = ipc.open_stream(pa.BufferReader(schema_data))
        schema = schema_reader.schema

        # Range read the record batch
        f.seek(offset)
        raw = f.read(size)

    msg = ipc.read_message(pa.BufferReader(raw))
    batch = ipc.read_record_batch(msg, schema)

    return {
        "labels": batch.column("labels")[batch_idx].as_py(),
        "supervoxels": batch.column("supervoxels")[batch_idx].as_py(),
        "compressed_data": batch.column("dvid_compressed_block")[batch_idx].as_py(),
    }
```

## Notes

- The Arrow IPC streaming data can be read by any standard Arrow library
- The CSV provides O(1) tuple-based lookup by chunk coordinates with byte offsets for range reads
- `offset` and `size` enable GCS range reads (HTTP Range requests require byte count)
- `batch_idx` identifies the row within a multi-row record batch
- The same CSV format works for any batch size — readers don't need to know the batch size
- `schema_size` in the CSV header comment tells readers how many bytes precede the first batch

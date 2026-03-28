# Optimize `export-shards` Pipeline

## Context

The `export-shards` RPC takes 12+ hours for large datasets. The pipeline has a
4-layer goroutine architecture:

1. **readBlocksZYX** — 8 concurrent Badger reader goroutines per epoch
2. **chunkHandler** (50 goroutines) — Decompress blocks, extract/map labels,
   skip all-zero blocks, send to shard writers
3. **shardWriter** (1 per shard) — Zstd-compress blocks, write Arrow IPC records

Profiling the data flow reveals several bottlenecks where CPU time and memory
allocations are wasted. The changes below are ordered by expected impact.

Zstd compression is kept at `SpeedDefault` (level 3) because the downstream
tensorstore-export system is memory-limited and cannot tolerate larger output
files.

---

## Changes

### 1. Replace stdlib gzip with klauspost/compress/gzip  (HIGH impact) ✅

**File:** `dvid/serialize.go`

Every block stored in the DB is gzip-compressed. `DeserializeData()` used Go's
stdlib `compress/gzip`, which is 3–5x slower than `klauspost/compress/gzip`.
Since `klauspost/compress` v1.18.0 is already a dependency (for zstd), this is a
drop-in import swap:

```go
// Before
"compress/gzip"

// After
"github.com/klauspost/compress/gzip"
```

The klauspost `gzip.NewReader` is API-identical to stdlib. This benefits **all**
DVID gzip operations, not just export. With 50 chunkHandler goroutines all
decompressing gzip simultaneously, this is likely the single largest CPU
bottleneck.

**Compatibility tests** added in `dvid/serialize_test.go`:
- `TestGzipCompatibility` — verifies cross-library decompression (stdlib↔klauspost)
  produces byte-identical output
- `TestGzipSerializeDataRoundTrip` — verifies the full `SerializeData`/`DeserializeData`
  path round-trips correctly, and that stdlib can decompress klauspost output

### 2. Eliminate redundant copy in UnmarshalBinary/MarshalBinary  (HIGH impact) ✅

**File:** `datatype/labelmap/export.go`, `chunkHandler`

The old per-block flow was:

```
DeserializeData(c.V, true)         ← gzip-decompress → fresh []byte ("blockData")
Block.UnmarshalBinary(blockData)   ← copies blockData into ANOTHER 8-byte-aligned
                                      buffer via New8ByteAlignBytes, then parses header
read b.Labels                      ← aliased view into the 8-byte-aligned copy
Block.MarshalBinary()              ← returns the 8-byte-aligned copy
```

Replaced with direct header parsing on the decompressed bytes using
`binary.LittleEndian.Uint64` (no alignment requirement). The decompressed
`blockData` is passed directly to the shard writer as `BlockData.Data` — no
copy. This eliminates one full allocation+copy per block (typically 10–200 KB).

### 3. ~~Switch zstd to SpeedFastest~~  (SKIPPED)

**Not implemented.** Downstream tensorstore-export is memory-limited, so output
file sizes must not grow. Zstd stays at `SpeedDefault` (level 3).

### 4. Configurable Arrow record batching  (MEDIUM impact) ✅

**File:** `datatype/labelmap/export.go`, `writeBlock()` and shard writer goroutine

The batch size (blocks per Arrow record batch) is now configurable via the
`batch_size` field in the export spec JSON. Default is 1 (preserving per-block
random access for GCS range reads). Larger values (e.g., 256) reduce Arrow IPC
framing overhead but make the smallest fetchable unit a batch of that many blocks.

- Added `defaultArrowBatchSize = 1` constant
- Added `BatchSize int` field to `exportSpec`, threaded to `shardWriter`
- `writeBlock` appends to builders; calls `flushBatch()` when batch is full
- `flushBatch()` builds arrays, writes the record batch, then writes pending
  CSV rows with the now-known byte offset and size
- Remaining blocks flushed on channel close

### 5. Reuse zstd output buffer per shardWriter  (LOW impact) ✅

**File:** `datatype/labelmap/export.go`

```go
// Before (allocates per block)
compressed := w.zenc.EncodeAll(block.Data, make([]byte, 0, len(block.Data)/2))

// After (reuses buffer — Arrow builder copies internally on Append)
w.zstdBuf = w.zenc.EncodeAll(block.Data, w.zstdBuf[:0])
ab.compressedBuilder.Append(w.zstdBuf)
```

Added `zstdBuf []byte` field to `shardWriter`.

### 6. Reuse metrics map allocations  (LOW impact) ✅

**File:** `datatype/labelmap/export.go`, `writeBlock()`

Previously `svSet` and `aggloSet` maps were allocated per block for counting
distinct values. Now stored as `shardWriter` struct fields and cleared with
`clear()` between blocks to avoid repeated allocation.

---

## Files modified

| File | Changes |
|------|---------|
| `dvid/serialize.go` | gzip import swap (#1) |
| `dvid/serialize_test.go` | gzip compatibility tests (#1) |
| `datatype/labelmap/export.go` | #2, #4, #5, #6, #7, #8, #9 |
| `datatype/labelmap/export_test.go` | Integration test, zero-block test, tensorstore cross-validation |

## Verification

- `make test` — all existing tests pass with the klauspost gzip swap
- New gzip compatibility tests pass, confirming byte-identical decompression
- Run `export-shards` on a small dataset and compare:
  - Output file sizes (should be identical — zstd level unchanged)
  - Arrow file readability from Python (`pyarrow.ipc.open_file(...)`)
  - Wall-clock time improvement

### 7. Write byte offsets in CSV during export  ✅

**File:** `datatype/labelmap/export.go`

BRAID's `ShardRangeReader` does GCS range reads of individual record batches.
Previously this required a `compute_offsets.py` post-processing step to generate
a separate offsets file.

Now DVID writes byte offsets directly into the single `.csv` index during export
using a `countingWriter` that wraps the Arrow IPC output file. The CSV format is
the same regardless of batch size:

```
# schema_size=688
x,y,z,offset,size,batch_idx
```

- `offset,size`: byte position and length of the record batch (needed for GCS
  range reads since HTTP Range requests require a byte count)
- `batch_idx`: row index within the record batch (always 0 for batch_size=1)
- `# schema_size=N`: bytes written before the first record batch (schema message)

One format, one reader code path, works for any batch size.

### 8. Parallelize Badger reads with 8 concurrent reader goroutines  (HIGH impact) ✅

**File:** `datatype/labelmap/export.go`, `readBlocksZYX`

The original pipeline had a single goroutine calling `ProcessRange` sequentially
for each chunk row within a shard strip epoch — e.g., 32×32 = 1024 sequential
Badger scans at scale 0. This was the I/O bottleneck, especially when the
dataset is larger than RAM and reads hit NVMe SSD or RAID.

Replaced with a work-queue pattern: all chunk-row key ranges are enqueued into
a `rowCh` channel, then 8 reader goroutines each pull work and call
`ProcessRange` independently with their own Badger read-only transactions.

```
Before: readBlocksZYX → sequential ProcessRange × 1024 → chunkCh
After:  readBlocksZYX → rowCh → 8 readers → each calls ProcessRange → chunkCh
```

- `numReadWorkers = 8` (hardcoded, reasonable default for NVMe)
- `numBlocks` counter changed to `atomic.Uint64` for safe concurrent increments
- `chunkCh` already handles concurrent sends (Go channels are safe for N writers)
- Sequential epoch structure preserved — no additional memory pressure

### 9. Skip all-zero label blocks  (MEDIUM impact) ✅

**File:** `datatype/labelmap/export.go`, `chunkHandler`

After extracting the label list from the block header and mapping supervoxels
to agglomerated labels, blocks where every agglomerated label is zero are
skipped. This avoids shard writer creation, zstd compression, and Arrow
writes for background-only blocks — matching tensorstore's behavior of not
writing chunks with no data.

The check is cheap: scan the `aggloLabels` slice (typically 1–8 entries per
block). Shards that would contain only zero blocks never get a writer
allocated, so no empty shard files are produced.

---

## Library note

The current zstd library (`klauspost/compress/zstd`) is the best pure-Go
implementation. The only faster option would be a CGo wrapper around libzstd
(e.g., `DataDog/zstd`), which adds build complexity and CGo overhead.
`klauspost` is the right choice.

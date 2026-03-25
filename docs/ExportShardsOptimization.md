# Optimize `export-shards` Pipeline

## Context

The `export-shards` RPC takes 12+ hours for large datasets. The pipeline has a
3-layer goroutine architecture:

1. **readBlocksZYX** — Sequential DB scan per strip of shards
2. **chunkHandler** (50 goroutines) — Decompress blocks, extract/map labels,
   send to shard writers
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

### 4. Batch Arrow records  (MEDIUM impact) ✅

**File:** `datatype/labelmap/export.go`, `writeBlock()` and shard writer goroutine

Previously a new Arrow record batch (7 arrays + IPC frame) was created **per
block**. Arrow is designed for columnar batches — creating 1-element arrays adds
substantial per-block overhead from array finalization, record construction, and
IPC framing.

Now accumulates blocks in the builders and flushes every 256 blocks:

- Added `const arrowBatchSize = 256`
- Added `batchCount int` field to `shardWriter`
- `writeBlock` only appends to builders; calls `flushBatch()` when the batch is
  full
- `flushBatch()` builds arrays, creates a record batch, and writes to IPC
- Remaining blocks are flushed on channel close

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
| `datatype/labelmap/export.go` | #2, #4, #5, #6, #7 |

## Verification

- `make test` — all existing tests pass with the klauspost gzip swap
- New gzip compatibility tests pass, confirming byte-identical decompression
- Run `export-shards` on a small dataset and compare:
  - Output file sizes (should be identical — zstd level unchanged)
  - Arrow file readability from Python (`pyarrow.ipc.open_file(...)`)
  - Wall-clock time improvement

### 7. Write byte offset CSVs during export  ✅

**File:** `datatype/labelmap/export.go`

BRAID's `ShardRangeReader` uses a companion `-offsets.csv` to do GCS range reads
of individual record batches without loading entire Arrow files. Previously this
required a `compute_offsets.py` post-processing step.

Now DVID writes both CSV files during export using a `countingWriter` that wraps
the Arrow IPC output file:

- **`.csv` index** — extended from `x,y,z,rec` to `x,y,z,rec,batch_offset,batch_idx`
- **`-offsets.csv`** — new file with `# schema_size=N` header and
  `x,y,z,rec,offset,size,batch_idx` columns (one row per block)

The `batch_idx` column gives BRAID the row index within a multi-row record batch
(from the 256-block batching in #4). With 256-block batches, each GCS range read
pulls 256 blocks at once — fewer requests, better throughput.

---

## Library note

The current zstd library (`klauspost/compress/zstd`) is the best pure-Go
implementation. The only faster option would be a CGo wrapper around libzstd
(e.g., `DataDog/zstd`), which adds build complexity and CGo overhead.
`klauspost` is the right choice.

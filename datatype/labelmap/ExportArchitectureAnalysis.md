# Export-Shards Architecture Analysis for possible crash issues

## Executive Summary

Analysis of the `export-shards` functionality in `export.go` reveals several architectural issues that can lead to out-of-memory (OOM) crashes when processing massive datasets like the male CNS (44M+ blocks). While the system is designed to run on high-end hardware (256-core, 1TB RAM), the unlimited accumulation of goroutines and memory resources can exhaust even this substantial memory capacity.

## System Context

- **Hardware**: 256-core server with 1TB RAM
- **Dataset**: Male CNS segmentation (~44 million blocks)
- **Output**: Tens of thousands of Arrow shard files
- **Processing**: 3-layer goroutine architecture with 100+ concurrent workers

## Critical Issues Identified

### 1. Unlimited Shard Writer Accumulation (Primary Issue)

**Problem**: The system creates unlimited numbers of `shardWriter` goroutines that persist until Z-direction shard boundaries are crossed.

**Evidence from logs**:
```
INFO Created shard writer 36045 -> shard file .../79872_24576_18432.arrow
INFO Created shard writer 36484 -> shard file .../81920_24576_18432.arrow
INFO Created shard writer 3727 -> shard file .../22528_26624_18432.arrow
[... thousands more ...]
```

**Root Cause**: 
```go
// Lines 592-596: Only closes writers when Z boundary crossed
if chunkZ*dvidChunkVoxelsPerDim > lastShardZ {
    handler.closeWriters(lastShardZ)
    lastShardZ = handler.getShardEndZ(scale, chunkZ)
}
```

**Impact**: For large XY extents, thousands of shard writers can accumulate before the next Z boundary, each consuming:
- 1 goroutine
- 1 file handle  
- Buffered channel (100 × BlockData entries ≈ 100MB per writer)
- Arrow IPC writer with internal buffers
- zstd encoder state
- Growing index map

**Memory Estimate**: 10,000 active writers × 100MB+ per writer = **1TB+ memory consumption**

### 2. Memory-Intensive Per-Writer Resources

Each `shardWriter` maintains substantial memory footprint:

```go
type shardWriter struct {
    ch         chan *BlockData    // 100-capacity buffer ≈ 100MB
    indexMap   map[string]uint64  // Growing with each block
    writer     *ipc.Writer        // Arrow IPC buffers
    pool       memory.Allocator   // Arrow memory allocator
    zenc       *zstd.Encoder      // Compression state
    // ... other fields
}
```

**Resource Breakdown per Writer**:
- **Channel buffer**: 100 BlockData × ~1MB each = **100MB**
- **Arrow resources**: IPC writer buffers = **10-50MB**  
- **Index map**: Growing linearly with blocks = **1-10MB**
- **zstd encoder**: Compression dictionaries = **1-5MB**
- **Total per writer**: **~150MB**

**Scaling Impact**: 10,000 writers × 150MB = **1.5TB memory usage**

### 3. Block Data Memory Multiplication

Block data gets copied multiple times through the pipeline:

1. **Original compressed data**: Read from database (`c.V`)
2. **Decompressed BlockData**: Created in `chunkHandler` (line 605-609)
3. **Unmarshaled labels.Block**: In `writeBlock` (line 255-258)
4. **Agglomerated labels slice**: Computed mapping (line 262)
5. **Arrow builder copies**: Multiple array builders (lines 273-279)
6. **zstd recompressed data**: Final compression (line 327)

**Memory Multiplier**: Each block potentially exists in 6+ copies simultaneously = **6x memory overhead**

### 4. Channel Buffer Memory Pressure

```go
// Line 640: Main chunk channel
chunkCh := make(chan *storage.Chunk, 1000) // ~1GB buffer

// Line 487: Per-shard writer channel  
ch: make(chan *BlockData, 100) // ~100MB per writer
```

**Total Channel Memory**: 1000 main chunks + (N writers × 100 blocks) = **Hundreds of GB in channel buffers alone**

### 5. Inadequate Resource Cleanup

**Writer Closure Logic Issue**:
```go
// Line 534-538: Potentially flawed closure condition
if w.lastShardZ <= lastShardZ-w.shardZSize || lastShardZ == FinalShardZ {
    closeList = append(closeList, w)
    delete(s.writers, shardID)
}
```

**Problems**:
- Complex boundary calculation may miss some writers
- No timeout-based cleanup for stalled writers
- Arrow resources not explicitly released before goroutine termination

## Architecture Strengths

### Effective Design Elements

1. **Three-layer pipeline**: Efficient separation of concerns
   - Layer 1: Fast sequential database scan
   - Layer 2: Parallel decompression and shard routing  
   - Layer 3: Parallel Arrow file writing

2. **ZYX ordering optimization**: Reading in native key order maximizes database throughput

3. **Per-shard channels**: Eliminates lock contention on file writes

4. **Arrow format**: Efficient binary format with built-in indexing

## Recommended Solutions

### Immediate Fixes

1. **Implement Writer Pool Limit**:
   ```go
   const MaxConcurrentWriters = 1000 // Limit based on available memory
   ```

2. **Add Periodic Writer Cleanup**:
   ```go
   // Close writers based on time/count, not just Z boundaries
   if numBlocks%10000 == 0 {
       handler.closeIdleWriters(time.Minute * 5)
   }
   ```

3. **Reduce Channel Buffer Sizes**:
   ```go
   chunkCh := make(chan *storage.Chunk, 100)     // Reduce from 1000
   ch: make(chan *BlockData, 10)                 // Reduce from 100  
   ```

4. **Explicit Arrow Resource Cleanup**:
   ```go
   defer func() {
       w.pool.(*memory.GoAllocator).Free() // Explicit cleanup
       w.zenc.Close()                       // Close encoder
   }()
   ```

### Architectural Improvements

1. **Writer Lifecycle Management**:
   - Implement writer LRU cache with size limits
   - Add writer timeout and idle detection
   - Batch writer creation/destruction

2. **Memory Pool for Block Data**:
   - Reuse BlockData structs across goroutines
   - Pre-allocate label slices to reduce GC pressure

3. **Backpressure Mechanism**:
   - Implement flow control when too many writers are active
   - Block `readBlocksZYX` when memory usage is high

4. **Progress-based Cleanup**:
   - Track processing progress per XY region
   - Close writers when their region is complete

## Performance vs Memory Tradeoffs

### Current Architecture
- **Pros**: Maximum parallelism, optimal I/O throughput
- **Cons**: Unbounded memory growth, potential OOM crashes

### Proposed Architecture  
- **Pros**: Bounded memory usage, stability for large datasets
- **Cons**: Slightly reduced peak throughput due to writer limits

### Memory Budget Allocation (1TB system)
- **OS + DVID core**: 200GB
- **Database cache**: 300GB  
- **Export pipeline**: 400GB (bounded)
- **Safety margin**: 100GB

## Monitoring Recommendations

1. **Add memory usage logging**: Track active writers and memory consumption
2. **Implement early warning**: Alert when approaching memory limits  
3. **Goroutine monitoring**: Track goroutine count growth patterns
4. **Resource leak detection**: Monitor file handles and Arrow resource usage

## Conclusion

The `export-shards` functionality is architecturally sound but suffers from unbounded resource growth that can overwhelm even high-end hardware. The primary issue is unlimited shard writer accumulation, compounded by high per-writer memory usage and insufficient cleanup mechanisms. 

Implementing writer limits, more frequent cleanup, and explicit resource management should resolve the OOM issues while maintaining the system's high-throughput characteristics. The recommended changes are focused on adding bounds and lifecycle management rather than fundamental architectural changes.
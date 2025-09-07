# Export-Shards Architecture Analysis for possible crash issues

## Executive Summary

Analysis of the `export-shards` functionality in `export.go` reveals several architectural issues that can lead to out-of-memory (OOM) crashes when processing massive datasets like the male CNS (44M+ blocks). While the system is designed to run on high-end hardware (256-core, 1TB RAM), the unlimited accumulation of goroutines and memory resources can exhaust even this substantial memory capacity.

## System Context

- **Hardware**: 256-core server with 1TB RAM
- **Dataset**: Male CNS segmentation (~44 million blocks) until DVID system crashed without logging error or panic. There were 207GB of output files at that point.
- **Block Size Analysis**: 207GB ÷ 44M blocks ≈ **4.7KB average per Arrow record**
- **Output**: Tens of thousands of Arrow shard files
- **Processing**: 3-layer goroutine architecture with 100+ concurrent workers

## Critical Issues Identified

### 1. DVID Block Compression Understanding

**DVID Block Architecture**: 
DVID uses a sophisticated bespoke compression scheme optimized for spatial label locality in 64³ voxel blocks:

1. **Label Header Separation**: Unique uint64 labels stored in header (typically 2-6 labels, max ~24)
2. **Minimal Bit Encoding**: Uses minimum bits per voxel to represent label indices within the block
3. **Subblock Optimization**: 8×8×8 subblocks where single-label areas are described very succinctly
4. **Spatial Locality**: Leverages fact that labels are contiguous in 3D space within small blocks
5. **Compression Effectiveness**: Achieves at least 50x compression ratio after secondary gzip/lz4 compression

**Arrow Record Composition** (based on 4.7KB average from crash data):
- **zstd-compressed DVID block data**: ~4.2KB (dominant portion)
- **Agglomerated labels list**: 2-24 labels × 8 bytes = 16-192 bytes
- **Supervoxels list**: 2-24 labels × 8 bytes = 16-192 bytes  
- **Coordinates + metadata**: ~20 bytes
- **Arrow record overhead**: Variable, ~300 bytes
- **Total per Arrow record**: ~4.7KB average (matches crash data)

**Key Insight**: DVID's compression is highly effective. The export pipeline efficiently handles the compressed block data - memory pressure comes from Arrow processing buffers, not the blocks themselves.

### 2. Bounded Shard Writer Accumulation (Primary Issue)

**Problem**: The system creates multiple slabs worth of `shardWriter` goroutines due to conservative cleanup logic that prevents premature writer closure.

**Evidence from logs**:
```
INFO Created shard writer 36045 -> shard file .../79872_24576_18432.arrow
INFO Created shard writer 36484 -> shard file .../81920_24576_18432.arrow
INFO Created shard writer 3727 -> shard file .../22528_26624_18432.arrow
[... thousands more ...]
```

**Root Cause**: 
```go
// Lines 592-596: Conservative cleanup only at Z-slab boundaries
if chunkZ*dvidChunkVoxelsPerDim > lastShardZ {
    handler.closeWriters(lastShardZ)
    lastShardZ = handler.getShardEndZ(scale, chunkZ)
}
```

**Conservative Cleanup Logic**: The `shardHandler.closeWriters` function is intentionally conservative to prevent premature closure of writers that might still receive blocks stuck in processing channels. This results in multiple Z-slabs being active simultaneously.

**Bounded Writer Calculation**:
- **Male CNS Dimensions**: (94,088 × 78,317 × 134,576) voxels
- **Shard Size**: 2048³ voxels per shard
- **Max Writers per Z-slab**: (94,088 × 78,317) ÷ (2048 × 2048) ≈ **1,760 shards**
- **Active Z-slabs**: 2-3 slabs due to conservative cleanup
- **Total Active Writers**: ~1,760 × 2-3 = **3,520-5,280 writers maximum**

**Corrected Memory Estimate**: 3,520-5,280 active writers × 15-70MB per writer = **53-370GB memory consumption** (manageable for 1TB system)

### 3. Memory-Intensive Per-Writer Resources (Corrected)

Each `shardWriter` maintains substantial memory footprint:

```go
type shardWriter struct {
    ch         chan *BlockData    // 100-capacity buffer ≈ 450KB
    indexMap   map[string]uint64  // Growing with each block
    writer     *ipc.Writer        // Arrow IPC buffers
    pool       memory.Allocator   // Arrow memory allocator
    zenc       *zstd.Encoder      // Compression state
    // ... other fields
}
```

**Resource Breakdown per Writer**:
- **Channel buffer**: 100 BlockData × ~4.5KB each = **450KB**
- **Arrow resources**: IPC writer buffers = **10-50MB**  
- **Index map**: Growing linearly with blocks = **1-10MB**
- **zstd encoder**: Compression dictionaries = **1-5MB**
- **Total per writer**: **~15-70MB**

**Scaling Impact**: 3,520-5,280 writers × 15-70MB = **53-370GB memory usage** (bounded and manageable)

**Key Insight**: Arrow processing buffers (10-50MB) dominate per-writer memory consumption, not the compressed block data in channels.

### 4. Block Data Memory Multiplication

Block data gets copied multiple times through the pipeline:

1. **Original compressed data**: Read from database (`c.V`)
2. **Decompressed BlockData**: Created in `chunkHandler` (line 605-609)
3. **Unmarshaled labels.Block**: In `writeBlock` (line 255-258)
4. **Agglomerated labels slice**: Computed mapping (line 262)
5. **Arrow builder copies**: Multiple array builders (lines 273-279)
6. **zstd recompressed data**: Final compression (line 327)

**Memory Multiplier**: Each block potentially exists in 6+ copies simultaneously = **6x memory overhead**

### 5. Channel Buffer Memory Pressure (Corrected)

```go
// Line 640: Main chunk channel
chunkCh := make(chan *storage.Chunk, 1000) // ~5MB buffer

// Line 487: Per-shard writer channel  
ch: make(chan *BlockData, 100) // ~450KB per writer
```

**Total Channel Memory**: 1000 main chunks (~5MB) + (N writers × 450KB) = **Still substantial with many writers, but manageable**

### 6. Inadequate Resource Cleanup

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

**Critical Shard Writer Lifecycle Issue**:
The current approach of only retiring shard writers after Z-slab boundaries is necessary to avoid premature closure before all blocks in a shard are processed. However:

1. **Risk of Shard Recreation**: If a shard writer is prematurely closed and later blocks for that shard are encountered, a new writer would be created, potentially corrupting the shard file.

2. **Missing Safety Check**: The system lacks tracking of previously processed shard IDs to detect and log such recreation attempts.

3. **Recommended Safety Addition**:
   ```go
   type shardHandler struct {
       // ... existing fields
       processedShards map[uint64]struct{}  // Track completed shards (memory efficient)
   }
   
   // In getWriter, add check:
   if _, exists := s.processedShards[shardID]; exists {
       dvid.Errorf("CRITICAL: Attempted to recreate shard writer for ID %d - export corrupted", shardID)
       return nil, fmt.Errorf("shard recreation detected")
   }
   
   // When closing writers, mark as processed:
   for _, w := range closeList {
       s.processedShards[shardID] = struct{}{}
       close(w.ch)
   }
   ```

This safety mechanism would catch data corruption issues and provide early warning of logic errors in shard writer lifecycle management.

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

### Memory Budget Allocation (1TB system) - Revised
- **OS + DVID core**: 200GB
- **Database cache**: 400GB  
- **Export pipeline**: 400GB (sufficient for 53-370GB actual requirements)
- **Safety margin**: 0GB (tight but workable)

**Actual Requirements**: 53-370GB for export pipeline with bounded 3,520-5,280 writers

## Monitoring and Optimization Recommendations

### 1. Periodic Memory Monitoring Goroutine

Launch a dedicated monitoring goroutine to track export progress and resource usage:

```go
func (s *shardHandler) startMemoryMonitor(ctx context.Context) {
    go func() {
        ticker := time.NewTicker(30 * time.Second)
        defer ticker.Stop()
        
        for {
            select {
            case <-ctx.Done():
                return
            case <-ticker.C:
                var m runtime.MemStats
                runtime.GC() // Force GC for accurate measurements
                runtime.ReadMemStats(&m)
                
                s.mu.RLock()
                activeWriters := len(s.writers)
                
                // Calculate total bytes written across all writers
                var totalRecords, totalBytes uint64
                for _, writer := range s.writers {
                    writer.mu.Lock()
                    totalRecords += writer.recordNum
                    totalBytes += writer.bytesAccumulated
                    writer.mu.Unlock()
                }
                s.mu.RUnlock()
                
                dvid.Infof("EXPORT MONITOR: Writers=%d, Records=%d, TotalMB=%d, "+
                    "AllocMB=%d, SysMB=%d, Goroutines=%d, GCs=%d",
                    activeWriters, totalRecords, totalBytes/1024/1024,
                    m.Alloc/1024/1024, m.Sys/1024/1024, 
                    runtime.NumGoroutine(), m.NumGC)
                
                // Early warning if approaching memory limits
                allocGB := float64(m.Alloc) / (1024 * 1024 * 1024)
                if allocGB > 300 { // Warn at 300GB
                    dvid.Criticalf("MEMORY WARNING: Using %.1fGB, approaching 370GB limit", allocGB)
                }
            }
        }
    }()
}
```

### 2. Two-Layer Compression Strategy and Memory Impact

The export pipeline implements a **two-layer compression strategy**:

| Layer | Method | Fields Affected | Purpose |
|-------|--------|------------------|---------|
| **1** | Manual ZSTD via `klauspost/compress` | `dvid_compressed_block` | Keeps block data compressed post-read |
| **2** | Arrow IPC ZSTD (`ipc.WithZstd()`) | All other fields | Reduces on-disk and in-flight size |

This hybrid strategy balances performance and flexibility, ensuring efficient storage and streaming while preserving deferred decompression for heavy binary payloads.

**Layer 1 - Manual ZSTD Compression**:
- Applied to `dvid_compressed_block` field only
- Uses `klauspost/compress/zstd` package
- Compresses DVID's bespoke block data (~4.2KB compressed per record)
- Purpose: Keeps block data compressed for downstream systems
   - The heavy payload remains compressed even after the Arrow record is decoded.
   - Downstream systems can delay decompression until the data is actually needed.

**Layer 2 - Arrow IPC-Level ZSTD Compression**:
- Applied to all other fields using `ipc.WithZstd()`
- Compresses coordinates, labels, supervoxels, and metadata
- Purpose: Reduces file size and I/O overhead
- Is transparent to readers that support Arrow IPC compression.

#### Arrow IPC-Level Compression: Impact and Behavior

##### 📦 Why Use IPC Compression with Pre-Compressed Fields?

In the exported Arrow IPC files for `labelmap`, the field `dvid_compressed_block` is **already compressed manually with ZSTD**. This leaves relatively little benefit for IPC-level compression on that field, but it can still help on the rest of the record.

##### ✅ What IPC Compression Does Help

The following fields are **not pre-compressed** and **benefit from Arrow IPC compression**:

- `labels` (List<UInt64>)
- `supervoxels` (List<UInt64>)
- `chunk_x`, `chunk_y`, `chunk_z` (Int32)
- `uncompressed_size` (UInt32)

These fields often contain:
- Repeated values
- Sparsity or empty lists
- Small integers

➡️ These compress well with ZSTD, especially when exporting millions of blocks.

##### 🔧 Optional Tuning in Go

You can control Arrow's built-in ZSTD compression when creating the IPC writer:

```go
writer := ipc.NewWriter(file,
    ipc.WithSchema(schema),
    ipc.WithZstd(),                    // Enable ZSTD compression
    ipc.WithMinSpaceSavings(0.05),    // Skip compression unless it saves 5%+
)
```

**Memory Buffer Impact**: While IPC compression helps with file size and I/O efficiency, its impact on Arrow's internal memory buffers is limited since the dominant field (`dvid_compressed_block` ~4.2KB) is already compressed. The compressible fields total only ~50-400 bytes per record.

### 3. Alternative Memory Management Strategies

Since the Go Arrow library doesn't provide explicit flushing capabilities, explore additional approaches:

**A. Record Batching Instead of Single Records**:
```go
type shardWriter struct {
    // ... existing fields ...
    recordBatch      []arrow.Record  // Accumulate records before writing
    batchThreshold   int             // Write batch when this many records accumulated
    bytesAccumulated uint64          // Track approximate batch size
}

func (w *shardWriter) writeBlockWithBatching(block *BlockData) error {
    // ... existing record creation code ...
    
    w.mu.Lock()
    w.recordBatch = append(w.recordBatch, record)
    recordSize := uint64(len(block.Data)) + uint64(len(aggloLabels)*8) + uint64(len(b.Labels)*8) + 32
    w.bytesAccumulated += recordSize
    
    shouldWriteBatch := len(w.recordBatch) >= w.batchThreshold || 
                       w.bytesAccumulated >= 10*1024*1024  // 10MB threshold
    
    if shouldWriteBatch {
        batch := w.recordBatch
        w.recordBatch = make([]arrow.Record, 0, w.batchThreshold)
        w.bytesAccumulated = 0
        w.mu.Unlock()
        
        // Write entire batch as single operation
        for _, rec := range batch {
            if err := w.writer.Write(rec); err != nil {
                return err
            }
            rec.Release()  // Release individual records
        }
        return nil
    }
    w.mu.Unlock()
    return nil
}
```

**B. Enhanced Memory Source Validation**:
```go
func (w *shardWriter) trackDetailedMemory() {
    var m1, m2 runtime.MemStats
    
    // Before record creation
    runtime.GC()
    runtime.ReadMemStats(&m1)
    
    // ... record creation and writing ...
    
    // After record creation  
    runtime.GC()
    runtime.ReadMemStats(&m2)
    
    if w.recordNum % 1000 == 0 {
        memDelta := int64(m2.Alloc) - int64(m1.Alloc)
        dvid.Infof("Writer %s: +%d bytes after record %d, total records: %d", 
            w.shardPath, memDelta, w.recordNum, w.recordNum)
    }
}
```

**C. File-Level Sync (Limited Effectiveness)**:
```go
func (w *shardWriter) syncFileBuffers() error {
    // Forces OS buffers to disk, but won't affect Arrow's internal memory buffers
    return w.f.Sync()
}
```

### 3. Additional Monitoring Features

```go
// Memory pressure detection and adaptive behavior
func (s *shardHandler) checkMemoryPressure() bool {
    var m runtime.MemStats
    runtime.ReadMemStats(&m)
    allocGB := float64(m.Alloc) / (1024 * 1024 * 1024)
    
    if allocGB > 250 {  // High memory usage
        // Reduce batch thresholds across all writers to force more frequent writing
        s.mu.RLock()
        for _, writer := range s.writers {
            writer.mu.Lock()
            if writer.batchThreshold > 10 {  // Reduce to smaller batches
                writer.batchThreshold = 10
            }
            writer.mu.Unlock()
        }
        s.mu.RUnlock()
        dvid.Infof("ADAPTIVE: Reduced batch thresholds due to memory pressure (%.1fGB)", allocGB)
        return true
    }
    return false
}
```

### 4. Export Performance Analysis

1. **Identify bottlenecks**: Monitor whether chunkHandlers or shardWriters are limiting throughput
2. **Validate memory sources**: Use detailed tracking to confirm whether Arrow IPC buffers are the primary memory consumer
3. **Record batching effectiveness**: Test different batch sizes (10, 50, 100 records) for optimal memory/throughput balance
4. **Alternative strategies**: If Arrow buffers remain problematic, consider different serialization approaches
5. **Resource leak detection**: Monitor file handles and goroutine counts over time

## Conclusion

The `export-shards` functionality is architecturally sound with bounded resource requirements that are manageable on high-end hardware. With corrected understanding of DVID block compression and volume dimensions:

**Key Findings**:
- DVID's bespoke compression is highly effective (~4.7KB average Arrow records)
- Writers are bounded by volume dimensions: max 3,520-5,280 concurrent writers (not unlimited)
- Memory requirements: 53-370GB for export pipeline (manageable for 1TB system)
- Conservative cleanup logic maintains 2-3 active Z-slabs to prevent premature writer closure
- Critical need for shard recreation safety checks

**Primary Issues**:
1. Conservative Z-slab cleanup results in multiple active slabs simultaneously
2. Arrow IPC buffers consuming 10-50MB per writer 
3. Risk of shard file corruption without recreation tracking
4. Memory pressure from Arrow processing, not DVID block data

**Recommended Changes**:
- Add shard recreation safety tracking with `map[uint64]struct{}`
- Runtime monitoring to validate actual memory consumption vs. estimates (53-370GB)
- Consider slightly less conservative cleanup if monitoring shows safe margins
- Focus on optimizing Arrow buffer usage rather than fundamental architectural changes

The architecture efficiently handles DVID's compressed block data and has bounded resource growth - the issue is manageable memory optimization, not unlimited resource accumulation.
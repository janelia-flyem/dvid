# DVID Server Initialization Optimization Plan

## Problem

DVID server startup takes ~18 minutes. Nearly all of this time is spent in
`labelmap.Initialize()` → `VCache.initToVersion()`, which walks the version DAG
(leaf → root), reading mutation log files from disk for each version and building
the in-memory supervoxel-to-label mapping cache.

### Real Performance Data (mCNS dataset)

| Version | Mutations | Time | Notes |
|---------|-----------|------|-------|
| v2 | 68M Mapping | **6m00s** | Bulk initial mapping |
| v5 | 501K Mapping | **2m20s** | Many supervoxels per MappingOp message |
| v6 | 28K Map + 27K Cleave + 634 SVSplit | 14s | |
| v22 | 2.7K Cleave + 23K Map + 322 SVSplit | 1.9s | |
| All others combined | various | ~10 min | |

The v5 count (501K) is *messages*, not supervoxels — each `MappingOp` has a
`repeated uint64 original` field. The v5 log file is 1.4 GB, so each message
averages ~2.8 KB → roughly 300-400 supervoxels per message → **~150-175M
actual `setMapping()` calls**, more than version 2's 68M messages.

### Mutation Log File Sizes (mCNS `segmentation` instance)

All files are on Optane storage (`/optane/dbs/cns3/mutlogs/`). Total ~6 GB.
Optane reads at near-memory speeds — the entire dataset loads from disk in
2-3 seconds. **The full 18 minutes of startup time is CPU-bound processing,
not I/O.**

| File size | Version | Processing time |
|-----------|---------|----------------|
| 2.6 GB | v2 | 6m00s |
| 1.4 GB | v5 | 2m20s |
| 671 MB | ? | ~1-2 min |
| 185 MB | ? | ~30s |
| 165 MB | ? | ~30s |
| 152 MB | ? | ~20s |
| 138 MB | ? | ~20s |
| ~80 files < 50 MB | various | seconds each |

### Three Structural Problems

1. **Single consumer goroutine per version** — for version 2's 68M messages,
   one goroutine does all protobuf unmarshal + `setMapping()` work sequentially.
   The 2.6 GB log file reads from Optane in ~1 second; the 6 minutes is entirely
   single-threaded CPU processing.
2. **Versions loaded sequentially** — version 2's 6 min blocks everything else
3. **`replace=true` in `setMapping` during init** — every call does an O(N)
   `excludeVersion()` scan that is unnecessary during initialization

## Current Startup Flow

```
main() → DoServe() → LoadConfig → Initialize → InitBackend
  → storage.Initialize()          # opens storage engines (fast)
  → datastore.Initialize()        # loads repos + metadata (moderate)
    → loadVersion0()              # deserialize repos, set up sync graph
      → InitDataHandlers()        # starts sync goroutines per instance (fast)
    → FOR EACH data instance SEQUENTIALLY:   ← BOTTLENECK
        validate log stores (fast)
        d.Initialize()            ← EXPENSIVE for labelmap and neuronjson
  → server.Serve()                # starts HTTP + RPC
```

### Where Time Is Spent

**labelmap `Initialize()`** (`datatype/labelmap/labelidx.go:46-98`):
1. Sets up freecache and mutation cache (fast)
2. Gets master branch ancestry
3. Calls `getMapping(d, masterLeafV)` → `VCache.initToVersion()` (SLOW)

**`VCache.initToVersion()`** (`datatype/labelmap/vcache.go:280-310`):
- Holds `vc.mu.Lock()` for entire duration
- Iterates ancestors from leaf to root
- For each version:
  - Reads entire mutation log file via `labels.StreamLog()` → `fileLogs.StreamAll()`
  - `StreamAll()` calls `io.ReadAll()` to load the whole file into memory
  - Sends entries down a buffered channel (size 1000)
  - A goroutine consumes the channel: protobuf unmarshal → `setMapping()`
  - Waits for goroutine to finish before moving to next version

### Sequential Comment Is Only Relevant During Startup

The comment "Should be done sequentially in case its necessary to start receiving
requests" (`repo_local.go:208`) is moot because:

- `Initializer.Initialize()` is called ONLY at `datastore/repo_local.go:208`
  during the startup loop
- When a labelmap is created at runtime via the API, only
  `DataInitializer.InitDataHandlers()` is called (`repo_local.go:2158`), which
  starts sync goroutines but does NOT load mapping history
- The HTTP server starts at `server.Serve()` AFTER `datastore.Initialize()`
  returns

---

## Proposed Optimizations

### 1. Multiple Consumer Goroutines Per Version  (HIGHEST impact)

**File:** `datatype/labelmap/vcache.go:179-274, 280-310`

The single biggest bottleneck: each version's mutation log is processed by ONE
goroutine doing protobuf unmarshal + `setMapping()` sequentially. Version 2's
2.6 GB log file reads from SSD in ~2-5 seconds, but single-threaded processing
of 68M messages takes ~350 seconds. The I/O is not the bottleneck — the CPU
work is.

Currently `loadVersionMapping` is the sole consumer of the channel:
```
StreamAll → ch (buffer 1000) → 1 goroutine: unmarshal + setMapping
```

Change to N consumer goroutines:
```
StreamAll → ch (buffer 1000) → N goroutines: each does unmarshal + setMapping
```

**Why this is safe:**
- `setMapping()` already uses per-shard locks (`fmMu`) on 100 independent shards,
  so N consumers writing to different shards don't contend
- For versions with only Mapping ops (v2: 68M Mapping, 0 Cleave, 0 Split), each
  supervoxel appears at most once, so there is no ordering concern
- For versions with mixed ops (Mapping + Cleave on the same supervoxel within one
  version), ordering matters — but these versions are already fast (seconds). We
  can either: (a) use multiple consumers only for Mapping-only versions, or
  (b) always use multiple consumers and accept that with `replace=false`, the
  `decodeMappings()` map-overwrite behavior means whichever consumer writes last
  "wins" for a given supervoxel. Since mutations within a version are logically
  unordered (they're all applied at the same version), this is acceptable.

**Implementation:** Refactor `loadVersionMapping` to spawn N workers:

```go
func (vc *VCache) loadVersionMapping(ancestors []dvid.VersionID, dataname dvid.InstanceName, ch chan storage.LogMessage, wg *sync.WaitGroup) {
    v := ancestors[0]
    numConsumers := 8

    // Per-consumer state for splits (merged after)
    type consumerResult struct {
        splits  []proto.SupervoxelSplitOp
        numMsgs map[string]uint64
    }
    results := make([]consumerResult, numConsumers)

    var cwg sync.WaitGroup
    cwg.Add(numConsumers)
    for i := 0; i < numConsumers; i++ {
        go func(idx int) {
            defer cwg.Done()
            r := &results[idx]
            r.numMsgs = map[string]uint64{...}
            var op proto.MappingOp  // reuse per consumer
            for msg := range ch {
                switch msg.EntryType {
                case proto.MappingOpType:
                    r.numMsgs["Mapping"]++
                    op.Reset()
                    pb.Unmarshal(msg.Data, &op)
                    for _, sv := range op.GetOriginal() {
                        vc.setMappingInit(v, sv, op.GetMapped())
                    }
                // ... other types ...
                }
            }
        }(i)
    }
    cwg.Wait()

    // Merge results
    // ... aggregate splits and numMsgs across consumers ...
    wg.Done()
}
```

Multiple consumers reading from the same channel is safe in Go — each message
goes to exactly one consumer.

**Expected speedup:** With 8 consumers, version 2 could drop from ~6 min to
under 1 min, and version 5 (~175M setMapping calls) similarly. This directly
attacks the dominant bottleneck rather than working around it.

### 2. Parallel Version Loading Within initToVersion  (HIGH impact)

**File:** `datatype/labelmap/vcache.go:280-310`

Currently loads versions one-at-a-time from leaf to root. Since version 2 alone
takes 6 minutes, parallel loading would reduce total time from ~18 min to ~6 min
(bounded by the slowest version). Combined with #1 (multiple consumers, which reduces version 2 itself), this
overlaps the remaining versions with each other and with neuronjson init (~2 min).

**Why this is safe:**
- `setMapping()` uses per-shard locks (`fmMu`) on 100 independent shards —
  concurrent writes from different versions serialize at the shard level
- `splits` writes are per-version keyed and protected by `splitsMu`
- Different versions read different log files (keyed by `dataID-versionUUID`)
- `mappedVersions` can be pre-populated before parallel loading begins
- `vc.mu` (the outer lock) prevents other code paths from calling
  `initToVersion` concurrently for the same VCache — during startup only one
  call occurs per VCache, and the lock does NOT block `setMapping()` or
  `mapLabel()` (read path) which use only shard-level locks

**Implementation:**

Refactor `initToVersion` into two phases:

```go
// Phase 1: Determine which versions need loading, pre-populate mappedVersions
var toLoad []int
for pos, ancestor := range ancestors {
    if _, found := vc.mappedVersions[ancestor]; found {
        break
    }
    toLoad = append(toLoad, pos)
}
// pre-populate mappedVersions for all toLoad entries

// Phase 2: Load mutation logs concurrently (bounded by 8 goroutines)
var g errgroup.Group
g.SetLimit(8)
for _, pos := range toLoad {
    g.Go(func() error { ... StreamLog + loadVersionMapping ... })
}
return g.Wait()
```

**Expected speedup:** ~3× (18 min → ~6 min, bounded by version 2)

### 3. Use `replace=false` During Initialization  (HIGH impact)

**File:** `datatype/labelmap/vcache.go`, `loadVersionMapping()` line 206

The `modify()` function's own comment (line 488-489) says: *"If it is known that
the mappings don't include a version, like when ingesting during initialization,
then replace should be set to false."*

During `initToVersion`, each version is loaded exactly once (verified via
`mappedVersions` check). So `excludeVersion(v)` is called 68M+ times for
version 2 but will NEVER find a match for that version — pure waste.

**Correctness with duplicate supervoxels within a version:** If a supervoxel gets
both a MappingOp and a CleaveOp in the same version, both entries are appended
to the vmap with `replace=false`. When `decodeMappings()` is called later to
read the mapping, it returns `map[VersionID]uint64` — the map overwrites earlier
entries, so the LAST entry for a given version wins. Since the channel streams in
log-file order (append-only), this preserves the correct "last mutation wins"
semantic.

**Implementation:** Add `setMappingInit` variant that passes `replace=false`:

```go
func (vc *VCache) setMappingInit(v dvid.VersionID, from, to uint64) {
    vc.mapUsed = true
    shard := from % vc.numShards
    lmap := vc.mapShards[shard]
    lmap.fmMu.Lock()
    vm := lmap.fm[from]
    lmap.fm[from] = vm.modify(v, to, false)
    lmap.fmMu.Unlock()
}
```

**Expected speedup:** Eliminates O(N) `excludeVersion` scan per `setMapping`
call where N = existing version entries per supervoxel. Most impactful for
versions loaded late in the leaf→root traversal (v2 has ~26 prior version
entries per supervoxel to scan through). Combined with parallel loading (#1),
this also reduces shard lock hold time, improving throughput for #1 and #2.

### 4. Parallelize Across Data Instances  (MEDIUM impact)

**File:** `datastore/repo_local.go:185-211`

Change the sequential `Initialize()` loop to run concurrently with
`errgroup.Group`, bounded by `runtime.NumCPU()`.

**Why this is safe:**
- Each labelmap instance has its own `VCache` keyed by `DataUUID` in the global
  `iMap` (which already uses its own mutex)
- Each neuronjson instance has its own `memdbs`
- Different instances read different mutation log files and different KV ranges
- No shared mutable state between different instances' `Initialize()` calls

**Race-safety fixes required:**

1. **labelmap `indexCache`** (`labelidx.go:33-58`): Package-level freecache
   initialized without synchronization. Add a `sync.Once`.

2. **labelmap `mutcache`** (`labelidx.go:60-79`): Map initialization and writes
   need a mutex.

3. **labelarray `indexCache`** (`labelidx.go:27-49`): Same pattern as labelmap.

**Expected speedup:** For mCNS with one dominant instance, modest (~1.5×). More
impactful for deployments with multiple heavy instances.

### 5. Reduce Allocations in the Hot Path  (LOW-MEDIUM impact)

**File:** `datatype/labelmap/vcache.go`

**a. Reuse protobuf struct in `loadVersionMapping`:**
```go
var op proto.MappingOp
for msg := range ch {
    case proto.MappingOpType:
        op.Reset()  // reuse instead of allocating new struct each iteration
        if err := pb.Unmarshal(msg.Data, &op); err != nil { ... }
```

**b. Stack-allocate vmap encoding** via `appendMapping` method:
```go
func (vm vmap) appendMapping(v dvid.VersionID, label uint64) vmap {
    var buf [binary.MaxVarintLen32 + binary.MaxVarintLen64]byte
    n := binary.PutUvarint(buf[:], uint64(v))
    n += binary.PutUvarint(buf[n:], label)
    return append(vm, buf[:n]...)
}
```

This replaces `createEncodedMapping()` which heap-allocates via `make()` on
every call (68M+ times for version 2).

### 6. Stream File Reads Instead of `io.ReadAll()`  (DEFERRED — likely unnecessary)

**File:** `storage/filelog/filelog.go:287-349`

Replace the `io.ReadAll()` in `StreamAll()` with buffered streaming reads so the
consumer goroutine can start processing entries before the file is fully read.

**Why this is low priority:** On Optane, `io.ReadAll` of the entire 6 GB dataset
takes ~2-3 seconds total. With 8 concurrent version loads, the worst-case peak
memory from file buffers is the 8 largest files simultaneously:
2.6G + 1.4G + 671M + 185M + 165M + 152M + 138M + 49M ≈ 5.4 GB. Plus VCache
map growth. On a server handling teravoxel datasets, this is almost certainly
fine. The trade-off (per-entry allocation vs zero-copy slicing) may actually be
net negative for performance.

To verify after implementing #2, watch RSS during startup:
```bash
# In one terminal, start DVID, note its PID
# In another terminal:
while kill -0 $PID 2>/dev/null; do
    grep VmRSS /proc/$PID/status
    sleep 5
done
```
Only implement streaming reads if peak RSS is problematic.

---

## Expected Combined Outcome

With #1 (multi-consumer) giving ~8× on per-version CPU time, #2 (parallel
versions) overlapping them, and #4 (parallel instances) overlapping labelmap
with neuronjson (~2 min):

| Stage | Current | After #1-#4 |
|-------|---------|-------------|
| Slowest version (v2 or v5) | 6m00s | ~45s-1m |
| All other versions | +12 min (sequential) | overlapped with above |
| neuronjson (~2 min) | +2 min (sequential) | overlapped with above |
| **Total** | **~18 min** | **~2 min** |

## Implementation Order

1. **#3 (replace=false)** — Simplest, zero structural risk, immediately measurable
2. **#1 (multiple consumers)** — Directly attacks the 6-min version 2 bottleneck
3. **#2 (parallel versions)** — Overlaps remaining versions with each other
4. **#4 (parallel instances)** — Overlaps labelmap with neuronjson (~2 min each)
5. **#5 (reduce allocations)** — Profile-guided, do after #1-4
6. **#6 (streaming reads)** — Deferred; likely unnecessary given Optane speeds

## Key Files

| File | Changes |
|------|---------|
| `datatype/labelmap/vcache.go:96-105, 179-310` | #1 multi-consumer, #2 parallel versions, #3 replace=false, #5 alloc reduction |
| `datastore/repo_local.go:185-211` | #4 parallelize Initialize() loop |
| `datatype/labelmap/labelidx.go:33-98` | #4 sync.Once + mutex |
| `datatype/labelarray/labelidx.go:27-49` | #4 sync.Once for indexCache |
| `storage/filelog/filelog.go:287-349` | #6 streaming file reads |

## Verification

1. `make test` — all existing tests pass
2. `go test -race -tags "badger filestore ngprecomputed" ./datatype/labelmap/...`
   — race detector clean
3. Start server, compare `/api/node/:master/segmentation/mappings` output against
   the baseline captured in `test_data/mappings-d79556` (the actual output from
   the HEAD version d79556 on the mCNS master branch, produced by the current
   sequential code)
4. Compare startup timing logs — instances should overlap (#4), versions should
   overlap (#2), per-version time should drop (#1)
5. Monitor RSS during startup to check memory pressure from concurrent loading

## Library Note

`golang.org/x/sync/errgroup` is already an indirect dependency in `go.mod`
(v0.20.0). Changes #1 and #3 need a direct import.

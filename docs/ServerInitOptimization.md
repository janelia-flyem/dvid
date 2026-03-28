# DVID Server Initialization Optimization Plan

## Problem

DVID server startup takes ~30 minutes. Nearly all of this time is spent in the
sequential `Initializer.Initialize()` loop in `datastore/repo_local.go:185-211`,
which initializes each data instance one at a time. The dominant cost is labelmap's
`VCache.initToVersion()`, which walks the entire version DAG (leaf → root), reading
mutation log files from disk for each version and building the in-memory
supervoxel-to-label mapping cache.

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

**neuronjson `Initialize()`** (`datatype/neuronjson/neuronjson.go:1001-1068`):
- Loads JSON schemas (fast)
- Calls `initMemoryDB()` → `loadMemDB()` which does `ProcessRange` over all
  annotation keys, JSON-unmarshaling each one into memory

### Why It's Slow

1. **Sequential across instances**: If there are 3 labelmap instances each taking
   10 min, total is 30 min instead of 10 min.
2. **Sequential across versions within one instance**: Each version's mutation log
   is read and processed before the next version starts.
3. **`io.ReadAll()` per version**: Reads entire file before parsing begins.

## Proposed Optimizations

### Tier 1: Parallelize Across Data Instances

**Impact: HIGH | Risk: LOW | Effort: ~1 day**

Change the sequential initialization loop to run all `Initializer.Initialize()`
calls concurrently, bounded by a semaphore.

**Why this is safe:**
- Each labelmap instance has its own `VCache` keyed by `DataUUID` in the global
  `iMap` (which already uses its own mutex).
- Each neuronjson instance has its own `memdbs`.
- Different instances read different mutation log files and different KV ranges.
- No shared mutable state between different instances' `Initialize()` calls.
- The comment "Should be done sequentially in case its necessary to start receiving
  requests" (line 208) is moot because the HTTP server hasn't started yet at this
  point — `server.Serve()` is called after `datastore.Initialize()` returns.

**File: `datastore/repo_local.go:185-211`**

Split the loop into two phases:

```go
// Phase 1: Validate log stores (fast, keep sequential)
for _, data := range m.iids {
    if data.IsDeleted() { continue }
    // ... existing WriteLog/ReadLog validation (lines 192-205) ...
}

// Phase 2: Initialize data instances concurrently
dvid.Infof("Initializing data instances concurrently...\n")
var g errgroup.Group
g.SetLimit(runtime.NumCPU())
for _, data := range m.iids {
    if data.IsDeleted() { continue }
    d, ok := data.(Initializer)
    if !ok { continue }
    dataName := data.DataName()
    typeName := data.TypeName()
    g.Go(func() error {
        dvid.TimeInfof("Initializing data instance %q, type %q\n", dataName, typeName)
        d.Initialize()
        dvid.TimeInfof("Finished initializing data instance %q\n", dataName)
        return nil
    })
}
if err := g.Wait(); err != nil {
    return err
}
```

**Required race-safety fixes:**

1. **labelmap `indexCache`** (`datatype/labelmap/labelidx.go:33-58`):
   The package-level `indexCache` is nil-checked and initialized without
   synchronization. Add a `sync.Once`:
   ```go
   var indexCacheOnce sync.Once
   // In Initialize():
   indexCacheOnce.Do(func() {
       numBytes := server.CacheSize("labelmap")
       if numBytes > 0 {
           indexCache = freecache.NewCache(numBytes)
           dvid.Infof("Created freecache of ~ %d MB for labelmap instances.\n", numBytes>>20)
       }
   })
   ```

2. **labelmap `mutcache`** (`datatype/labelmap/labelidx.go:60-79`):
   Add a mutex around the `mutcache` map initialization and writes:
   ```go
   var mutcacheMu sync.Mutex
   // In Initialize():
   mutcacheMu.Lock()
   if mutcache == nil {
       mutcache = make(map[dvid.UUID]storage.OrderedKeyValueDB)
   }
   mutcache[d.DataUUID()] = okvDB
   mutcacheMu.Unlock()
   ```

3. **labelarray `indexCache`** (`datatype/labelarray/labelidx.go:36-49`):
   Same pattern as labelmap — add `sync.Once`.

**Dependency:** `golang.org/x/sync/errgroup` — already an indirect dependency
in `go.mod` (v0.20.0), just needs direct import.

**Expected speedup:** If N independent instances take roughly equal time,
startup drops from `sum(times)` to `max(times)` — roughly N× improvement
(bounded by CPU count and I/O bandwidth). For a typical deployment with
3-5 heavy instances, ~3-5× speedup.

---

### Tier 2: Parallelize Version Loading Within One VCache

**Impact: MEDIUM-HIGH | Risk: MODERATE | Effort: ~1-2 days**

Within `initToVersion()`, load mutation logs for multiple ancestor versions
concurrently instead of one at a time.

**Why this is safe:**
- `setMapping(v, from, to)` uses shard-level locks (`lmap.fmMu.Lock()`) on
  100 independent shards. Two goroutines writing different versions for the
  same supervoxel are serialized at the shard lock. The `modify(v, to, true)`
  call replaces the entry for a specific version within the locked section,
  so byte-slice operations are atomic with respect to each other.
- `splits` map writes are per-version keyed and protected by `splitsMu`.
- `mappedVersions` can be pre-populated before parallel loading begins.
- Different versions have different log files (`dataID-versionUUID`), so
  file I/O doesn't contend.

**File: `datatype/labelmap/vcache.go:280-310`**

Refactor `initToVersion()`:

```go
func (vc *VCache) initToVersion(d dvid.Data, v dvid.VersionID, loadMutations bool) error {
    vc.mu.Lock()
    defer vc.mu.Unlock()

    ancestors, err := datastore.GetAncestry(v)
    if err != nil { return err }

    // Phase 1: Determine which versions need loading
    var toLoad []int
    for pos, ancestor := range ancestors {
        vc.mappedVersionsMu.RLock()
        _, found := vc.mappedVersions[ancestor]
        vc.mappedVersionsMu.RUnlock()
        if found {
            break // this and all deeper ancestors already loaded
        }
        toLoad = append(toLoad, pos)
    }
    if len(toLoad) == 0 { return nil }

    // Phase 2: Pre-populate mappedVersions (fast, no I/O)
    for _, pos := range toLoad {
        vc.mappedVersionsMu.Lock()
        vc.mappedVersions[ancestors[pos]] = getDistFromRoot(ancestors[pos:])
        vc.mappedVersionsMu.Unlock()
    }

    if !loadMutations { return nil }

    // Phase 3: Load mutation logs concurrently
    var g errgroup.Group
    g.SetLimit(8) // limit concurrent file reads per instance
    for _, pos := range toLoad {
        ancestor := ancestors[pos]
        ancestorSlice := ancestors[pos:]
        g.Go(func() error {
            ch := make(chan storage.LogMessage, 1000)
            wg := new(sync.WaitGroup)
            wg.Add(1)
            go vc.loadVersionMapping(ancestorSlice, d.DataName(), ch, wg)
            if err := labels.StreamLog(d, ancestor, ch); err != nil {
                return fmt.Errorf("loading mapping logs for %q, version %d: %v",
                    d.DataName(), ancestor, err)
            }
            wg.Wait()
            return nil
        })
    }
    return g.Wait()
}
```

**Note on `vc.mu.Lock()`**: This outer lock prevents other code paths from
calling `initToVersion` concurrently for the same VCache. During startup,
only one call occurs per VCache. The lock does NOT block `setMapping()` or
`mapLabel()` (read path) because those use only shard-level locks and
`mappedVersionsMu`, not `vc.mu`. So holding `vc.mu` during parallel loading
does not create a deadlock or bottleneck.

**Concurrency limit**: 8 concurrent file reads per instance is a reasonable
starting point. On SSD-backed storage this should be fine; on HDD it may need
to be reduced. Could be made configurable via TOML.

**Expected speedup:** If an instance has 100 ancestor versions, loading 8 at a
time instead of 1 gives roughly 4-6× speedup per instance (less than 8× due
to I/O bandwidth sharing). Combined with Tier 1, total startup could drop
from ~30 min to ~2-4 min.

---

### Tier 3: Stream File Reads Instead of `io.ReadAll()`

**Impact: LOW-MEDIUM | Risk: LOW | Effort: ~0.5 day**

Replace the `io.ReadAll()` in `StreamAll()` with buffered streaming reads
so the consumer goroutine can start processing entries before the file is
fully read.

**File: `storage/filelog/filelog.go:287-349`**

Replace lines 312-334 (the `io.ReadAll` + parse loop) with:

```go
reader := bufio.NewReaderSize(f, 256*1024) // 256KB read buffer
header := make([]byte, 6)
for {
    _, err := io.ReadFull(reader, header)
    if err == io.EOF { break }
    if err != nil { return err }
    entryType := binary.LittleEndian.Uint16(header[:2])
    sz := binary.LittleEndian.Uint32(header[2:6])
    databuf := make([]byte, sz)
    if _, err := io.ReadFull(reader, databuf); err != nil { return err }
    ch <- storage.LogMessage{EntryType: entryType, Data: databuf}
}
```

**Trade-off:** The current code slices one big `[]byte` (zero-copy per entry);
the streaming approach allocates a new `[]byte` per entry. For very large files
with many small entries, this could increase GC pressure. Profile before
committing. The main benefit is reducing time-to-first-entry and peak memory
per file read, which matters most when multiple versions are loaded concurrently
(Tier 2).

---

## Risk Analysis

### Memory Pressure
With Tier 1 + Tier 2, multiple instances loading multiple versions
simultaneously will have more data in flight. The semaphores (NumCPU for
instances, 8 for versions) bound this. Worst case: `NumCPU × 8` mutation
log files in memory simultaneously. Monitor RSS during startup and adjust
limits if needed.

### Disk I/O
On SSDs (typical for DVID), concurrent reads to different files are efficient.
On HDDs, random seeks from concurrent reads could actually be slower. The
concurrency limits provide a dial to tune this.

### Correctness
The `setMapping` shard-lock design already supports concurrent writes from
different goroutines. The key invariant — every ancestor version's mutations
are loaded before the VCache is used for queries — is maintained because
`initToVersion` holds `vc.mu.Lock()` for its entire duration and queries go
through `getMapping()` which calls `initToVersion` if the version isn't
loaded.

### Badger Mutation Cache
The `mutcache` setup (labelmap `Initialize()` lines 63-79) opens Badger
databases. Each instance opens a different path, so concurrent opens are fine.
Only the `mutcache` map write needs a mutex.

## Implementation Order

1. **Tier 1 first** — highest impact-to-risk ratio. Test with `go test -race`.
2. **Tier 2 second** — requires more careful testing. Verify mapping correctness
   by comparing `/api/node/{uuid}/{instance}/mappings` output before and after.
3. **Tier 3 last** — profile to confirm benefit before committing.

## Verification

1. **Race detector**: `go test -race -tags "badger filestore ngprecomputed" ./...`
2. **Correctness**: Start server, hit `/api/node/:master/{labelmap}/mappings`
   endpoint for each labelmap instance. Compare output to a baseline from the
   sequential implementation (binary diff or hash).
3. **Timing**: Compare `TimeInfof` log timestamps for "Initializing data instance"
   / "Finished initializing" before and after. The instances should overlap in
   time after Tier 1.
4. **Memory**: Monitor RSS during startup with concurrent loading to ensure it
   stays within acceptable bounds.

## Key Files

| File | What to Change |
|------|----------------|
| `datastore/repo_local.go:185-211` | Tier 1: parallelize Initialize() loop |
| `datatype/labelmap/labelidx.go:33-98` | Tier 1: sync.Once for indexCache, mutex for mutcache |
| `datatype/labelarray/labelidx.go:27-49` | Tier 1: sync.Once for indexCache |
| `datatype/labelmap/vcache.go:280-310` | Tier 2: parallel version loading in initToVersion() |
| `storage/filelog/filelog.go:287-349` | Tier 3: streaming file reads in StreamAll() |

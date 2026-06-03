# MaxLabel / MaxRepoLabel Tracking Bugs in Labelmap

**Date:** 2026-03-22
**Affected file:** `datatype/labelmap/labelmap.go`
**Dataset context:** mCNS DVID server (supervoxel IDs in 101M–2B range, NextLabel initialized low ~960K)

## Observed symptoms

From `GET /api/node/:master/segmentation/info`:

1. **MaxLabel decreases between parent → child versions.** For example, some versions show MaxLabel around 900K or 557K while neighboring versions show ~1B.
2. **MaxRepoLabel is 53,373,772,488 (53 billion)** — far exceeding any actual supervoxel ID in the dataset, which all fit in 32 bits.

## Background: How label IDs are allocated

DVID tracks three counters for label allocation:

- **`MaxLabel[v]`** — the maximum label ID seen at version `v`. Per the code comment: "Can be unset if no new label was added at that version, in which case you must traverse DAG to find max label of parent." However, this DAG traversal is **not implemented** for labelmap (only labelvol and labelarray have `adjustMaxLabels()`).
- **`MaxRepoLabel`** — monotonically-increasing counter across the entire repo. Used by `newLabel()` to allocate IDs when NextLabel is not set.
- **`NextLabel`** — optional override. When non-zero, `newLabel()` allocates from this counter instead of MaxRepoLabel, allowing new body IDs to use a lower number range than existing supervoxels.

The mCNS dataset uses `NextLabel` (currently ~959,578) so that newly allocated body IDs are short, while original supervoxel IDs occupy the 101M–2B range.

---

## Bug 1: `newLabel()` / `newLabels()` with NextLabel path skip MaxLabel tracking

**Location:** `labelmap.go:2342–2348` and `labelmap.go:2369–2377`

```go
func (d *Data) newLabel(v dvid.VersionID) (uint64, error) {
    d.mlMu.Lock()
    defer d.mlMu.Unlock()

    if d.NextLabel != 0 {
        d.NextLabel++
        if err := d.persistNextLabel(); err != nil {
            return d.NextLabel, err
        }
        return d.NextLabel, nil  // ← MaxLabel[v] NOT updated
    }
    d.MaxRepoLabel++
    d.MaxLabel[v] = d.MaxRepoLabel  // ← only this path updates MaxLabel[v]
    // ...
}
```

When `NextLabel != 0`, the function returns a new label from the NextLabel counter but **never updates `MaxLabel[v]` or `MaxRepoLabel`**. The same issue exists in `newLabels()`.

The callers that use explicit label IDs (e.g., `splitlabel != 0` in `mutate.go:937–939`) call `updateMaxLabel(v, label)` separately. But auto-allocated splits (`splitlabel == 0`) go through `newLabel()` and the returned label is never passed to `updateMaxLabel`.

**Effect:** On proofreading versions where the only label-creating operations are auto-allocated splits via NextLabel, `MaxLabel[v]` is either:
- Never set (if no other label operation touched this version)
- Set to a low value from an explicit-label operation

Parent versions from the original data ingest have `MaxLabel[v]` reflecting the true max SV (~2B), so child versions appear to "decrease."

**This directly explains the observed symptom of decreasing MaxLabel across versions.** Versions like 80 (MaxLabel=557,586) and 126 (MaxLabel=958,374) are showing values from the NextLabel range, not from actual supervoxel data.

---

## Bug 2: TOCTOU race condition in `updateMaxLabel()`

**Location:** `labelmap.go:2236–2262`

```go
func (d *Data) updateMaxLabel(v dvid.VersionID, label uint64) (changed bool, err error) {
    d.mlMu.RLock()
    curMax, found := d.MaxLabel[v]
    if !found || curMax < label {
        changed = true
    }
    d.mlMu.RUnlock()        // ← releases read lock
    if !changed { return }
    d.mlMu.Lock()            // ← acquires write lock (gap between locks!)
    defer d.mlMu.Unlock()
    d.MaxLabel[v] = label    // ← overwrites WITHOUT re-checking current value
    // ...
}
```

Between `RUnlock()` and `Lock()`, another goroutine can acquire the write lock and set `MaxLabel[v]` to a higher value. This goroutine then overwrites it with the lower value.

**Example race:**
1. Goroutine A (label=100): reads MaxLabel[v]=50, changed=true, releases read lock
2. Goroutine B (label=200): reads MaxLabel[v]=50, changed=true, releases read lock
3. Goroutine B: acquires write lock, sets MaxLabel[v]=200
4. Goroutine A: acquires write lock, sets MaxLabel[v]=100 ← **regression!**

Note: `MaxRepoLabel` is partially protected by its own `if label > d.MaxRepoLabel` check inside the write lock, so the race primarily affects `MaxLabel[v]`, not `MaxRepoLabel`.

---

## Bug 3: TOCTOU race condition in `updateBlockMaxLabel()`

**Location:** `labelmap.go:2266–2293`

Same pattern as Bug 2. The block label scan happens outside the write lock, so the computed `curMax` may be stale by the time it's written:

```go
func (d *Data) updateBlockMaxLabel(v dvid.VersionID, block *labels.Block) {
    d.mlMu.RLock()
    curMax, found := d.MaxLabel[v]
    d.mlMu.RUnlock()
    // ... scan all labels in block, find blockMax ...
    if changed {
        d.mlMu.Lock()
        d.MaxLabel[v] = curMax  // ← could overwrite a higher value
        // ...
    }
}
```

This function is called concurrently via goroutines (`go d.updateBlockMaxLabel(...)` in `write.go:193, 347, 467, 506`), making the race practical.

---

## Bug 4: `loadLabelIDs()` assigns 10 billion to corrupted versions

**Location:** `labelmap.go:2461–2467`

```go
var label uint64 = veryLargeLabel  // 10,000,000,000
if len(kv.V) != 8 {
    dvid.Errorf("Got bad value.  Expected 64-bit label, got %v", kv.V)
} else {
    label = binary.LittleEndian.Uint64(kv.V)
}
d.MaxLabel[v] = label  // ← 10B if corrupted!
```

If any version's MaxLabel key has a corrupted value (not exactly 8 bytes), it defaults to 10 billion. This feeds into `repoMax` and can inflate `MaxRepoLabel`.

---

## MaxRepoLabel = 53 billion — probable cause

The 53,373,772,488 value appears only on version 35 in the MaxLabel map. The most likely causes:

1. **Corrupted block data in `updateBlockMaxLabel()`:** During block ingest, the function scans all 64-bit values in `block.Labels`. If a decompressed label block contained garbage data (from storage corruption, truncation, or an encoding bug), any large 64-bit value would be accepted as the new MaxLabel[v] and propagate to MaxRepoLabel. There is **no validation** that scanned label values are reasonable.

2. **The 10B fallback from Bug 4** could produce a high MaxRepoLabel, though 53B != 10B specifically. Multiple corrupted versions combined with the race conditions could push it higher.

3. **Less likely:** A large batch `POST /nextlabel/<count>` when NextLabel was 0 would allocate from MaxRepoLabel, but would require a count of ~51B.

Without server logs from the time version 35 was active, the exact trigger cannot be confirmed. Once MaxRepoLabel is inflated, it's persisted and never decreases (by design — it's a monotonic counter).

---

## Data corruption and label collision analysis

### Can these bugs cause label ID collisions?

**Yes, under specific conditions.** `newLabel()` and `newLabels()` allocate from MaxRepoLabel (or NextLabel) with **no existence check** — they assume the counter is above all existing labels. If the counter is wrong, collisions are silent.

### Scenario analysis for mCNS

| Scenario | Risk | Assessment |
|----------|------|------------|
| **Current state (NextLabel=~960K, MaxRepoLabel=53B)** | No collision | NextLabel allocates in 960K range, far below existing SVs (101M+). MaxRepoLabel is too high (53B), which is wasteful but safe. |
| **NextLabel wraps into SV range** | Collision when NextLabel reaches ~101M | At ~960K now with ~100M headroom. Not imminent, but **no guard exists**. If proofreading generates 100M+ splits over the dataset's lifetime, collisions would occur silently. |
| **NextLabel disabled (set to 0)** | No collision | Allocations fall back to MaxRepoLabel (53B), well above all existing labels. |
| **Server restart with corrupted maxRepoLabelTKey** | Collision possible | MaxRepoLabel is recomputed as `max(MaxLabel[v])`. Due to Bug 1, MaxLabel[v] values for proofreading versions are artificially low (~960K). If the stored maxRepoLabelTKey is corrupted, MaxRepoLabel could be set to ~1B (from ingest versions), which is correct. But if ingest-version MaxLabel keys are also corrupted, MaxRepoLabel could be far too low. |
| **Compound failure: Bug 1 + Bug 4 + corrupted repo key** | Collision | Bug 1 produces low MaxLabel[v] for proofreading versions. Bug 4 produces 10B for corrupted ingest versions. If maxRepoLabelTKey is also corrupted, MaxRepoLabel = max(low values, 10B) = 10B. Future allocations from MaxRepoLabel start at 10B+1, which is above 2B SVs, so still safe. But if Bug 4 didn't fire (no corrupted versions), MaxRepoLabel = max(low NextLabel values) = ~960K → **direct collision with existing SVs on next allocation.** |

### What would a collision look like?

If a newly allocated label ID matches an existing supervoxel:
- The new SV's mapping (supervoxel → body) overwrites or conflicts with the existing SV's mapping
- Merge/split/cleave operations on either SV would affect both
- Label index entries would be corrupted (two different spatial regions sharing one label)
- **Segmentation data appears silently wrong** — no error is raised

### Servers without NextLabel

For DVID servers that do **not** use NextLabel:
- All allocations go through `MaxRepoLabel++`
- The TOCTOU race (Bug 2) can regress `MaxLabel[v]` but MaxRepoLabel is protected by the inner `if label > d.MaxRepoLabel` check at runtime
- The primary collision risk is the restart scenario: corrupted maxRepoLabelTKey + incorrect MaxLabel[v] values → MaxRepoLabel set too low

### Missing DAG traversal

The code comment on `MaxLabel` (labelmap.go:1938–1939) states: "Can be unset if no new label was added at that version, in which case you must traverse DAG to find max label of parent." This DAG traversal is implemented in `labelvol.go` and `labelarray.go` (as `adjustMaxLabels()`) but **not in labelmap**. This means:
- `GET /maxlabel` returns an error for versions with no MaxLabel entry
- Clients cannot reliably determine the true maximum label for a version
- The `loadLabelIDs()` recovery logic cannot fall back to parent version values

---

## Recommended fixes

### Immediate (low risk)

1. **Fix `newLabel()` / `newLabels()` NextLabel path** to update `MaxLabel[v]` when the new label exceeds it. This makes MaxLabel tracking correct regardless of which allocation path is used.

2. **Fix the TOCTOU race** in `updateMaxLabel()` and `updateBlockMaxLabel()` by re-checking the current value after acquiring the write lock.

3. **Fix `loadLabelIDs()` corrupted version handling** — skip corrupted versions instead of assigning 10 billion.

### Medium-term

4. **Add NextLabel upper-bound guard** — when NextLabel reaches a configurable threshold (or approaches the minimum known supervoxel ID), log a warning or error.

5. **Add `repair-maxlabel` RPC command** that recomputes MaxLabel[v] and MaxRepoLabel from actual label index data, allowing correction of the mCNS 53B value without manual database surgery.

6. **Implement DAG traversal for MaxLabel** (`adjustMaxLabels()` equivalent for labelmap) so that child versions inherit their parent's MaxLabel when no label operations have occurred on the child.

### Validation

7. **Add existence check in `newLabel()`** — after allocating a new ID, verify it doesn't appear in the label index. This is a safety net, not a primary fix, since correct counter tracking should prevent collisions.

---

## Appendix: Code locations

| Function | File | Lines | Purpose |
|----------|------|-------|---------|
| `newLabel()` | labelmap.go | 2337–2358 | Allocate single new label |
| `newLabels()` | labelmap.go | 2360–2389 | Allocate batch of new labels |
| `updateMaxLabel()` | labelmap.go | 2236–2262 | Update per-version max |
| `updateBlockMaxLabel()` | labelmap.go | 2266–2293 | Update max from block ingest |
| `loadLabelIDs()` | labelmap.go | 2447–2512 | Load counters at startup |
| `persistMaxLabel()` | labelmap.go | 2300–2312 | Persist per-version max |
| `persistMaxRepoLabel()` | labelmap.go | 2314–2323 | Persist repo-wide max |
| `handleMaxlabel()` | handlers.go | 1580–1628 | GET/POST /maxlabel endpoint |
| `handleNextlabel()` | handlers.go | 1630–1676 | GET/POST /nextlabel endpoint |
| Split (auto-alloc) | mutate.go | 942, 950 | `newLabel(v)` for split SVs |
| Split (explicit) | mutate.go | 939, 947 | `updateMaxLabel(v, label)` |
| Cleave | mutate.go | 349 | `newLabel(v)` for cleave |
| Block ingest | write.go | 193, 347, 467, 506 | `go d.updateBlockMaxLabel()` |

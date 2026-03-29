# Fix Pre-existing Data Race on `manager` Variable

## Problem

The race detector flags a data race between tests in the `datatype/labelmap` package:

```
Write at datastore.Initialize() → repo_local.go:130  (manager = m)
Read  at datastore.NotifySubscribers() → pubsub.go:166  (if manager == nil)
```

**Root cause:** The package-level `var manager *repoManager` in `datastore/datastore.go`
is read and written without synchronization. In production this is fine (written once at
startup, then only read), but in tests each `OpenTest()`/`CloseTest()` cycle reinitializes
the variable. Background goroutines from a previous test can still be reading `manager`
when the next test's `Initialize()` overwrites it.

### How the goroutine outlives CloseTest

1. `storeBlocks` (write.go) spawns `go callback(...)` goroutines (line 515/521)
2. Each callback calls `datastore.NotifySubscribers()` which reads `manager`
3. `putWG.Wait()` ensures all callbacks complete before the HTTP handler returns
4. But `NotifySubscribers` sends messages on sync channels (`sub.Ch <- m`), and
   sync goroutines consuming those channels may still be running
5. `CloseTest()` → `Shutdown()` waits for `d.Updating()` to drain, but doesn't
   guarantee all sync channel consumers have finished
6. Next test's `OpenTest()` → `Initialize()` writes `manager = m` → **RACE**

### Scope

The `manager` variable has ~66 read sites + 2 write sites across 6 files in `datastore/`:

| File | Reads | Writes |
|------|-------|--------|
| `datastore.go` | ~51 | 0 |
| `repo_local.go` | ~5 | 2 |
| `copy_local.go` | ~6 | 0 |
| `push_local.go` | ~2 | 0 |
| `datainstance.go` | ~3 | 0 |
| `pubsub.go` | ~4 | 0 |

Almost all reads follow the same pattern:
```go
if manager == nil {
    return ..., ErrManagerNotInitialized
}
return manager.someMethod(...)
```

## Implementation Plan

### 1. Replace `manager` with `atomic.Pointer[repoManager]` + accessors

In `datastore/datastore.go`:

```go
import "sync/atomic"

var managerPtr atomic.Pointer[repoManager]

// getManager returns the current repo manager, or nil if not initialized.
func getManager() *repoManager {
    return managerPtr.Load()
}

// setManager atomically sets the repo manager.
func setManager(m *repoManager) {
    managerPtr.Store(m)
}
```

### 2. Update all read sites (mechanical)

Replace the common pattern across all files:

```go
// Before:
if manager == nil {
    return ..., ErrManagerNotInitialized
}
return manager.someMethod(...)

// After:
m := getManager()
if m == nil {
    return ..., ErrManagerNotInitialized
}
return m.someMethod(...)
```

For functions with multiple `manager` accesses (e.g., `DeleteConflicts`,
`GetStorageDetails`), load once at the top and use `m` throughout.

### 3. Update the 2 write sites

```go
// repo_local.go:130 and :274
// Before:
manager = m
// After:
setManager(m)
```

### 4. Nil out manager in Shutdown

In `datastore.go` `Shutdown()`, set `manager` to nil after shutdown completes
so orphan goroutines hit the nil check and return `ErrManagerNotInitialized`:

```go
func Shutdown() {
    m := getManager()
    if m == nil {
        return
    }
    m.Shutdown()
    setManager(nil)
}
```

### 5. Verify

- `make test` — all tests pass
- `go test -race ./datastore/...` — no races in datastore
- `go test -race ./datatype/labelmap/...` — no races in full suite
  (except `TestShardHandlerWithRealSpecs` timeout, which was removed separately)

## Notes

- The change touches ~68 call sites across 6 files — entirely mechanical
- Zero behavior change in production — `atomic.Pointer.Load()` is a single
  atomic read, same cost as reading a plain pointer on x86/arm64
- The `getManager()` pattern also improves correctness: callers hold a
  stable reference to the manager, preventing TOCTOU issues if `Shutdown`
  nils it out between the nil-check and the method call

## Production Safety (verified 2026-03-28)

1. **Go version**: `go.mod` specifies Go 1.25.0; `atomic.Pointer` requires Go 1.19+
2. **ReloadMetadata** (repo_local.go:274): Uses `dvid.DenyRequests()` to block HTTP
   traffic before swapping `manager`. This is still needed with `atomic.Pointer` —
   the atomic swap ensures pointer visibility but does not prevent in-flight requests
   from operating on stale repo data structures
3. **Shutdown nil-out**: New behavior — sets manager to nil after `m.Shutdown()`.
   Safe in production (server is terminating) and is the key fix for the test race:
   orphan goroutines hit the nil check and return `ErrManagerNotInitialized`
4. **No external access**: `manager` is unexported; all ~68 access sites are within
   the `datastore` package. No `//go:linkname` or other directives reference it

# Design exploration: recovering deleted neuronjson values

> **Status: UNSETTLED — this is a thinking/record document, not an approved plan.**
> The endpoint's usefulness turned out to depend on a storage-retention subtlety, and there
> is an open architectural fork (modify the backends vs. keep the logic in the neuronjson
> datatype). Nothing here is committed to. The verified code facts (below) hold regardless
> of which approach is chosen; the "Architectural options" section is where the real
> decision lives.

## Problem statement

A FlyEM service, after a labelmap merge of bodies `Y1..YN` into a target, deletes the
merged-away bodies' annotations via HTTP `DELETE` on the `*_annotations` neuronjson
instance. We want a way to **recover the annotation content of a deleted body** — ideally
"the value it had immediately before it was deleted."

The naive assumption was that this is a simple read-side feature (scan the versioned store,
return the last non-tombstone value). Investigation showed that whether the *true*
pre-delete value is even still in the store depends on the editing workflow — so the design
splits into "what can we recover from what's already stored" vs. "should we change how data
is stored/deleted so the value is always recoverable."

## Workflow reality (corrects an earlier wrong assumption)

An earlier draft assumed deletes happen on a "fresh node" with no same-version edits to the
target bodies, and therefore that a store-scan "almost always" returns the true pre-delete
value. **That assumption was unfounded and is wrong.** Corrected understanding (from the
repo owner):

- Deletes land on whatever **open (uncommitted) HEAD node** the proofreading session is
  using. That node **accumulates** annotation edits, merges, and merge-cleanup deletes
  together, and is typically committed some time later.
- Whether the true pre-delete value survives depends on **where the body's value was last
  written** relative to the delete:
  - Last written in a **prior committed** version, delete-only on the open node → the delete
    just adds a tombstone at HEAD; the committed value survives → **recoverable, and it is
    the true pre-delete value.**
  - **Written on the same open node** before the delete (body edited — or created via a
    split — then merged away in the same session) → that data key is point-deleted (fact 2)
    → only an older committed value (or nothing) remains → **the true last value is lost.**
- "Committed shortly after" does **not** rescue the lost value: the edit→delete collapse to
  a single tombstone happens *within* the open version, before the commit; committing only
  freezes that final tombstone state.
- **Frequency of the lossy case is workflow-dependent and unknown to the author of this
  doc** — it is the repo owner's domain knowledge. Since the "fresh node" premise is
  rejected, the lossy case is not assumed negligible, and it is *silent* (a store-scan
  returns a stale value with no signal that a newer one was destroyed).

This is the crux: a pure read-side feature (Option A) cannot recover what the store no
longer holds. Guaranteeing recovery requires capturing the value at/for the delete (Option
B) or changing storage so nothing is destroyed (Option C).

## Background / key facts established from the code

Load-bearing facts (file:line for the implementer; verify they haven't drifted). These hold
for all options.

1. **Key layout + sparse, copy-on-write storage.** A neuronjson data key is
   `[dataKeyPrefix][instanceID][TKey][versionID][clientID][marker]`
   (`storage/context.go:452`). For a fixed `TKey` all versions are contiguous, sorted
   ascending by `versionID` (big-endian). `putStoreData` stamps ONE entry at the current
   write version (`neuronjson.go:901`); reads resolve up the DAG. ⇒ the store holds **one
   entry per actual change-point** (a write or a delete-tombstone), not one per version.

2. **Per-version retention = final state only.**
   The data key and the tombstone are **distinct keys** for the same instance/TKey/version/
   client, differing only in the final marker byte: `MarkData=0x03` (`ConstructKey` →
   `constructDataKey`, `context.go:452-457`) vs `MarkTombstone=0x4F` (`TombstoneKey`,
   `context.go:436-441`). There is no "tombstone bit" on a single record.
   - `Put` at version V (`badger.go:1206-1211`): `Set`s the data key AND point-`Delete`s the
     tombstone key at V.
   - `Delete` at version V (`badger.go:1258-1265`): point-`Delete`s the data key (`0x03`) at
     V AND `Set`s the separate tombstone key (`0x4F`) at V.
   Both are **point** operations on the *current* version's keys (the version ID is embedded
   in the key), so:
   - A value written at the **same** version as a later delete is physically removed — only
     the tombstone survives at that version (one value XOR one tombstone; no intra-version
     history).
   - Data keys at **ancestor** versions are different keys and are NOT touched — committed
     ancestor values survive, masked at read time by DAG resolution hitting the nearer
     tombstone.
   Nothing prunes ancestor values in normal operation (the `flatten`/prune code in
   `copy_local.go`/`push_local.go` targets a new export store).

3. **Each retained value is a FULL document snapshot (normal write paths).**
   `storeAndUpdate` → `updateJSON` (`neuronjson.go:1512`, `:1446-1465`) carries forward
   unchanged fields; `replace=true` still runs `updateJSON` and adds `_user`/`_time`
   (`:1468-1508`). **Caveat:** the `import-kv` path writes raw source bytes directly
   (`memstore.go:266-272`), bypassing `storeAndUpdate`; such entries may not be full docs.

4. **Stored value encoding is plain JSON.** `putStoreData` stores `json.Marshal(value)`;
   `kv.V` from a range query is the JSON body verbatim (no DVID serialization wrapper;
   Badger block compression is transparent). Tombstone entries have empty values.

5. **The value is also emitted to the debug log — but that is NOT recovery infrastructure.**
   Every put logs `dvid.Infof(... "Orig: %s ... New: %s")` (`neuronjson.go:1536`), so the
   sequence of values exists in the logs. But it is unstructured, un-versioned, subject to
   rotation, and not queryable — usable for forensic spelunking, not as a feature backend.

6. **neuronjson has an in-memory db (memstore) layered over the kv store.**
   `getMemDBbyVersion` holds the flattened current state for a version (no history). On
   delete, `DeleteData` does `delete(mdb.data, bodyid)` (`neuronjson.go:1680`), clearing it.
   Reads for the HEAD/mutable version come from the memdb; the kv store is the persistent
   source rebuilt on load. **Key point for Option B:** at the *top* of `DeleteData`, before
   the clear, `mdb.data[bodyid]` still holds the exact current value — the cleanest capture
   point, no DAG resolution needed. (Deletes only occur on open/unlocked nodes, which have a
   memdb.)

7. **Tombstone detection.** `Key.IsTombstone()` (`storage/context.go:156`) checks the final
   marker byte.

8. **Range bounds for one key.** `ctx.MinVersionKey(tk)` / `MaxVersionKey(tk)`
   (`storage/context.go:639,648`) bound exactly that key's versions and span both markers.
   `RawRangeQuery` (`badger.go:1147`) returns raw keys with NO DAG/tombstone filtering (so
   it surfaces tombstones). **It emits on the output channel during the call**, so start a
   consumer goroutine before calling it (else deadlock); pattern in `version_files.go:82-134`.

9. **Ancestor-set primitive already exists — reuse it.** `repoManager.ancestorsClosure(v)`
   (`datastore/public.go:96`) is the full DAG-aware closure (all parents, revisit-guarded,
   returns `map[dvid.VersionID]struct{}` incl. `v`). Unexported (only caller `public.go:52`);
   add a thin exported wrapper. NOTE: the existing exported `GetAncestry(v)`
   (`datastore.go:384`) is **linear** (`parents[0]` only) and would drop a merged-in
   branch's ancestors — do NOT use it here.

10. **Ordering.** Version IDs are assigned monotonically at node creation
    (`repo_local.go:951-955`); ancestor paths have strictly decreasing IDs
    (`copy_local.go:935,938`). ⇒ after filtering change-points to `v`'s ancestor set, sort
    by descending version ID for newest→oldest (a topological order, sufficient for "newest
    retained value").

11. **HTTP routing.** `ServeHTTP` (`neuronjson.go:2283`): `parts[3]` = command, `parts[4]` =
    key; `ctx` carries the resolved `VersionedCtx`. New endpoint adds a `parts[3]` case.

## Architectural options (the real decision)

Fidelity ladder: A recovers only what the store still holds; B guarantees the
value-at-deletion; C preserves everything.

### Option A — read-only store scan (weakest, no new mechanism)

Scan the key's existing entries, return the newest non-tombstone value on the branch.
- **Mechanism:** `RawRangeQuery` over the key's version range → filter to ancestor set →
  newest non-tombstone. (Detailed in Implementation §A/§B below.)
- **Pro:** read-only, zero write overhead, no new keyspace, no backend change, smallest diff.
- **Con:** **cannot** recover the same-version edit-then-delete value — it is already gone
  (fact 2). Per the corrected workflow this gap is real and silent. Good as a *fallback*,
  insufficient as a *guarantee*.

### Option B — datatype-level capture-on-delete (neuronjson "graveyard")

Preserve the value at delete time, in neuronjson, using existing storage primitives.
- **Mechanism:** in `DeleteData`, before clearing, capture `mdb.data[bodyid]` (fact 6) and
  write it to a **separate versioned TKey class** (e.g. `keyDeletedAnnotation`) keyed by
  bodyid, at the current version. The `lastvalue` read: if the body is currently tombstoned,
  return the graveyard value at the nearest ancestor (the delete version); else return the
  live value. A later re-create supersedes naturally (live value wins).
- **Pro:** contained entirely to the neuronjson package — no backend, key-format, storage
  interface, or migration changes. Recovers the true value-at-deletion **including** the
  same-version edit-then-delete case. Bounded cost: one extra versioned write per delete
  (comparable to the tombstone already written). The separate TKey class keeps it out of
  normal annotation scans/queries and the memstore rebuild.
- **Con:** bespoke to neuronjson; captures the value *at deletion*, not full intra-version
  edit history (which is all we need here). Must be threaded through copy/flatten/push if
  the graveyard should travel with the instance, and the memstore loader must skip the
  graveyard class. Grows with number of deletes (as tombstones already do).
- Matches the repo owner's leaning: "since this is really only intended for one datatype …
  build off what's available and have that logic in the datatype implementation."

### Option C — backend-level "append-only" data instances

Mark certain instances append-only so the backend never destroys data; all writes/deletes
append; full history is retained and retrievable.
- **What it requires (per repo owner: "modify the TKey and the retrieval logic"):**
  - An **instance-level append-only flag** the backend/context can see.
  - **Key-format change:** today a `Put` within a version overwrites the same key and a
    `Delete` point-deletes it (fact 2). To retain every write, each write must produce a
    distinct key — e.g. add a monotonic sequence/timestamp component to the versioned key
    suffix so nothing is overwritten, and make `Delete` append a tombstone entry rather than
    point-delete. This is the "modify the TKey" piece.
  - **Retrieval-logic change:** reads must select the *latest* entry (max seq) for the
    resolved version and honor tombstones; `findMatch` / `VersionedKeyValue` / the range
    grouping in `sendKV` currently assume one entry per version and would need to group by
    seq. A "value before delete" read walks back to the latest non-tombstone.
  - **Backend coverage:** `Delete`/key construction live per backend (badger) plus the
    `storage` interface contract; append-only must be implemented consistently there.
  - **Operational:** unbounded growth for append-only instances (the point, but a real
    cost); may need an explicit compaction/"forget" policy.
  - **Compatibility/migration:** changing the key layout affects reading existing stores;
    needs a per-instance/versioned key-format flag. Key-format changes are sensitive in DVID
    (cf. the Basho LevelDB corruption caveat in CLAUDE.md/GUIDE.md).
- **Pro:** general capability any datatype could opt into; preserves *full* history (would
  also resurrect the original "Nth prior value" idea); "correct" data-preservation at the
  storage layer; recovery becomes trivial for consumers.
- **Con:** by far the most invasive — key format, retrieval, every backend, migration, and
  unbounded storage. And only neuronjson is known to need it, so it risks over-engineering a
  global mechanism for a single consumer.

### Rough recommendation (to be confirmed)

If the goal is a reliable "recover the deleted body's value" feature for neuronjson and
nothing else needs append-only semantics, **Option B** is the best fidelity/effort trade:
it guarantees the value-at-deletion, stays within one package, and avoids storage-format and
migration risk. **Option A** can ship first as a read-only fallback (covers the
prior-committed-value case) and be superseded/augmented by B. **Option C** is worth it only
if append-only data preservation is wanted as a general DVID capability (multiple datatypes,
or a full-history/audit requirement), which would justify the storage-layer investment.

## Endpoint shape (Options A/B)

```
GET  <api URL>/node/<UUID>/<data name>/lastvalue/<key>
```
**[DECISION A — endpoint name. Candidates: `lastvalue`, `recover`, `value-before-delete`.]**

- `<key>`: bodyid string (same parsing as the `key` endpoint).
- Optional query params: `show=user|time|all|none`, `fields=...` applied via existing
  `selectFields`/`showFields(r)`/`fieldMap(r)`. **[DECISION B — field selection in v1 or defer.]**
- Response: `200` + JSON body when found; `404` when the key has no retained/recoverable
  value on the branch.
- Headers on `200`: `X-DVID-Version`, `X-DVID-UUID` (`datastore.UUIDFromVersion`), and
  `X-DVID-Deleted: true` when the body's current resolved state at the requested version is
  a tombstone (value was recovered).

## Implementation sketch

### §A. datastore: export the ancestor-set closure (needed by A and B)
Thin exported wrapper over `ancestorsClosure` (do NOT write new traversal), mirroring
`GetParentsByVersion`/`GetAncestry` (`datastore.go:365,384`):
```go
func GetAncestrySet(v dvid.VersionID) (map[dvid.VersionID]struct{}, error) {
    m := getManager()
    if m == nil { return nil, ErrManagerNotInitialized }
    return m.ancestorsClosure(v)
}
```

### §B. neuronjson: Option A read path (also the fallback under B)
`func (d *Data) getLastValue(ctx, keyStr) (value []byte, fromVersion dvid.VersionID, currentlyDeleted, found bool, err error)`
- `tk := NewTKey(keyStr)`; scan `RawRangeQuery(MinVersionKey(tk), MaxVersionKey(tk), false, ch, cancel)`
  with the consumer-goroutine pattern (fact 8); propagate scan/`ValueCopy` errors.
- Filter kvs to `GetAncestrySet(ctx.VersionID())`; sort by descending version ID; dedupe/error
  on repeated version IDs (scan spans all client IDs though `VersionFromKey` ignores client).
- `currentlyDeleted` = newest kept entry `IsTombstone()`; return the first `!IsTombstone()`
  entry (value+version), else `found=false`.
- Consistency: scan is a Badger `View` snapshot; writes hit memdb before store, so an
  in-flight write may be invisible. **[DECISION D — document snapshot semantics vs. lock.]**

### §C. neuronjson: Option B capture-on-delete (if chosen)
- New TKey class `keyDeletedAnnotation` in `keys.go`; encode/decode by bodyid like the
  annotation class.
- In `DeleteData` (`neuronjson.go:1652`): before `delete(mdb.data, bodyid)`, if the body has
  a current value, `Put` it under the graveyard class at `ctx` (versioned). Then proceed with
  the existing tombstone delete.
- `getLastValue`: if `currentlyDeleted`, read the graveyard entry for the resolved
  delete-version (DAG-resolved) and return it; fall back to the Option A scan if absent
  (e.g. pre-existing deletes with no graveyard entry).
- Thread the graveyard class through the memstore loader (skip it), and through
  copy/flatten/push if it must travel with the instance.

### §D. sketch only — Option C
Not detailed pending the architecture decision; see the "what it requires" list above.

### §E. Docs & tests (Options A/B)
- **Required:** update `helpMessage` (returned verbatim by `Help()`, `neuronjson.go:2215`) —
  endpoint, "looks past a current deletion", `X-DVID-Deleted`, 404 case, and the retention
  limitation (edit+delete within one uncommitted version).
- Regenerate `docs/datatype-neuronjson.txt` if generated from `helpMessage`.
- Tests in `neuronjson_test.go`: value present (== GET); post-then-delete across committed
  versions (recovers, `X-DVID-Deleted`); **create+edit+delete within one uncommitted version**
  (Option A ⇒ 404/stale; Option B ⇒ recovers the captured value — the discriminating test);
  re-create after delete; delete after re-create; branch isolation; `import-kv` entry
  (DECISION C); field selection (DECISION B).

## Open decisions

- **DECISION 0 (top-level): which architecture** — A (read-only fallback), B (datatype
  capture-on-delete; recommended if only neuronjson needs it), or C (backend append-only;
  only if append-only is wanted as a general DVID capability). A and B can ship together.
- DECISION A: endpoint name.
- DECISION B: field selection in v1 or defer.
- DECISION C: `import-kv`-imported (possibly non-full-document) entries — verbatim vs flag.
- DECISION D: read consistency vs in-flight writes — snapshot-at-store vs lock.
- If Option B: does the graveyard travel with copy/flatten/push, and what (if any) is its
  compaction/retention policy?
- If Option C: key-format/versioning strategy for backward compatibility; which backends;
  storage-growth/compaction policy; whether to also expose full-history ("Nth") reads.

## History of the thinking (why the spec moved)

1. **Original idea:** "Nth-prior changed content" endpoint (`H[N]`, newest→oldest over the
   DAG ancestry). Dropped: per-edit history is not retained (fact 2); the natural unit is the
   committed version, and Nth added indexing/tombstone/merge-conflict complexity with no
   reliable payoff.
2. **Second idea:** "return the most recent non-tombstone value, looking past a current
   deletion" (`lastvalue`, Option A). Simpler; removed the Nth-era decisions.
3. **Limitation surfaced:** `lastvalue` is only "last value in *previous* versions," not the
   true pre-delete value, when the body was written and deleted within one uncommitted
   version (fact 2). An earlier "fresh node, almost always correct" defense was wrong — see
   "Workflow reality."
4. **Current fork:** guarantee recovery via datatype capture-on-delete (Option B) or a
   backend append-only mode (Option C), vs. accept Option A's limitation. Undecided.

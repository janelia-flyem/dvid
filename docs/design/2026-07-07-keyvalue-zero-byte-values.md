# Keyvalue: store and return zero-byte values consistently

**Date:** 2026-07-07
**Affected files:** `datatype/keyvalue/keyvalue.go` (primary); `storage/badger/badger.go` (recommended); `docs/datatype-keyvalue.txt` (regen); `dvid/serialize.go` (context only)
**Dataset context:** mCNS DVID server (`emdata6.int.janelia.org:9000`, `v1.0.0-235-g71fcc`), instance `segmentation_meshes`, open HEAD version 141 (UUID `5e616ad5…`)

> **Status: PLAN — not implemented.**
> **Decision (reversed from the earlier "empty POST = delete" draft):** a keyvalue POST of zero
> bytes stores a genuine zero-byte value, and **all read endpoints must return that key as
> present with its empty value** — `GET /key` → 200 with an empty body; batch and range reads
> include it with an empty value — consistent with `/keyrange`, `/keys`, and `HEAD`. An empty
> value is a legitimate value, not a deletion.

## Problem statement

`/keyrange`, `/keys`, and `HEAD /key` report a key as present, but `GET /key` returns 404 and
`/keyrangevalues` omits it. Reported against `segmentation_meshes` on the mCNS server
(reporter's example `400355.ngmesh`; still-broken examples `400394`, `400402`, `400426`,
`400451`, `400455`, `100058`, `500112511`, ~1% of a 774-key sample). All affected keys were
written with a zero-byte body (`postkv Bytes:0`) at version 102 and never rewritten.

The desired behavior: those keys should read back as present with a zero-length value, matching
what the key-listing endpoints already report.

## Two facts that make this a read-only fix

1. **The write path already stores an empty value correctly — no write change needed.**
   `SerializeData([])` returns `[]byte{}` (`dvid/serialize.go:188-191`); `PutData` therefore
   calls `Put(tk, []byte{})`; badger `Put` does `txn.Set(dataKey, [])` **and**
   `txn.Delete(tombstoneKey)` (`storage/badger/badger.go:1204-1213`). So a zero-byte POST
   already produces a *present, empty-valued, non-tombstoned* data key. That is exactly the
   desired stored state.

2. **No data migration is needed.** Because the desired end state is "present with empty value,"
   the ~1% of keys already stored empty at v102 become correct the moment the read paths are
   fixed — they will simply start returning 200 with an empty body. (Whether a merged-away body
   *should* carry an empty mesh is orthogonal — that's mesh regeneration, not keyvalue.)

## Root cause (read side)

The store cannot distinguish "absent" from "present-but-empty," and several read paths resolve
that ambiguity as "absent/drop":

- **Ambiguous `Get`.** `Get(ctx, tk) ([]byte, error)` returns `nil` for **both** absent and
  present-empty: for a found key with an empty value, badger's `item.ValueCopy(nil)` yields
  `nil` (`storage/badger/badger.go:406-417`).
- **`GetData` treats nil as not-found.** `if data == nil { return nil, false, nil }`
  (`datatype/keyvalue/keyvalue.go:587`) → `GET /key` 404. This is the root for single-key and
  **all** batch gets, which funnel through `getKeyReadValue` → `GetData`
  (`:846-861`, callers at `:850, :870, :1346, :1381, :1411`).
- **Range/stream paths drop empty values.**
  - `sendJSONValuesInRange` (`/keyrangevalues`): `if kv.V == nil { return nil }`
    (`:1254`) skips the key entirely.
  - `StreamKV` (full-instance scan used by copy/backup and unqualified value dumps):
    `if kv.V == nil { return nil }` (`:1181`).
- **`DeserializeData` is already empty-safe.** `DeserializeData(nil/[]byte{})` returns
  `([]byte{}, Uncompressed, nil)` (`dvid/serialize.go:300-302`) — so once a present-empty key is
  *not* discarded, it deserializes cleanly to an empty value.

Note the single-key `GET` HTTP handler already tolerates an empty value (it sets Content-Length
0 and writes no body — `:1088-1099`); the *only* thing turning it into a 404 is `GetData`
returning `found=false`.

## Fix approach

Make "present" independent of value length across the read paths.

### Option 1 — tighten the storage `Get` contract (recommended)

Define the contract: **`Get` returns `nil` iff the key is absent; a present key returns a
non-nil, possibly zero-length slice.** Implement in badger:

- `Get` (`badger.go:383-435`): when `txn.Get` succeeds (key present), return a non-nil slice —
  coerce a nil `ValueCopy` result to `[]byte{}`. Return `nil` only when the resolved key is nil
  or `txn.Get` returns `ErrKeyNotFound`.
- `loadVersionedValue` (`badger.go:541-558`, used by the range/stream value paths): likewise
  coerce a found winner's value to non-nil.

Effects, with no datatype logic change beyond the drops below:
- `GetData`'s `if data == nil` now correctly means absent; present-empty flows `[]byte{}` →
  `DeserializeData([]byte{})` → `[]byte{}` → `found=true` → `GET` 200 empty. Fixes single + all
  batch gets.
- In the range/stream callbacks, `kv.V` is `[]byte{}` (non-nil) for present-empty, so the
  `kv.V == nil` guards (`:1181`, `:1254`) no longer drop them; keep the guards only as
  protection against a genuinely unloaded value.
- Apply the same coercion to `basholeveldb` `Get` for parity (legacy backend).
- **Risk to audit:** any caller that treats `len(Get(...)) == 0` as "absent." `Exists` and
  `GetVersion` are unaffected (they key off the resolved key, not the value).

### Option 2 — datatype-layer disambiguation (fallback)

Leave storage as-is. In `GetData`, when `db.Get` returns `(nil, nil)`, call `Exists`
(`KeyValueGetter.Exists`, `storage/keyvalue.go:246-249`) to decide absent vs present-empty;
return `([]byte{}, true, nil)` when present. Separately remove the `kv.V == nil` drops in
`sendJSONValuesInRange` and `StreamKV` (emit `DeserializeData(kv.V)`, which is empty-safe).

Trade-offs: localized and no storage-contract change, but adds an `Exists` lookup (a second DAG
resolution) in the nil case and must be repeated per read path. Note `GetData` currently holds
an `OrderedKeyValueDB` (via `GetOrderedKeyValueDB`); confirm `Exists` is reachable on that
interface or obtain a `KeyValueDB` handle as `KeyExists` does (`:507-517`).

**Recommendation:** Option 1 — it fixes the ambiguity at the source and makes every consumer
(including future ones and neuronjson) consistent, versus patching each read site.

## Endpoint-by-endpoint outcome after the fix

| Endpoint | Before | After |
| --- | --- | --- |
| `HEAD /key` | 200 | 200 (unchanged) |
| `GET /key` (raw) | 404 | 200, empty body |
| `GET /keyrange` | lists key | lists key (unchanged) |
| `GET /keys` | lists key | lists key (unchanged) |
| `GET /keyrangevalues` (tar/protobuf) | omits key | includes key, empty value |
| `GET /keyvalues` (batch, tar/protobuf) | not-found | includes key, empty value |

## JSON-mode representation (OPEN)

In `json=true` responses, an empty value is currently rendered as `{}` (`:1285`, `:1352`), which
is the **same** sentinel the docs define for "key not found" (`helpMessage` `:289`, `:340`). So
JSON mode cannot distinguish present-empty from absent. The raw single-key `GET` (200 + 0-length
body) and the tar/protobuf batch/range forms represent zero bytes unambiguously.

**DECISION A:** accept the JSON-mode ambiguity and document it (recommended — JSON responses
require valid-JSON values anyway, and a zero-byte value is not valid JSON), or change the
not-found sentinel so the two are distinguishable in JSON mode.

## Docs

- Update `helpMessage` (`keyvalue.go` help block): a zero-byte POST stores a zero-byte value
  (not a delete); `GET` returns 200 with an empty body; `keyrange`/`keyrangevalues`/`keyvalues`
  include such keys; state the JSON-mode representation per DECISION A. Keep `DELETE /key` as the
  way to remove a key.
- Regenerate `docs/datatype-keyvalue.txt` from `helpMessage`.

## Tests (`datatype/keyvalue/*_test.go`)

- **Single-key round trip of empty value:** POST 0 bytes; `GET` → 200 with 0-length body;
  `HEAD` → 200; key present in `/keyrange` and `/keys`.
- **Range values include empty:** `/keyrangevalues` in tar and protobuf forms includes the key
  with a zero-length value (and json form returns `{}` per DECISION A).
- **Batch get includes empty:** `/keyvalues` (tar/protobuf) includes the key with empty value.
- **Empty overwrites prior value (same version):** POST bytes, POST 0 bytes; `GET` → 200 empty
  (not the old value), and the key remains listed.
- **Empty at child over value at parent:** value at committed parent, 0-byte POST at child;
  child `GET` → 200 empty; parent `GET` → original value.
- **Absent key unaffected:** never-written key → `GET` 404, `HEAD` 404, absent from `/keyrange`.
- **Storage-level (Option 1):** `Get` returns non-nil for a present empty-valued key and `nil`
  for an absent key (badger; and basholeveldb if built).

## Open decisions

- **DECISION 0 — fix layer:** Option 1 (storage `Get` contract; recommended) vs Option 2
  (datatype disambiguation).
- **DECISION A — JSON-mode empty representation:** accept `{}` ambiguity (document) vs change the
  not-found sentinel.
- **DECISION B — basholeveldb parity:** apply the same `Get` coercion to the legacy backend now,
  or only badger (the production store here).
- **DECISION C — neuronjson:** it shares the same read-side idioms — `data == nil` → not-found
  (`neuronjson.go:878`) and `kv.V == nil` drops in range/memstore scans (`memstore.go:136`,
  `neuronjson.go:957,988,1886`) — so Option 1 makes its reads consistent too. This does **not**
  collide with neuronjson's deletion model: deletion is a separate tombstone key identified by
  the marker byte via `IsTombstone()` (`version_files.go:102`, `storage/context.go:156`),
  independent of value length. Tombstone entries happen to carry empty values
  (recovery doc fact 4), but an empty *data*-key value is not a tombstone — so "empty value ==
  deleted" is not how deletion is detected. neuronjson also rejects an empty POST outright (its
  `PutData` requires a JSON object with a `bodyid` field, `neuronjson.go:1631,1634`), so empty
  values only arise there via the raw `import-kv` path. Decide whether to also drop-proof
  neuronjson's range/memstore scans, or leave them (empty values are effectively unreachable
  through neuronjson's own write path).

## References

- Investigation: keyrange/keys/HEAD vs GET/keyrangevalues disagreement; all affected keys pinned
  to `postkv Bytes:0` at version 102; local unit-test reproduction of the read-side 404.
- `dvid/serialize.go:188` (`SerializeData` empty → `[]byte{}`), `:300` (`DeserializeData`
  empty-safe).
- `storage/badger/badger.go`: `Get:383`, `Put:1189`, `loadVersionedValue:541`.
- `datatype/keyvalue/keyvalue.go`: `GetData:574`, `getKeyReadValue:846`,
  `sendJSONValuesInRange:1210` (drop at `:1254`), `StreamKV:1167` (drop at `:1181`),
  single-key `GET` handler `:1048`.

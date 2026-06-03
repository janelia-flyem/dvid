# DVID Per-UUID Public Access + Origin-Based Auth

Status: design / for review (Bill + Codex)
Builds on: `docs/design/2026-04-11-DSGAuthIntegration.md` (DSG token validation already implemented in `server/auth.go`).

## Context and goal

A single DVID server can host both **publicly released** data and **private**
(unreleased) data in the same repo DAG. The release point is a committed node;
everything in that node's ancestry is public, and newer commits/branches (the
private HEAD) are not.

We want **selective authorization**:

1. Requests against a **public** node need no DatasetGateway (DSG) check — the
   data is released.
2. Requests against a **private** node depend on the request's **origin**:
   - **Internal** clients (reaching DVID via the internal nginx vhost) can be
     allowed through with no DSG, controlled by `enforce_internal`.
   - **External** clients must present a valid DSG token (existing
     `enforce = "dsg"` flow).

This splits cleanly into three parts:

- **A. `datastore.IsPublic(uuid)`** — fast, startup-cached, ancestry-closed test.
- **B. Origin-based enforcement** in `server/auth.go` — public-first, then
  internal/external policy; internal detection via a trusted nginx header.
- **C. Repo-level routes + DAG pruning** — `/api/repos/info` and
  `/api/repo/:uuid/info` must not leak private nodes, and `POST /api/repos`
  must be gated.

## Decision flow (per request)

```
resolve concrete UUID (repoRawSelector already sets c.Env["uuid"])
        │
        ▼
datastore.IsPublic(uuid)?  ──yes──►  method ∈ {GET,HEAD,OPTIONS}? ──yes──► ALLOW (no DSG)
        │ no                                   │ no
        ▼                                      ▼
   (private node)                         (write on public node: treat as private)
        │
        ▼
effectiveEnforce(r):
   internal request (X-DVID-Internal) AND enforce_internal set ──► use enforce_internal
   else ──► use base enforce
        │
        ├─ "none"      ──► ALLOW
        ├─ "dsg"       ──► DSG token + dataset permission check (existing)
        └─ token/authfile ──► existing legacy JWT path
```

Key invariant: **public is checked on the resolved UUID and only relaxes
reads.** Writes (any verb beyond GET/HEAD/OPTIONS), including branch/newversion
off a public node, always fall through to `effectiveEnforce`.

---

## Part A — `datastore.IsPublic` (the public set)

### Semantics

- Config supplies a list of **public release UUIDs** (`[auth] public_versions`).
- For each, **that node and all of its ancestors are public** (ancestor-closed).
- Anything not in the expanded set is private (fail-closed). New / pushed /
  pulled nodes after startup are private until the operator adds them to the
  TOML and restarts.

Ancestor-closure is a **security requirement**, not a convenience: a DVID read
at version V is composed from V's ancestors (deltas up the DAG via
`findMatch`). If ancestors weren't public, a read of a "public" node could
surface data the operator only released implicitly — and conversely, closure
guarantees a public read can *never* reach a private descendant.

We are redefining the existing experimental `public_versions` key to mean
"this UUID **and its ancestors**, reads only." (Old behavior was exact-UUID,
read-only; the rename of semantics is acceptable — ancestors *should* be
included in any public release.)

### Build the set with a full multi-parent traversal (do NOT reuse `GetAncestry`)

`repoManager.getAncestry` (`datastore/repo_local.go:2044`) follows only
`parents[0]`, so a merge node's `parents[1+]` lineage would be missed. The data
path `findMatch` (`repo_local.go:2090`) traverses **all** parents, so the public
set must too. Add a dedicated traversal:

```go
// collect v and all transitive ancestors across ALL parents.
func (m *repoManager) ancestorsClosure(v dvid.VersionID) (map[dvid.VersionID]struct{}, error) {
    seen := map[dvid.VersionID]struct{}{}
    var walk func(dvid.VersionID) error
    walk = func(cur dvid.VersionID) error {
        if _, ok := seen[cur]; ok {
            return nil
        }
        seen[cur] = struct{}{}
        parents, err := m.getParentsByVersion(cur)
        if err != nil {
            return err
        }
        for _, p := range parents {
            if err := walk(p); err != nil {
                return err
            }
        }
        return nil
    }
    err := walk(v)
    return seen, err
}
```

### Data structure and API (new file `datastore/public.go`)

```go
type publicSet struct {
    uuids    map[dvid.UUID]struct{}       // for IsPublic(uuid) — middleware hot path
    versions map[dvid.VersionID]struct{}  // for DAG pruning by node version
}

// written once at startup, never mutated afterward (no reload) -> lock-free reads.
var publicVersions atomic.Pointer[publicSet]

// SetPublicVersions takes the RAW config strings (tc.Auth.PublicVersions,
// passed unchanged — no pre-resolution by the server), validates each per the
// release-point rules below, expands each over its full ancestry, and caches
// the public set. Called once at startup AFTER the repo manager is loaded.
//
// The []string signature is deliberate (per review): validation must see the
// raw string before any resolver leniency (prefixes, ":branch" selectors). A
// []dvid.UUID signature would lose that boundary.
func SetPublicVersions(configured []string) error { ... }

// IsPublic reports whether a resolved UUID is publicly accessible. Fail-closed:
// returns false if nothing configured or UUID unknown. One map lookup.
func IsPublic(uuid dvid.UUID) bool {
    ps := publicVersions.Load()
    if ps == nil {
        return false
    }
    _, ok := ps.uuids[uuid]
    return ok
}

func IsPublicVersion(v dvid.VersionID) bool { ... } // used by DAG pruning
```

`SetPublicVersions` validates each raw string (below), resolves it with
`VersionFromUUID`, runs `ancestorsClosure`, and converts versions back to UUIDs
with `UUIDFromVersion` to populate both maps. Decisions:

- **No runtime reload** (per review): set is immutable after startup.
  `POST /api/server/reload-auth` reloads the auth file and clears the DSG cache
  but does **not** rebuild the public set; document this and require a restart
  to change `public_versions`.
- Per-request cost: a single `map[dvid.UUID]struct{}` lookup, no allocation, no
  lock — meets the "very fast, every request" requirement.

#### Validate the release point strictly (per review)

A public release must be a real, durable commit — not a moving HEAD.
`matchingUUID` (`datastore/repo_local.go:969`) is far too permissive for a
durable security config: it accepts unique **prefixes**, and anything containing
a colon is parsed as a **branch selector** (`root:master`, bare `:master`) that
resolves to the branch's *leaf* — i.e. exactly the moving-HEAD reference we must
forbid (and branch names support `~n` HEAD offsets, `repo_local.go:1308`).

`SetPublicVersions` therefore validates the **raw config string first**, before
any resolution, and fails closed at startup (refuses to serve) if any value:

- **contains a `:`** — branch selectors (`:master`, `root:master`,
  `root:master~1`) are rejected outright; they name moving or relative
  positions, not releases.
- **is not a full canonical UUID** — reject prefixes/short strings; require the
  full UUID form, then resolve and require the resolved UUID to **equal the
  config string exactly** (belt and braces against any resolver leniency).
- **is unlocked / uncommitted.** A public release node must be locked
  (committed). Reject any release UUID whose node is not locked
  (`Locked(v)` / the node's `locked` flag) — otherwise "public" would track an
  open node whose contents can still change. Ancestors of a locked node are
  themselves locked, so only the named release UUIDs need the check.
- **is unknown.** Already fail-closed via `IsPublic`, but startup should error
  loudly rather than silently producing an empty public set.

### Startup wiring

Call `datastore.SetPublicVersions(tc.Auth.PublicVersions)` — the raw config
strings, **unchanged** — from server initialization after the repo manager is up
and before serving. The server must not resolve, trim, or normalize the values
first; all validation lives in `SetPublicVersions`. Today
`authorizations.initialize()` (`server/auth.go:68`) builds `auth.public` from
`tc.Auth.PublicVersions`; replace that block with this call so there is **one**
source of truth. The server-side `isPublic` map and `authorizations.public` are
removed.

---

## Part B — Origin-based enforcement (`server/auth.go`)

### Public-first check (replace existing `isPublic`)

`isAuthorized` (`server/auth.go:188`) already checks public first. Re-point it:

```go
func isPublicRead(r *http.Request, envUUID interface{}) bool {
    uuid, ok := envUUID.(dvid.UUID)
    if !ok {
        return false
    }
    switch r.Method {
    case http.MethodGet, http.MethodHead, http.MethodOptions:
        return datastore.IsPublic(uuid)
    }
    return false
}
```

The read-only restriction stays in the server layer; `datastore.IsPublic`
remains a pure UUID→bool.

### Internal detection via trusted nginx header (replaces RemoteAddr/CIDR)

Internal vs external is no longer inferred from `r.RemoteAddr` (useless behind
nginx — it's nginx's own address). Instead the **internal** nginx vhost stamps a
header that the **public** vhost strips:

```nginx
# internal vhost (listens on the internal/split-horizon IP, e.g. 10.10.1.20)
proxy_set_header X-DVID-Internal "true";

# public vhost (reached via the public IP, e.g. 206.241.0.137)
proxy_set_header X-DVID-Internal "";   # always clear client-supplied value
```

```go
const internalHeader = "X-DVID-Internal"

func isInternalRequest(r *http.Request) bool {
    if !strings.EqualFold(r.Header.Get(internalHeader), "true") {
        return false
    }
    // Optional hardening: only trust the header from a known proxy peer.
    if len(trustedProxyNets) == 0 {
        return true
    }
    host, _, err := net.SplitHostPort(r.RemoteAddr)
    if err != nil {
        host = r.RemoteAddr
    }
    ip := net.ParseIP(host)
    if ip == nil {
        return false
    }
    for _, n := range trustedProxyNets {
        if n.Contains(ip) {
            return true
        }
    }
    return false
}
```

Threat model (accepted): spoofing requires reaching DVID's internal port
directly *and* the public vhost never forwards a client `X-DVID-Internal`. The
optional `trusted_proxies` peer check closes the "attacker connects straight to
DVID's port" hole cheaply.

**Decision (Bill):** `trusted_proxies` stays **optional**. When unset, a
well-formed `X-DVID-Internal: true` is trusted regardless of peer. The two
controls that make this safe are operator responsibilities, and the plan must
state them as deployment requirements, not nice-to-haves:

1. DVID's listening port is not reachable from untrusted networks (only the
   nginx hosts can connect to it); and
2. the public vhost **always** clears any client-supplied `X-DVID-Internal`
   (`proxy_set_header X-DVID-Internal "";`).

If `trusted_proxies` *is* set, the CIDRs are parsed **once at startup** (fail to
start on a malformed CIDR) rather than re-parsed per request as the current
`isInternalRequest` does. The old `internal_cidrs` (client-IP-based) is
**removed**; the hardening is repurposed as `trusted_proxies` (CIDRs of the
nginx host(s)).

> Codex recommended making `trusted_proxies` *required* whenever
> `enforce_internal` relaxes auth and failing closed. We are not adopting that;
> the residual risk (a host that can reach DVID's port directly and forge the
> header) is accepted under requirement (1) above.

### `effectiveEnforce` rewrite

```go
func effectiveEnforce(r *http.Request) string {
    enforce := authMode()
    if tc.Auth.EnforceInternal == "" {
        return enforce // no internal policy configured
    }
    if isInternalRequest(r) {
        return strings.ToLower(tc.Auth.EnforceInternal)
    }
    return enforce
}
```

### `enforce_internal` stays a **string** (validated at startup)

Keep symmetry with `enforce` (`none`/`token`/`authfile`/`dsg`) and future
flexibility (e.g. internal `authfile`, external `dsg`). Add startup validation
that rejects any value outside the known set, so a typo fails loudly instead of
silently falling through to the JWT path. Empty string = "internal same as
external".

### Legacy JWT modes (`token` / `authfile`) — deprecate (per review)

"Legacy JWT" = the `enforce = "token"` and `enforce = "authfile"` modes: DVID
mints/validates its own JWT (via `/api/server/token`, backed by the legacy proxy
or Google OAuth, signed with `DVID_JWT_SECRET_KEY`) and, for `authfile`, checks
the user against a local auth file. This predates DSG and is the
`jwt.Parse(...)` branch in `isAuthorized` (`server/auth.go:228+`).

**Decision (Bill):** with DSG integration these are obsolete and should be
**sunset**. For *this* change:

- The per-UUID public-access + metadata-filtering feature targets
  `enforce = "dsg"` (and `none`). DSG is the supported auth path going forward.
- `token`/`authfile` are marked **deprecated** (log a startup warning when
  configured). To avoid the scope inconsistency Codex flagged, `requestAccessScope`
  mirrors the legacy decision while these modes still exist (valid JWT → `full`,
  per the bullet above) — so a JWT user is never demoted to public-pruned output.
- **Recommended follow-up (separate change, not here):** remove the
  `token`/`authfile` branches, `generateJWT`, `serverTokenHandler`, and the
  legacy proxy/OAuth code once no deployment depends on them. Flagged as an open
  decision below (remove-now vs deprecate-now-remove-later).

---

## Part C — Repo-level routes and DAG pruning

### The exposure

`isAuthorized` is attached only to `repoMux`/`nodeMux`/`instanceMux`
(`server/web.go:811/830/851`). The plural repos routes live on `mainMux` and
are **never** auth-gated:

```
web.go:795  mainMux.Post("/api/repos", reposPostHandler)     // no isAuthorized, but handler requires admintoken
web.go:796  mainMux.Get("/api/repos/info", reposInfoHandler)  // every repo's full DAG: ungated
```

(`reposPostHandler` is *not* on an `isAuthorized` mux, but its body requires
`adminPriv` — see the `POST /api/repos` decision below. `reposInfoHandler` is the
real leak: it serializes every repo's full DAG with no gating at all.)

And `repoInfoHandler` (`/api/repo/:uuid/info`) *is* gated, but the public bypass
returns the **entire** repo DAG (`repoT.MarshalJSON` → full `dagT`), including
private descendant branches — a leak when `:uuid` is a public node.

### Access scope helper

> **Why a separate helper (per review).** `isAuthorized` returns immediately on
> the public-read bypass (`server/auth.go:190`) *before* it ever fetches the DSG
> user, so on a public read it never sets `c.Env["user"]`. The metadata handlers
> below cannot rely on the middleware to tell them "is this an authed admin?" —
> they must do their own **soft** auth. `requestAccessScope` is that soft auth:
> it re-resolves origin and DSG identity itself, reusing the `getDSGUser` cache,
> and returns public-only on any missing/invalid token (never rejects).

```go
type accessScope struct {
    internal  bool             // X-DVID-Internal trusted (isInternalRequest)
    adminPriv bool             // admintoken matched (from c.Env["adminPriv"])
    user      *dsgUserCache    // nil if no/invalid token
    full      bool             // sees the whole repo, every node + instance
}

// requestAccessScope does soft auth: it never writes an error response.
// Takes the goji context because adminPriv lives in c.Env (set by the
// adminPrivHandler middleware, web.go:1119) and is NOT derivable from the
// request alone.
func requestAccessScope(c web.C, r *http.Request) accessScope
```

**Signature (per review, round 3):** the helper takes `web.C`, not just
`*http.Request` — `adminPriv` is computed by the `adminPrivHandler` middleware
into `c.Env` and a request-only helper cannot see it. All handlers that call
this are goji handlers with `web.C` in hand.

`full` is **not** granted by being internal alone (per review). It is granted
only when the request's *effective* enforcement is already "none", or by a real
authenticated admin (DSG admin or admintoken):

- `full = true` iff `effectiveEnforce(r) == "none"` **or** `adminPriv` **or**
  (`user != nil && user.Admin`).
  - This means an internal request gets `full` only when `enforce_internal`
    resolves to `none`. If `enforce_internal = "dsg"` (or `token`/`authfile`),
    an internal request must still present a valid token; `requestAccessScope`
    resolves it and decides `full`/per-repo from the token, not from origin.
- If `effectiveEnforce(r) == "dsg"`, attempt to resolve a DSG token
  (`extractDSGToken` → `getDSGUser`). On success set `user`; per-repo visibility
  is then `userHasDatasetAccess(user, dataset_map[root], "GET")`.
- If `effectiveEnforce(r) == "token"` or `"authfile"` (legacy JWT — being
  sunset, see below): mirror `isAuthorized`'s decision so a JWT that already
  passes the middleware is not silently demoted to public-pruned metadata. A
  valid JWT (and, for `authfile`, an authorized user) → `full = true` (legacy
  JWT has no per-dataset granularity). Invalid/absent → public-only.
- No token / invalid token / no dataset access → **public-only** for that repo.

**Datastore boundary (per review, round 3 — import cycle):** `accessScope` is a
`server`-package type, and `server` imports `datastore`, so
`datastore.GetRepoJSONFiltered(uuid, scope)` cannot take it. The per-request
auth decision stays entirely in `server`; `datastore` receives only a
datastore-owned visibility verdict:

```go
// datastore-owned; no auth knowledge.
type RepoVisibility int

const (
    RepoHidden     RepoVisibility = iota // omit repo entirely
    RepoPublicOnly                       // pruned DAG, no Log
    RepoFullView                         // existing full marshal
)

func GetRepoJSONFiltered(uuid dvid.UUID, vis RepoVisibility) (string, error)

// for /api/repos/info: server supplies the verdict per repo root.
func MarshalJSONFiltered(visFor func(rootUUID dvid.UUID) RepoVisibility) ([]byte, error)
```

The server computes `accessScope` once per request and maps it (plus per-repo
`userHasDatasetAccess`) to a `RepoVisibility` per repo. `datastore` never sees
tokens, headers, or admin state.

### `GET /api/repos/info` — filter to accessible repos/nodes

Replace the bare `datastore.MarshalJSON()` (`web.go:1692`) with a filtered
marshal. Note `reposInfoHandler` is currently a plain `(w, r)` handler
(`web.go:1691`) and must be **widened to take `web.C`** — same as
`serverConfigHandler` — so it can call `requestAccessScope(c, r)` (admintoken
state lives in `c.Env`). Per repo:

- `full` scope → include the whole repo.
- authed user with dataset access to that repo's root → include the whole repo.
- else → include a **pruned** repo whose DAG contains only public nodes.
- **Hide repos with zero visible nodes** entirely (per review).

### `GET /api/repo/:uuid/info` — prune on the public/unauth path

When the request is served via the public bypass (or an authed user lacking
dataset access), return the pruned DAG instead of the full one. The full DAG is
returned only for `scope.full` (which per the rule above means
`effectiveEnforce(r)=="none"`, admintoken, or DSG admin — **not** internal
origin by itself) or an authed user with dataset access to this repo's root.

### Pruned-DAG construction (`datastore`)

Add `datastore.GetRepoJSONFiltered(uuid, vis RepoVisibility)` (and the
`MarshalJSONFiltered` callback form for the repos list — see the datastore
boundary above; `datastore` receives only the `RepoVisibility` verdict, never a
server auth type) that builds a copy of `dagT` keeping only visible nodes:

- keep node iff `IsPublicVersion(node.version)` (public scope) — ancestor
  closure guarantees these form a connected sub-DAG rooted at `dag.root`.
- within each kept node, **filter `children` to kept versions** (drop private
  HEAD/branch tips) so no private UUID leaks by reference. `parents` need no
  filtering (ancestors are always public).
- branch HEAD / `branch-versions` references that point at private nodes are
  dropped or repointed to the latest visible node on that branch.
- keep the **repo root** as the filtered DAG's root (ancestor-closure guarantees
  the root is public); do **not** recompute the root.
- `DataInstances`, alias, description, repo notes: **kept** if the repo is
  visible. Repo `Log`: **omitted** on the public scope. See the metadata contract.

Because `dagT` is `{root, nodes map[VersionID]*nodeT}`, the filtered copy is a
new `dagT` with a reduced `nodes` map and trimmed `children` slices; existing
`dagT.MarshalJSON` then serializes it unchanged.

> ### ⚠️ High (implementation): do NOT reuse `dagT.duplicate(versions)`
>
> `dagT.duplicate(versions)` (`datastore/repo_local.go:3186`) is the wrong tool
> and must not be used for public pruning:
>
> - It **rewires** parents/children of removed nodes *through* their neighbors
>   via `editVersionSlice` (`repo_local.go:3202,3209`) — splicing a dropped
>   node's parents/children into adjacent nodes. We want the opposite: keep only
>   public nodes and **drop** any child reference to a private node, with no
>   rewiring.
> - It **recomputes the root** to the minimum version in `versions`
>   (`repo_local.go:3219-3231`), which is not the contract (root must stay the
>   repo root).
>
> Build a **purpose-built filtered copy** instead: copy only kept nodes, copy
> each kept node's `children` filtered to kept versions, leave `parents` as-is
> (all public), and keep `dag.root`/`rootV`. **Acceptance test:** after filtering,
> assert that **every child VersionID referenced by any serialized node exists in
> the filtered `Nodes` map** (no dangling/private references), for linear chains
> and diamond DAGs. This test is required, not optional.

### Public metadata contract (formal)

Codex's central concern (correct): `repoT.MarshalJSON` (`datastore/repo_local.go:2679`)
is **not** just a DAG. It serializes the repo-wide `r.data` map, and each entry
(`Data.MarshalJSON`, `datastore/datainstance.go:540`) carries `DataUUID`,
`RepoUUID`, `KVStore`, `LogStore`, `Tags`, and `Syncs`. Data instances are
repo-wide and keyed by **name**, not by version: an instance created on a
private branch (`newData`, `repo_local.go:2146`) lives in the same `r.data` map
and also appends a coarse line to the repo log. So a literal "keep DataInstances"
does expose the *existence and storage wiring* of instances that have no public
data yet.

**Decision (Bill):** the DVID console and other clients rely on
`/api/repos/info` and `/api/repo/:uuid/info` to discover what instances are
available, and exposing instance existence beyond the public node is **not**
considered critical for the initial rollout. So for the public/unauth scope we
**keep** the existing fields, and the contract below records exactly what that
means and what the deferred tightening is.

Per-scope, the filtered marshal emits:

| Field | `full` scope | authed-with-access | public / unauth |
|-------|--------------|--------------------|-----------------|
| DAG `nodes` / `children` | all | all | **pruned** to public versions; private children dropped |
| `Root`, `Alias`, `Description`, `Properties` | yes | yes | yes |
| Repo `Log` | full | full | **omitted** (per review — can contain private node UUIDs) |
| `DataInstances` (whole `r.data`) | yes | yes | **yes, for now** (console dependency) |

Notes on the public column:

- **No private node UUID may appear.** This is the hard line. The pruned DAG, the
  `branch-versions` route, and any branch/HEAD reference must never surface a
  private version's UUID.
- **Repo `Log` is omitted on the public scope (decision: Bill).** The earlier
  draft claimed repo log lines never embed node UUIDs; that is **wrong**. RPC
  `branch`/`newversion`/`merge` append `cmd.String()` — which includes UUID args
  — via `AddToRepoLog` (`server/rpc.go:496,513,531`), and `POST /api/repo/:uuid/log`
  (`server/web.go:1912`) lets a client write arbitrary text. So the public/unauth
  marshal drops `Log` entirely (empty slice), and `GET /api/repo/:uuid/log`
  returns an empty log on the public path. Node-level commit/branch history lives
  in `node.log`, reachable only via the `nodeMux` routes (already per-UUID scoped).
- **Residual exposure we are accepting (documented, not hidden):** for the public
  scope, `DataInstances` still reveals (a) the *existence* of instances that only
  have data on private branches; (b) per-instance `KVStore`/`LogStore` backend
  strings, `DataUUID`, `RepoUUID`, and `Tags`; and (c) **datatype-specific
  extended metadata** emitted by each type's own `MarshalJSON` (e.g. labelmap
  `MaxLabel`/`MaxRepoLabel`, voxel `Extents`/`MinPoint`/`MaxPoint`). Note some of
  this extended metadata is resolved at the **master leaf**, not at the public
  release node, so the values shown may reflect private-HEAD state. This whole
  block is the gap Codex flagged as High; we are choosing to live with it for the
  first release.

**Deferred improvement (tracked, not in this change):** make `DataInstances`
version-aware on the public scope — include only instances that have committed
data at a public version; sanitize the base DTO (drop
`KVStore`/`LogStore`/`DataUUID`/`RepoUUID`, keep `Name`/`TypeName`/`Versioned`);
and resolve datatype-specific extended metadata (extents, max labels) **at the
public release version rather than the master leaf**. This needs a cheap "does
instance X have data at any public version" signal — and a way to ask each type
to marshal at a specified version — that DVID does not expose today, so it is out
of scope here.

### `POST /api/repos`

**Correction (per review):** the earlier draft was self-contradictory. The route
is *not* on a mux with `isAuthorized`, but `reposPostHandler`
(`server/web.go:1703`) **already** requires `adminPriv` and a non-read-only
server, and `adminPriv` is set **only** by a matching `admintoken` query param
(`adminPrivHandler`, `web.go:1119`) — it is *not* the DSG `user.Admin` bit.

**Decision:** repo creation stays **admintoken-only**. DSG admin and the
internal origin do **not** grant repo creation (a new repo has no root UUID to
map to a dataset, and creation is an operator action). This needs **no code
change** — the existing `adminPriv` + non-readonly guard is correct; we only
correct the plan/test text to match it. The DSG path (`enforce_internal` /
`X-DVID-Internal`) is irrelevant to `POST /api/repos`.

### Sibling routes — required scope, not "revisit" (per review)

These are under `isAuthorized` (so the public bypass applies) and must have
defined public-scope behavior as part of **acceptance**, not follow-up:

- **`GET /api/repo/:uuid/branch-versions/:name`** (`web.go:819` →
  `getAncestryByBranch`, `repo_local.go:3089`) returns the full branch
  ancestry as a UUID list straight from the full DAG — this directly leaks
  private node UUIDs and would defeat the DAG pruning above. **Required:** on the
  public scope, return the branch ancestry **filtered to visible (public)
  versions** — i.e. the public *suffix* of the branch. The normal case is a
  branch whose tip (HEAD) is private but whose lower ancestors are public; that
  must return the visible ancestors with the effective HEAD = **the last public
  node on the branch**, *not* an empty result. Empty list (or 404) is reserved
  for the genuine "no visible node on this branch" case. `full`/authed-with-access
  unchanged.
  - **Output order (per review, round 3):** the existing route returns
    **HEAD/leaf-to-root** (`GetBranchVersions`, `datastore/datastore.go:197`),
    and the filtered result keeps that order:
    `[lastPublicOnBranch, parent, ..., root]` — *not* root-to-leaf. ("Public
    suffix" means the lower, public portion of the branch, but it is emitted
    leaf-first like today.)
  - *Simplification (Bill):* there are no merged branches today and none are
    foreseen, so the public suffix is a simple linear walk — no multi-parent
    branch reconstruction needed. (The `merge` feature is a candidate for
    deprecation; tracked separately.)
- **`GET /api/repo/:uuid/log`** (`web.go:1892` → `GetRepoLog`) — **omit on the
  public scope (decision: Bill).** Per the corrected metadata contract, repo log
  lines *can* contain private node UUIDs (RPC `cmd.String()`; arbitrary client
  POSTs), so the public path returns an empty log. `full`/authed-with-access
  return the real log.
- **`GET /api/node/:uuid/note`, `GET /api/node/:uuid/log`** are per-node and on
  `nodeMux`. Because `IsPublic` is checked on the *resolved* node UUID, a public
  read only succeeds when `:uuid` is itself public; a request against a private
  node falls through to `effectiveEnforce`. So these need **no extra filtering**
  — the per-UUID public check already scopes them. (Confirm in tests.)

### `HEAD /api/repo/:uuid` — close the UUID-probing oracle (per review, round 3)

`repoRawMux` (`web.go:800-804`) carries only `activityLogHandler` +
`repoRawSelector` — **no `isAuthorized`** — and `repoHeadHandler`
(`web.go:1756`) doesn't just confirm existence: it **returns the repo's root
UUID** for any string `repoRawSelector` can resolve. Since `matchingUUID`
accepts unique prefixes, an anonymous client can probe hex prefixes
(`HEAD /api/repo/a`, `/api/repo/ab`, …) to **enumerate private node UUIDs**
character by character, then use them elsewhere. This would defeat the DAG
pruning above.

**Fix:** add `isAuthorized` to `repoRawMux`, *after* `repoRawSelector` (same
ordering as `repoMux`, so `c.Env["uuid"]` is set when the public check runs):

```go
repoRawMux.Use(repoRawSelector)
if authorizationOn {
    repoRawMux.Use(isAuthorized)
}
```

Behavior after the fix: `HEAD` on a public UUID still works anonymously (public
bypass); `HEAD` on a private UUID or prefix requires auth like any other gated
route. One wrinkle to preserve: `repoHeadHandler` returns the **root** UUID,
which is always public for a repo with any public release (ancestor closure), so
no response filtering is needed — only the gate.

**Residual existence oracle (documented, accepted):** even gated, a prober can
distinguish "no such UUID" (selector error from `repoRawSelector`, which runs
*before* `isAuthorized`) from "exists but unauthorized" (401/403). That
distinction leaks one bit per probe on **every** `repoRawSelector` route
(`repoMux`/`nodeMux`/`instanceMux` too), so prefix-walking can still confirm
which prefixes exist — but it no longer yields full UUIDs or root UUIDs, which
is the substantive fix. Fully closing it would mean unifying the anonymous
error response for not-found vs unauthorized across all UUID routes; noted as
optional hardening, out of scope for this slice.

### `/api/server/*` admin routes — gate in DVID **and** block at nginx (per review)

These routes are on `serverMux`, which has **no** `isAuthorized` middleware, and
several do not check `adminPriv` either. For a public-facing deployment this is a
larger exposure than DAG pruning:

| Route | Handler | Today | Fix |
|-------|---------|-------|-----|
| `GET /api/server/config` | `serverConfigHandler` (`web.go:1545`) | dumps the **entire TOML** (`tcContent`) incl. `dataset_map`, `dsg_address`, store paths — **no** `adminPriv` check | require `adminPriv` |
| `POST /api/server/settings` | `serverSettingsHandler` (`web.go:1602`) | mutates GC %/throttle — **no** `adminPriv` check | require `adminPriv` |
| `POST /api/server/reload-auth` | `serverReloadAuthHandler` (`web.go:1634`) | reloads auth file, clears DSG cache — **no** `adminPriv` check | require `adminPriv` |
| `POST /api/server/reload-blocklist` | `serverReloadBlocklistHandler` (`web.go:1644`) | reloads blocklist — **no** `adminPriv` check | require `adminPriv` |

**Decision (Bill): defense in depth — do both.**

1. **In DVID:** add an `adminPriv` guard to each handler above (the same
   `c.Env["adminPriv"].(bool)` pattern `reposPostHandler` already uses). Note
   these are `web.C` handlers; the four above currently take `(c web.C, ...)` or
   plain `(w, r)` — `serverConfigHandler` is `(w, r)` and must be widened to take
   `web.C` to read `adminPriv`. `adminPriv` is the **admintoken** check, the right
   bar for server-wide operations.
2. **At nginx:** the public vhost must additionally block `/api/server/` POSTs
   and `GET /api/server/config` outright (return 404/403), so these never reach
   DVID from the public network even if a token leaks. Document this in the nginx
   snippet alongside the `X-DVID-Internal` stripping.

**`GET /api/server/blobstore/:ref` (per review — Medium):** this is *not*
informational. `blobstoreHandler` (`web.go:1653`) serves the configured mutation
blobstore by opaque ref, and those blobs can carry mutation payloads tied to any
node, including private ones. **Decision (Bill): require authorization** — deny
anonymous/public. Concretely (`blobstoreHandler` already takes `web.C`):

```go
scope := requestAccessScope(c, r)
allow := scope.full || scope.user != nil
```

With `scope.full` defined as above, this resolves exactly per the global policy
— **internal origin is not a standalone grant**:

- admintoken → allowed; valid DSG token → allowed;
- internal request when `enforce_internal = "none"` → `effectiveEnforce=="none"`
  → `full` → allowed (the "local via nginx screening" case);
- internal request when `enforce_internal = "dsg"` and no token → **denied**,
  consistent with internal clients needing DSG tokens under that config;
- anonymous external → denied.

The public nginx vhost additionally screens this route like the other
non-public `/api/server/*` routes. Blob refs are opaque hashes, so there is no
per-UUID public test to apply; treat the whole route as non-public.

(Read-only informational server routes — `info`, `note`, `types`,
`compiled-types`, and `groupcache` — stay open **explicitly**, not by omission.
`serverGroupcacheHandler` (`web.go:1586`) returns only marshaled
`storage.GetGroupcacheStats()` — numeric cache hit/miss counters, no secrets.)

---

## Config example (`[auth]`)

```toml
[auth]
enforce = "dsg"               # external default
enforce_internal = "none"     # internal clients (X-DVID-Internal) skip DSG
dsg_address = "https://auth.janelia.org"
dsg_cache_ttl = 300
# trusted_proxies = ["127.0.0.1/32", "10.10.1.0/24"]  # optional: only trust header from nginx
# Full 32-hex UUIDs required: prefixes and ":branch" selectors are rejected at startup.
# Each UUID + all its ancestors become public (reads only); node must be committed (locked).
public_versions = [
    "2f4ac91325c9452a9c1a499e1c5c1c4f",
    "7b91c8e3a64f4e6bb2d5f0a9e8d7c6b5",
]
# root UUID -> DSG dataset
dataset_map = { "a4e1c3d5b7f94a2c8e6d0b1a3c5e7f90" = "vnc", "c8b6a4d2e0f94c6a8b2d4f6e8a0c2e41" = "manc" }
```

Note: `internal_cidrs` (old client-IP meaning) is removed.

---

## Files to modify

| File | Change |
|------|--------|
| `datastore/public.go` (new) | `publicSet`, `SetPublicVersions(configured []string)` (raw config strings — validation before resolution), `IsPublic`, `IsPublicVersion`, `ancestorsClosure` |
| `datastore/repo_local.go` | multi-parent `ancestorsClosure`; release-point validation (raw-string check: no `:` selectors, full canonical UUID, exact match, locked node); **purpose-built filtered `dagT` copy** (drop private children, keep repo root — **do NOT reuse `dagT.duplicate`**); `GetRepoJSONFiltered` / filtered `MarshalJSON` (omit `Log` on public scope); public-suffix `GetBranchVersionsJSON` (leaf→root order) |
| `datastore/datastore.go` | export wrappers (`SetPublicVersions`, `IsPublic`, filtered JSON, scoped branch-versions); **`RepoVisibility` type** (`RepoHidden`/`RepoPublicOnly`/`RepoFullView`) so `datastore` never sees server auth types (import cycle) |
| `server/auth.go` | `isPublicRead` via `datastore.IsPublic`; header-based `isInternalRequest`; `effectiveEnforce` rewrite; `enforce_internal` startup validation; **deprecation warning for `token`/`authfile`**; drop `auth.public`/`internal_cidrs`; add optional `trusted_proxies` (parsed once at startup); `requestAccessScope(c web.C, r)` (soft auth — `full` when `effectiveEnforce=="none"`, admintoken, DSG admin, or valid legacy JWT) + mapping to `datastore.RepoVisibility` |
| `server/web.go` | filtered `reposInfoHandler` (**widen from `(w, r)` to `web.C`**, `web.go:1691` — needed for `requestAccessScope`); pruned `repoInfoHandler` + public-suffix `repoBranchVersionsHandler` + empty-log `getRepoLogHandler` on public path; **add `isAuthorized` to `repoRawMux` after `repoRawSelector`** (close the `HEAD /api/repo/:uuid` prefix-probing oracle); wire `datastore.SetPublicVersions(tc.Auth.PublicVersions)` (raw strings, unchanged) at startup; **add `adminPriv` guard to `serverConfigHandler` (widen to `web.C`), `serverSettingsHandler`, `serverReloadAuthHandler`, `serverReloadBlocklistHandler`**; `serverGroupcacheHandler` stays open (stats only, explicit decision); **gate `blobstoreHandler` via `scope.full \|\| scope.user != nil`**; `reposPostHandler` unchanged (already admintoken-gated) |
| `scripts/distro-files/config-full.toml` | document new `public_versions` semantics (locked-UUID requirement), `enforce_internal`, `X-DVID-Internal`, optional `trusted_proxies`; mark `token`/`authfile` deprecated; remove `internal_cidrs` |
| nginx config (deployment doc) | internal vhost stamps `X-DVID-Internal: true`; public vhost clears it and blocks `/api/server/` POSTs + `GET /api/server/config` + screens `GET /api/server/blobstore/:ref` |

---

## Test plan

- `IsPublic`: configured UUID, an ancestor, a **merge** ancestor via `parents[1+]`
  (must be public), a private descendant (false), an unknown UUID (false),
  nothing configured (all false).
- `ancestorsClosure`: linear chain, merge node, diamond DAG.
- `isInternalRequest`: header present/absent/forged; with and without
  `trusted_proxies` (when set, malformed CIDR fails startup; peer outside CIDRs
  is rejected even with the header).
- `effectiveEnforce`: internal+`enforce_internal=none`→none, external→`dsg`,
  internal+`enforce_internal=dsg`→still requires a token, empty
  `enforce_internal`→base enforce, invalid value rejected at startup.
- `requestAccessScope` (High #4): `full` only when `effectiveEnforce(r)=="none"`,
  `adminPriv` (admintoken), or the DSG user is admin; internal+
  `enforce_internal=dsg` with **no** token → public-only (NOT full); internal+
  `enforce_internal=dsg` with a non-admin scoped token → per-repo, not full;
  admintoken query param alone → full (verifies `web.C` plumbing).
- `SetPublicVersions` validation (per review): branch selectors rejected at
  startup — `:master`, `root:master`, `root:master~1` each fail loudly; short
  prefixes rejected; resolved UUID must equal the raw config string;
  unlocked/uncommitted release UUID rejected; unknown UUID errors loudly;
  `reload-auth` does not change the public set.
- Public bypass is read-only: `POST`/branch/newversion on a public UUID still
  requires auth.
- `/api/repos/info`: public scope hides private nodes and zero-public repos;
  authed-with-access sees full; internal-with-`enforce_internal=none` sees full.
- `/api/repo/:uuid/info`: pruned children, **no private node UUID by reference**;
  `DataInstances` present on public scope (accepted), repo `Log` **omitted** on
  public scope; full DAG + real log for authed-with-access.
- **Filtered-DAG integrity (High, implementation):** for linear-chain and
  diamond DAGs, assert **every child VersionID in any serialized node exists in
  the filtered `Nodes` map** (no dangling/private refs); assert the filtered root
  equals the repo root (not recomputed). Guards against accidentally reusing
  `dagT.duplicate`.
- `/api/repo/:uuid/branch-versions/:name` (Medium): public scope returns the
  **visible public suffix in leaf→root order** — a branch whose tip is private
  but whose ancestors are public returns exactly
  `[lastPublicOnBranch, parent, ..., root]` (assert order, not just membership),
  **never** a private UUID; empty/404 only when the branch has no visible node;
  full ancestry for authed-with-access.
- `HEAD /api/repo/:uuid` (round 3): anonymous HEAD on a **public** UUID succeeds;
  on a **private** UUID or a private-matching **prefix** returns an auth error
  and **never** the root UUID in the body; authed/full unchanged. (The
  not-found-vs-unauthorized response distinction is an accepted residual — see
  the HEAD section.)
- `/api/repo/:uuid/log` (per review): empty on public scope; real log for
  authed-with-access. Include a case where a `branch`/`merge` RPC has written a
  UUID-bearing log line — public scope must not return it.
- `/api/node/:uuid/log|note`: public read succeeds only when `:uuid` resolves to
  a public node; private node falls through to `effectiveEnforce`.
- Legacy JWT scope (Medium): under `enforce=token`/`authfile`, a valid JWT user
  gets `full` metadata (not public-pruned); deprecation warning logged at startup.
- `/api/server/*` gating (Medium): `GET /api/server/config` and the
  `settings`/`reload-auth`/`reload-blocklist` POSTs return 401/403 without
  `admintoken`; succeed with it. `GET /api/server/blobstore/:ref`: denied for
  anonymous/public **and** for internal-with-`enforce_internal=dsg`-no-token;
  allowed for `admintoken`, valid DSG token, or
  internal-with-`enforce_internal=none` (i.e. `scope.full || scope.user != nil`).
- `POST /api/repos`: rejected without `admintoken` (incl. for a DSG admin and an
  internal request); allowed only with `admintoken` on a non-readonly server.

---

## Decisions resolved (Codex review, rounds 1–3)

1. **Public metadata contract** — `DataInstances` **stays** on the public scope
   (the DVID console depends on `/api/repos/info` and `/api/repo/:uuid/info` to
   discover instances); repo `Log` is **omitted** (round 2 — logs can carry
   private node UUIDs). The residual `DataInstances` exposure (private-branch
   instance existence, storage-backend strings, datatype-specific extended
   metadata) is accepted for the first release; version-aware instance filtering
   is a **deferred improvement**. The hard line — no private node UUID may appear —
   is enforced via DAG pruning, scoped `branch-versions`, and log omission.
2. **`trusted_proxies`** — stays **optional**; relies on (a) DVID's port being
   unreachable from untrusted networks and (b) the public vhost clearing
   `X-DVID-Internal`. Parsed once at startup if set.
3. **`/api/server/*`** — defense in depth: `adminPriv` guard added in DVID **and**
   nginx blocks these externally. `GET /api/server/blobstore/:ref` (round 2) is
   gated to admin / DSG token / internal — not public.
4. **`POST /api/repos`** — admintoken-only (existing guard); DSG admin / internal
   do not grant repo creation. No code change.
5. **`internal⇒full`** — `full` granted only when `effectiveEnforce(r)=="none"`,
   admintoken (`adminPriv`), or the DSG user is admin; internal origin alone
   does not grant full.
6. **`branch-versions` public behavior** (round 2) — return the visible public
   **suffix** (HEAD = last public node), not empty; empty/404 only when no
   visible node. No merged-branch handling needed (none exist or are foreseen).
7. **Legacy JWT** (round 2) — `token`/`authfile` modes are **deprecated** and
   slated for sunset; DSG is the supported path. While they exist,
   `requestAccessScope` honors a valid JWT as `full` so metadata isn't
   inconsistently pruned.
8. **Filtered DAG copy** (round 2, **High**) — must be a purpose-built copy that
   drops private children and keeps the repo root; **`dagT.duplicate` must not be
   reused**, enforced by the child-existence acceptance test.
9. **`requestAccessScope` plumbing** (round 3) — takes `web.C` so `adminPriv`
   (admintoken) is part of the scope (`full`); the auth decision never crosses
   into `datastore`, which gets only a `RepoVisibility` verdict (avoids the
   server→datastore import cycle).
10. **`HEAD /api/repo/:uuid`** (round 3) — `repoRawMux` gets `isAuthorized`
    (after `repoRawSelector`), closing the anonymous prefix-probing oracle that
    returned root UUIDs. The not-found-vs-unauthorized response distinction is
    an accepted residual (one existence bit per probe; applies to all UUID
    routes; optional hardening deferred).
11. **`public_versions` raw-string validation** (round 3) — reject any `:`
    selector (`:master`, `root:master`, `root:master~1`), require full canonical
    UUID with exact resolved match, locked node.
12. **`branch-versions` output order** (round 3) — leaf→root, i.e.
    `[lastPublicOnBranch, ..., root]`, matching existing behavior.
13. **Implementation assumptions confirmed** (round 3, Codex + plan agree) —
    `X-DVID-Internal` stays a hardcoded const; `enforce_internal` stays a
    validated string; legacy JWT is **deprecated in this slice but not removed**;
    version-aware `DataInstances` filtering stays deferred.

## Open questions still for review

1. **Deferred instance filtering** — when we make `DataInstances` version-aware,
   do we need a new per-instance "has data at version" signal in `datastore`, or
   is "created at a public version" a good-enough proxy? (Out of scope here.)
2. **Anonymous error-response unification** (optional hardening) — make
   not-found vs unauthorized indistinguishable for anonymous requests across
   UUID routes to fully close the existence oracle left by decision 10.

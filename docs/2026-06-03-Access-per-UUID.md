# DVID Per-UUID Public Access + Origin-Based Auth

Status: design / for review (Bill + Codex)
Builds on: `docs/2026-04-11-DSGAuthIntegration.md` (DSG token validation already implemented in `server/auth.go`).

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

// SetPublicVersions expands each configured UUID over its full ancestry and
// caches the public set. Called once at startup AFTER the repo manager is loaded.
func SetPublicVersions(uuids []dvid.UUID) error { ... }

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

`SetPublicVersions` resolves each UUID with `m.matchingUUID` /
`VersionFromUUID`, runs `ancestorsClosure`, and converts versions back to UUIDs
with `UUIDFromVersion` to populate both maps. Decisions:

- **No runtime reload** (per review): set is immutable after startup.
- Per-request cost: a single `map[dvid.UUID]struct{}` lookup, no allocation, no
  lock — meets the "very fast, every request" requirement.

### Startup wiring

Call `datastore.SetPublicVersions(...)` from server initialization after the
repo manager is up and before serving. Today `authorizations.initialize()`
(`server/auth.go:68`) builds `auth.public` from `tc.Auth.PublicVersions`;
replace that block with a call into datastore so there is **one** source of
truth. The server-side `isPublic` map and `authorizations.public` are removed.

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
DVID's port" hole cheaply; recommended but not required. The old
`internal_cidrs` (client-IP-based) is **removed**; if we keep the hardening, it
is repurposed as `trusted_proxies` (CIDRs of the nginx host(s)).

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

---

## Part C — Repo-level routes and DAG pruning

### The exposure

`isAuthorized` is attached only to `repoMux`/`nodeMux`/`instanceMux`
(`server/web.go:811/830/851`). The plural repos routes live on `mainMux` and
are **never** auth-gated:

```
web.go:795  mainMux.Post("/api/repos", reposPostHandler)     // create repo: ungated
web.go:796  mainMux.Get("/api/repos/info", reposInfoHandler)  // every repo's full DAG: ungated
```

And `repoInfoHandler` (`/api/repo/:uuid/info`) *is* gated, but the public bypass
returns the **entire** repo DAG (`repoT.MarshalJSON` → full `dagT`), including
private descendant branches — a leak when `:uuid` is a public node.

### Access scope helper

Add a soft-auth helper that classifies a request *without* rejecting (a public
user must still see public repos):

```go
type accessScope struct {
    internal bool              // X-DVID-Internal trusted
    user     *dsgUserCache     // nil if no/invalid token
    full     bool              // internal or admin -> sees everything
}

func requestAccessScope(r *http.Request) accessScope
```

- internal (or `user.Admin`) → `full = true`.
- valid DSG token → `user` set; per-repo visibility decided by
  `userHasDatasetAccess(user, dataset_map[root], "GET")`.
- otherwise → public-only.

### `GET /api/repos/info` — filter to accessible repos/nodes

Replace the bare `datastore.MarshalJSON()` (`web.go:1692`) with a filtered
marshal that, per repo:

- `full` scope → include the whole repo.
- authed user with dataset access to that repo's root → include the whole repo.
- else → include a **pruned** repo whose DAG contains only public nodes.
- **Hide repos with zero visible nodes** entirely (per review).

### `GET /api/repo/:uuid/info` — prune on the public/unauth path

When the request is served via the public bypass (or an authed user lacking
dataset access), return the pruned DAG instead of the full one. Authenticated
users with access (and internal) get the full DAG unchanged.

### Pruned-DAG construction (`datastore`)

Add `datastore.GetRepoJSONFiltered(uuid, scope)` (and a `MarshalJSONFiltered`
for the repos list) that builds a copy of `dagT` keeping only visible nodes:

- keep node iff `IsPublicVersion(node.version)` (public scope) — ancestor
  closure guarantees these form a connected sub-DAG rooted at `dag.root`.
- within each kept node, **filter `children` to kept versions** (drop private
  HEAD/branch tips) so no private UUID leaks by reference. `parents` need no
  filtering (ancestors are always public).
- branch HEAD / `branch-versions` references that point at private nodes are
  dropped or repointed to the latest visible node on that branch.
- `DataInstances`, alias, description, repo/node notes, log: **kept** if the
  repo is visible (per review — public metadata is acceptable). Repo log lines
  are coarse; node logs are reachable only via node routes which are gated.

Because `dagT` is `{root, nodes map[VersionID]*nodeT}`, the filtered copy is a
new `dagT` with a reduced `nodes` map and trimmed `children` slices; existing
`dagT.MarshalJSON` then serializes it unchanged.

### `POST /api/repos`

Admin-only, not DSG-scoped (a new repo has no root UUID to map to a dataset).
Requires `adminPriv` and a non-read-only server; fixed in `reposPostHandler`
(`server/web.go`).

Also revisit the sibling leak-y/gated routes for consistency:
`/api/repo/:uuid/log`, `/api/repo/:uuid/branch-versions/:name`,
`/api/node/:uuid/log|note` — these are under `isAuthorized`, so the public
bypass applies; ensure they only expose public-node data on the public path.

---

## Config example (`[auth]`)

```toml
[auth]
enforce = "dsg"               # external default
enforce_internal = "none"     # internal clients (X-DVID-Internal) skip DSG
dsg_address = "https://auth.janelia.org"
dsg_cache_ttl = 300
# trusted_proxies = ["127.0.0.1/32", "10.10.1.0/24"]  # optional: only trust header from nginx
public_versions = ["2f4a...", "7b91..."]  # each UUID + all ancestors are public (reads)
dataset_map = { "2f4a..." = "vnc", "7b91..." = "manc" }  # root UUID -> DSG dataset
```

Note: `internal_cidrs` (old client-IP meaning) is removed.

---

## Files to modify

| File | Change |
|------|--------|
| `datastore/public.go` (new) | `publicSet`, `SetPublicVersions`, `IsPublic`, `IsPublicVersion`, `ancestorsClosure` |
| `datastore/repo_local.go` | multi-parent `ancestorsClosure`; `GetRepoJSONFiltered` / filtered `MarshalJSON`; pruned `dagT` copy helper |
| `datastore/datastore.go` | export wrappers (`SetPublicVersions`, `IsPublic`, filtered JSON) |
| `server/auth.go` | `isPublicRead` via `datastore.IsPublic`; header-based `isInternalRequest`; `effectiveEnforce` rewrite; `enforce_internal` validation; drop `auth.public`/`internal_cidrs`; add optional `trusted_proxies`; `requestAccessScope` |
| `server/web.go` | filtered `reposInfoHandler`; pruned `repoInfoHandler` on public path; gate `reposPostHandler`; wire `datastore.SetPublicVersions` at startup |
| `scripts/distro-files/config-full.toml` | document new `public_versions` semantics, `enforce_internal`, `X-DVID-Internal`, `trusted_proxies`; remove `internal_cidrs` |

---

## Test plan

- `IsPublic`: configured UUID, an ancestor, a **merge** ancestor via `parents[1+]`
  (must be public), a private descendant (false), an unknown UUID (false),
  nothing configured (all false).
- `ancestorsClosure`: linear chain, merge node, diamond DAG.
- `isInternalRequest`: header present/absent/forged; with and without
  `trusted_proxies`.
- `effectiveEnforce`: internal+`enforce_internal=none`, external→`dsg`, empty
  `enforce_internal`, invalid value rejected at startup.
- Public bypass is read-only: `POST`/branch/newversion on a public UUID still
  requires auth.
- `/api/repos/info`: public scope hides private nodes and zero-public repos;
  authed-with-access sees full; internal sees full.
- `/api/repo/:uuid/info`: pruned children, no private UUID by reference;
  full DAG for authed-with-access.
- `POST /api/repos`: rejected for external-without-token; allowed for internal /
  admin per chosen rule.

---

## Open questions / decisions for review

1. **`trusted_proxies` hardening** — include now (recommended) or rely on header
   alone + network isolation? Affects whether `internal_cidrs` is removed or
   repurposed.
2. **Configurable internal header name/value** vs hardcoded `X-DVID-Internal:
   true` (plan hardcodes a const).
3. **Pruned metadata** — confirm `DataInstances`/alias/notes/log are acceptable
   to show on any repo that has ≥1 public node (plan assumes yes per review).
4. **`enforce_internal` string vs bool** — plan keeps string + validation;
   confirm we want the extra flexibility over a simple `internal_no_auth` bool.

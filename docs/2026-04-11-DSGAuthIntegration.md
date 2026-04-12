# DVID ↔ DatasetGateway Auth Integration

## Context

DVID servers running inside Janelia's network (DMZ or k8s nodes reachable externally) need to enforce authentication and authorization for external clients while optionally allowing unauthenticated access for internal clients. DatasetGateway already provides the auth stack (Google OAuth, dataset-scoped grants, TOS, permission cache). The goal is to connect DVID's existing auth middleware to DatasetGateway as the identity and authorization backend.

DVID already has the plumbing for this — its `[auth]` config section, the `isAuthorized` JWT middleware on repo/node/instance routes, and a `/api/server/token` endpoint that exchanges Google credentials for a JWT. The current design just needs to be pointed at DatasetGateway instead of the legacy `flyem-services` proxy or standalone Google token validation.

## How it works today

```
                    DVID auth flow (current)
                    ========================

Client ──Bearer JWT──► DVID
                        │
                  isAuthorized()
                        │
              ┌─────────┼─────────┐
              │         │         │
         enforce=none  token    authfile
         (pass all)  (valid JWT) (JWT user in auth_file)
```

- `serverTokenHandler` (`GET /api/server/token`) issues JWTs by either:
  - **Legacy proxy**: calling `getEmailFromProxy()` → forwards cookies to `https://{proxy_address}/profile` → gets email → signs JWT
  - **Google OAuth**: validating a Google ID token via `oauth2.Tokeninfo()` → gets email → signs JWT
- `isAuthorized` middleware validates `Authorization: Bearer {jwt}` on repo/node/instance routes
- `enforce` modes: `none` (open), `token` (any valid JWT), `authfile` (JWT user must be in a local JSON file)
- Public versions bypass auth for read-only requests

## Design: DSG as DVID's auth backend

```mermaid
flowchart LR
    subgraph External
        EC[External Client]
    end
    subgraph Internal
        IC[Internal Client]
    end
    subgraph DSG["DatasetGateway"]
        OAuth[OAuth Login]
        Cache["/api/v1/user/cache"]
        Token[dsg_token / APIKey]
    end
    subgraph DVID
        TH["/api/server/token"]
        IA["isAuthorized middleware"]
        Data["repo/node/instance handlers"]
    end

    EC -- "1. Login" --> OAuth
    OAuth -- "dsg_token cookie" --> EC
    EC -- "2. GET /api/server/token\n(Bearer dsg_token)" --> TH
    TH -- "3. GET /api/v1/user/cache\n(Bearer dsg_token)" --> Cache
    Cache -- "user cache JSON" --> TH
    TH -- "4. JWT (user + permissions)" --> EC
    EC -- "5. Bearer JWT" --> IA
    IA -- "6. validate JWT, check dataset" --> Data

    IC -. "optional: same flow\nor direct access\n(enforce=none)" .-> Data
```

### Core idea

Add a new `enforce` mode — `"dsg"` — to DVID's `authConfig` that integrates with DatasetGateway:

1. **Token issuance** (`/api/server/token`): Instead of calling a legacy proxy or Google directly, DVID calls DatasetGateway's `GET /api/v1/user/cache` with the client's `dsg_token` (passed as a Bearer token). DSG returns the user's identity + permissions. DVID embeds the email and permissions into a JWT and returns it.

2. **Authorization** (`isAuthorized` middleware): When `enforce = "dsg"`, after validating the JWT, DVID checks the embedded dataset permissions against the dataset being accessed (derived from the UUID's root repo alias/name). This replaces the flat `authfile` with dataset-scoped authorization.

3. **Internal/external split**: DVID's config gains an `internal_cidrs` list. Requests from internal IPs can optionally skip auth (`enforce_internal = "none"`) while external requests require DSG auth (`enforce = "dsg"`). This gives per-deployment control over internal access.

### Changes to DVID (Go)

#### 1. `server/auth.go` — Extend `authConfig`

```toml
[auth]
enforce = "dsg"                    # new mode: use DatasetGateway
enforce_internal = "none"          # optional: separate policy for internal IPs
dsg_address = "https://auth.example.org"  # DatasetGateway base URL
internal_cidrs = ["10.0.0.0/8", "172.16.0.0/12"]  # Janelia internal ranges
dataset_name = "vnc1"              # which DSG dataset this DVID instance serves
public_versions = []               # unchanged: committed UUIDs with public access
```

New fields on `authConfig`:
```go
type authConfig struct {
    PublicVersions  []string `toml:"public_versions"`
    ProxyAddress    string   `toml:"proxy_address"`    // legacy, kept for compat
    AuthFile        string   `toml:"auth_file"`
    Enforce         string   `toml:"enforce"`          // "none", "token", "authfile", "dsg"
    NoEnforce       bool     `toml:"no_enforce"`       // legacy

    // New DSG fields
    EnforceInternal string   `toml:"enforce_internal"` // policy for internal IPs; defaults to Enforce
    DSGAddress      string   `toml:"dsg_address"`      // DatasetGateway base URL
    InternalCIDRs   []string `toml:"internal_cidrs"`   // CIDRs considered internal
    DatasetName     string   `toml:"dataset_name"`     // DSG dataset name for this DVID server
}
```

#### 2. `server/auth.go` — New `getDSGUserCache()` function

Calls `GET {dsg_address}/api/v1/user/cache` with the client's `dsg_token` as a Bearer token. Returns the parsed user cache (email, permissions map, admin flag).

```go
type dsgUserCache struct {
    Email         string              `json:"email"`
    Admin         bool                `json:"admin"`
    PermissionsV2 map[string][]string `json:"permissions_v2"`
}

func getDSGUserCache(dsgToken string) (*dsgUserCache, error) {
    // HTTP GET to tc.Auth.DSGAddress + "/api/v1/user/cache"
    // with Authorization: Bearer {dsgToken}
    // parse JSON response into dsgUserCache
}
```

#### 3. `server/auth.go` — Extend `generateJWT()` to embed permissions

Currently JWTs only contain `user` (email). Add a `permissions` claim:

```go
claims["user"] = email
claims["permissions"] = permissionsV2  // map[string][]string from DSG
claims["admin"] = admin
claims["exp"] = time.Now().Add(24 * time.Hour).Unix()
```

Also add expiration (`exp`) claim — currently JWTs never expire, which is a security gap.

#### 4. `server/auth.go` — Extend `serverTokenHandler()`

Add a `"dsg"` branch alongside the existing proxy and Google branches:

```go
func serverTokenHandler(w http.ResponseWriter, r *http.Request) {
    var email string
    if tc.Auth.Enforce == "dsg" {
        // Extract dsg_token from Authorization header
        dsgToken := extractBearerToken(r)
        if dsgToken == "" {
            BadRequest(w, r, "dsg_token required")
            return
        }
        cache, err := getDSGUserCache(dsgToken)
        if err != nil {
            BadRequest(w, r, "DSG auth failed: %v", err)
            return
        }
        email = cache.Email
        // Generate JWT with permissions embedded
        tokenString, err := generateJWTWithPermissions(email, cache.PermissionsV2, cache.Admin)
        // ...
    } else if len(tc.Auth.ProxyAddress) != 0 {
        // existing legacy proxy flow
    } else {
        // existing Google ID token flow
    }
}
```

#### 5. `server/auth.go` — Extend `isAuthorized()` middleware

For `enforce = "dsg"`:
- Parse JWT and extract `user`, `permissions`, `admin` claims
- If `admin` is true, allow all requests
- Look up the repo name from `c.Env["uuid"]` (already resolved by `repoRawSelector`)
- Check if `tc.Auth.DatasetName` has at least `"view"` in the JWT permissions for GET/HEAD/OPTIONS, or `"edit"` for POST/PUT/DELETE
- For internal IPs (matched against `internal_cidrs`), apply `enforce_internal` policy instead

```go
func isAuthorized(c *web.C, h http.Handler) http.Handler {
    fn := func(w http.ResponseWriter, r *http.Request) {
        if isPublic(r, c.Env["uuid"]) {
            h.ServeHTTP(w, r)
            return
        }

        enforce := effectiveEnforce(r)  // checks IP against internal_cidrs
        switch enforce {
        case "none":
            h.ServeHTTP(w, r)
            return
        case "dsg":
            // parse JWT, extract permissions, check dataset access
        case "token":
            // existing: valid JWT is enough
        case "authfile":
            // existing: user must be in auth file
        }
    }
}

func effectiveEnforce(r *http.Request) string {
    if len(tc.Auth.InternalCIDRs) > 0 && isInternalIP(r) {
        if tc.Auth.EnforceInternal != "" {
            return strings.ToLower(tc.Auth.EnforceInternal)
        }
    }
    return strings.ToLower(tc.Auth.Enforce)
}
```

#### 6. `server/auth.go` — New `isInternalIP()` helper

Parses client IP from `X-Forwarded-For` or `RemoteAddr`, checks against parsed `internal_cidrs` (parsed once at startup into `[]*net.IPNet`).

### Changes to DatasetGateway (this repo)

No code changes required for the core integration — DVID calls existing DSG endpoints (`/api/v1/user/cache`). However, one small addition would be useful:

#### Optional: `POST /api/v1/check-dvid-access` convenience endpoint

A lightweight endpoint optimized for DVID's use case that answers "can this user read/write dataset X?" in a single call, without the client needing to parse the full permission cache. This is optional — DVID can parse the existing `/api/v1/user/cache` response instead.

This could also just be the existing `POST /api/v1/check-access` endpoint, which already does exactly this.

### Deployment configuration

For a DVID server at Janelia serving the `vnc1` dataset:

```toml
# dvid.toml
[auth]
enforce = "dsg"
enforce_internal = "none"           # internal clients: no auth required
dsg_address = "https://auth.janelia.org"
dataset_name = "vnc1"
internal_cidrs = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
public_versions = ["abc123"]        # specific committed UUIDs always public
```

Environment variable:
```
DVID_JWT_SECRET_KEY=<RSA private key for JWT signing>
```

### Client workflow

1. User logs into DatasetGateway via Google OAuth → gets `dsg_token` cookie
2. Client calls `GET dvid-server/api/server/token` with `Authorization: Bearer {dsg_token}`
3. DVID calls DSG's `/api/v1/user/cache` to validate the token and get permissions
4. DVID returns a JWT with the user's email and dataset permissions embedded
5. Client uses the JWT for all subsequent DVID API calls
6. DVID's `isAuthorized` middleware validates the JWT and checks dataset permissions on each request

### What doesn't change

- DVID's route structure and middleware pipeline
- The `repoRawSelector` → `isAuthorized` → handler chain
- Public version bypass logic
- Blocklist functionality
- CORS handling
- The `authorizationOn` flag (now true when `enforce != "none"`)

## Implementation order

All changes are to the DVID repo (`server/auth.go` primarily, with a config addition to `server/server_local.go`):

1. **Add new `authConfig` fields** in `server/server_local.go` and `server/auth.go`
2. **Add `isInternalIP()` and `effectiveEnforce()`** — IP classification logic  
3. **Add `getDSGUserCache()`** — HTTP client to call DSG
4. **Extend `generateJWT()`** — embed permissions + expiry in JWT claims
5. **Extend `serverTokenHandler()`** — add `"dsg"` branch
6. **Extend `isAuthorized()`** — add `"dsg"` enforcement with dataset permission checking
7. **Update `initRoutes()`** — set `authorizationOn = true` when `enforce == "dsg"`
8. **Update config example** — `scripts/distro-files/config-full.toml`

## Verification

1. **Unit test**: `isInternalIP()` with various CIDRs and IPs
2. **Unit test**: JWT generation with permissions claims, verify parsing
3. **Unit test**: `effectiveEnforce()` returns correct policy for internal vs external IPs
4. **Integration test**: Mock DSG server, verify token exchange flow end-to-end
5. **Manual test**: Deploy DVID with `enforce = "dsg"`, authenticate via DSG, verify access control
6. **Manual test**: Verify internal IP clients bypass auth when `enforce_internal = "none"`
7. **Manual test**: Verify external client without JWT gets 400/401 on repo/node/instance routes

## Files to modify

| File | Change |
|------|--------|
| `server/auth.go` | `authConfig` struct, `getDSGUserCache()`, `effectiveEnforce()`, `isInternalIP()`, extend `generateJWT()`, extend `serverTokenHandler()`, extend `isAuthorized()` |
| `server/server_local.go` | Parse `internal_cidrs` into `[]*net.IPNet` at startup in `Serve()` |
| `server/web.go` | Update `authorizationOn` condition: `len(tc.Auth.ProxyAddress) != 0 || tc.Auth.Enforce == "dsg"` |
| `scripts/distro-files/config-full.toml` | Add `dsg` mode documentation and example fields |

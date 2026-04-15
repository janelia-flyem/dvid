# DSG Authorization for DVID

This page describes how to configure DVID to authenticate requests with DatasetGateway (DSG) and authorize access per dataset.

## Overview

In DSG mode, DVID does not mint its own auth token. Clients send a DSG token directly to DVID, and DVID:

1. validates the token against DSG's `/api/v1/user/cache` endpoint
2. caches the returned user info in memory for a short TTL
3. resolves the request's root UUID
4. maps that root UUID to a canonical DSG dataset ID
5. checks the user's DSG permissions for that dataset

This mode is enabled with:

```toml
[auth]
enforce = "dsg"
```

## Config

Example:

```toml
[auth]
enforce = "dsg"
enforce_internal = "none"
dsg_address = "https://auth.janelia.org"
dsg_cache_ttl = 300
internal_cidrs = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16"]
dataset_map = {
  "2f4a..." = "vnc",
  "7b91..." = "manc"
}
public_versions = ["abc123"]
```

Fields:

- `enforce = "dsg"` enables DSG-backed authentication and authorization.
- `enforce_internal` optionally overrides the auth policy for trusted internal clients.
- `dsg_address` is the base URL of the DatasetGateway instance.
- `dsg_cache_ttl` is the in-memory cache TTL in seconds for DSG user info.
- `internal_cidrs` defines which client IP ranges count as internal.
- `dataset_map` maps DVID root UUIDs to canonical DSG dataset IDs.
- `public_versions` still allows public read-only access for listed committed UUIDs.

## Token Sources

DVID checks for a DSG token in this order:

1. `Authorization: Bearer <dsg_token>`
2. `dsg_token` cookie

`Authorization` headers are recommended for API clients. Cookies are useful for browser-based tools using DSG SSO.

## Authorization Rules

For DSG mode:

- `GET`, `HEAD`, and `OPTIONS` require DSG `view` access
- mutation methods require DSG `edit`, `manage`, or `admin` access
- DSG global admin users are allowed through directly
- if the request's root UUID is not in `dataset_map`, DVID denies access

## Status Codes

DSG-related failures return these statuses:

- `401` for missing or invalid DSG tokens
- `403` for insufficient dataset access or missing dataset mapping
- `502` when DVID cannot reach DSG or DSG returns an unexpected upstream error

Response bodies include a readable error message plus the request path, consistent with other DVID HTTP errors.

## Internal Bypass

If `enforce_internal = "none"` is set, requests from `internal_cidrs` bypass auth.

Use this carefully:

- prefer trusted network segments only
- only trust forwarded client IPs behind a known proxy
- avoid broad CIDR definitions unless they are operationally justified

## Legacy Notes

`/api/server/token` is not used in DSG mode. It remains relevant only for legacy JWT-based auth configurations.

## Operational Notes

- Use short cache TTLs if permission changes need to take effect quickly.
- Keep `dataset_map` authoritative and explicit rather than inferring from repo aliases.
- If you change auth-file configuration and use `POST /api/server/reload-auth`, DVID also clears the DSG user cache.

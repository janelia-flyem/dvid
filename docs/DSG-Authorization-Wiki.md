# DSG Authorization for DVID

This page describes DVID's native DatasetGateway (DSG) authentication,
node-grain authorization, Terms-of-Service handling, and public-release
seeding.

## Overview

In DSG mode, DVID does not mint an application token. A client sends its DSG
token directly to DVID, and DVID uses only the native `/api/dsg/v1` surface:

1. `GET /api/dsg/v1/user` resolves the authenticated identity.
2. `POST /api/dsg/v1/authorize` evaluates one or more node-grain targets.
3. DVID evaluates cross-branch `service_eval` anchors against its local DAG.
4. DVID periodically reads native dataset/version metadata to seed anonymous
   public reads.

The authenticated identity and evaluated authorization decisions share the
configured cache TTL. A pending Terms-of-Service decision is never cached.

## Configuration

```toml
[auth]
enforce = "dsg"
enforce_internal = "none"
dsg_address = "https://auth.janelia.org"
dsg_service = "dvid"
dsg_cache_ttl = 300
dsg_public_refresh = 3600
trusted_proxies = ["127.0.0.1/32", "10.10.1.0/24"]
```

Set DVID's metadata credential in the service environment, not in TOML:

```text
DVID_DSG_TOKEN=<dedicated-service-account-token>
```

Fields:

- `enforce = "dsg"` enables native DSG authentication and authorization.
- `enforce_internal` optionally overrides the policy for requests bearing a
  trusted `X-DVID-Internal: true` header.
- `dsg_address` is the base URL of the DSG instance.
- `dsg_service` is the DSG service name and alias vocabulary; it defaults to
  `dvid`.
- `dsg_cache_ttl` is the identity and evaluated-decision cache TTL in seconds;
  it defaults to 300.
- `dsg_public_refresh` is the public metadata refresh interval in seconds; it
  defaults to 3600, and `0` means startup only.
- `trusted_proxies` optionally limits which nginx peers may set the internal
  header.
- `DVID_DSG_TOKEN` authenticates DVID's own metadata requests. A dedicated
  service account with no grants is sufficient because public datasets remain
  visible to that principal.

The `dataset_map` and `public_versions` settings are retired. Configuring
either causes a pointed startup error: dataset aliases, version anchors, and
public flags now live in DSG.

## DSG Registration

Create a DSG `Service` named `dvid` with DAG version evaluation. For every
local DVID repository:

1. Create the DSG dataset.
2. Add a name-level service alias whose client name is the full DVID root UUID.
3. Register release anchors as dataset versions. Use DVID's branch name
   (`master` for the default branch) and the raw decimal DVID `VersionID` as
   the ordinal.
4. Add a version-level service alias for every anchor, with client name equal
   to the root UUID and client version equal to the decimal `VersionID`.
5. Mark public release anchors and attach Terms-of-Service documents as
   required.

The version-level alias is required for version-scoped Terms-of-Service. A
bare numeric target can otherwise be resolved only as an ordinal and will not
bind to the dataset-version row that owns the document.

## Client Tokens and Identity

DVID checks for a token in this order:

1. `Authorization: Bearer <dsg_token>`
2. `dsg_token` cookie

Bearer headers are recommended for API clients. Cookies support browser tools
that use DSG SSO.

Human users and user-type service accounts are attributed by email. Dedicated
service accounts have no email and are attributed by their DSG slug name. The
authenticated value overrides a client-provided `?u=` value in activity
records, datatype request contexts, and HTTP mutation records.

## Node-Grain Authorization

For each non-public UUID-scoped request, DVID sends an authorize entry with:

- `name`: the repository's full root UUID
- `branch`: the node's branch (`master` for the default branch)
- `version`: the node's raw decimal `VersionID`
- `permission`: `view` for `GET`, `HEAD`, and `OPTIONS`; `edit` for mutation
  methods

An `allow` decision still must carry an applicable role. `view` is read-only;
`edit`, `manage`, and `admin` permit reads and writes. Unknown roles do not
grant DVID access.

DSG can decide dataset-grain and same-branch containment directly. For a
cross-branch `service_eval` response, DVID skips unknown anchors, applies the
ordinal pre-filter, and then confirms that the requested node equals or
precedes an anchor in the local version DAG. The evaluated verdict is cached
by token, root UUID, branch, version, and requested permission.

DSG administrators short-circuit before the authorize call. This deliberately
preserves administrator access to local repositories that have not yet been
registered in DSG.

## Terms of Service

When DSG returns `tos_required`, DVID responds with HTTP 403 and includes the
opaque `tos_url` in its plain-text error. The client opens that URL in a
browser, accepts the terms, and retries the original API request. Pending TOS
decisions are not cached, so the retry queries DSG immediately instead of
waiting for the cache TTL.

## Public Reads

After datastore initialization, DVID uses `DVID_DSG_TOKEN` to list visible DSG
datasets and their registered versions. It collects committed local anchors
marked public, computes their complete ancestor closure in the local DAG, and
atomically publishes that set for unauthenticated `GET`, `HEAD`, and `OPTIONS`
requests.

Unknown datasets, null or unknown ordinals, cross-repository ordinals, and
uncommitted anchors are skipped with a log message. A startup fetch failure
publishes an empty set (fail closed). A later refresh failure retains the
last-good set. If the credential is absent, DVID warns and anonymous public
reads remain disabled until configuration is corrected.

Public-read detection remains the first authorization check. A UUID already
in the published ancestor set therefore remains readable without a token.

## Repository Metadata

Repository visibility is determined with a `view` probe of the repository root
node, not a dataset-name-only request. Every dataset- or version-grain grant
covers that root. An allowed root probe gets full repository metadata;
denied or TOS-pending probes get the public-only DAG and no private repo log.

`GET /api/repos/info` enumerates all local roots and sends one authorize batch,
regardless of repository count. Per-repository info, branch-version, and
repo-log routes use the same root-probe rules. The mutation blobstore is not
root-addressed and continues to require a valid authenticated identity.

## Status Codes

- `401`: token missing, invalid, or identity malformed
- `403`: insufficient access, or Terms-of-Service acceptance required
- `502`: DSG is unreachable or returns an invalid/unexpected response

Authorize responses are correlated against the request entry echoes. A length
or echo mismatch rejects the whole fetch and caches none of its decisions.

## Internal Bypass

If `enforce_internal = "none"` is set, requests with a trusted
`X-DVID-Internal: true` header bypass authentication.

```nginx
# internal vhost
proxy_set_header X-DVID-Internal "true";

# public vhost
proxy_set_header X-DVID-Internal "";
```

Keep DVID's direct port unreachable from untrusted networks. Configure the
public vhost to clear client-supplied internal headers, set `trusted_proxies`
when needed, and block public access to server administration/configuration and
mutation blobstore routes.

## Legacy Modes

`token` and `authfile` remain deprecated legacy JWT modes and retain their
existing behavior and startup warnings. `/api/server/token` is not used in DSG
mode.

## Rollout Checklist

1. Deploy the native DSG API and authorization ledger.
2. Register the DVID service, root aliases, anchor versions, per-anchor aliases,
   public flags, grants, and Terms-of-Service documents.
3. Provision `DVID_DSG_TOKEN` and writer service-account tokens.
4. Remove retired settings, configure the native DSG fields, rebuild DVID, and
   redeploy.
5. Verify anonymous public/private reads; same- and cross-branch grants;
   sibling-branch denial; TOS acceptance and immediate retry; dedicated-service-
   account write attribution; view-only write denial; administrator access to
   an unregistered repository; filtered repo metadata; and a downstream client
   smoke test.

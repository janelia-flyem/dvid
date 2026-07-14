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

Clio-backed deployments also set the capability signing secret in the DVID
process environment:

```text
DVID_CAPABILITY_SECRET=<high-entropy-shared-secret>
# During a rolling rotation only:
DVID_CAPABILITY_PREVIOUS_SECRET=<previous-secret>
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
- `DVID_CAPABILITY_SECRET` enables the Clio broker and signs its short-lived
  capabilities. It is independent of DSG user tokens and legacy DVID JWTs.
- `DVID_CAPABILITY_PREVIOUS_SECRET` is verification-only and should be set only
  during a rolling secret rotation.

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
  methods. A datatype may declare an immutable POST endpoint, such as
  neuronjson `/query`; that endpoint also uses `view`.

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

## Clio Authorization Broker

`POST /api/auth/clio/:uuid` is a narrow, authenticated decision broker for
clio-store. It is available only when top-level `enforce = "dsg"`,
`dsg_address`, and `DVID_CAPABILITY_SECRET` are configured. The request carries
a DSG Bearer token and this JSON body:

```json
{"permission": "view", "return_url": "https://clio.example.org/"}
```

`permission` must be `view` or `edit`; `return_url` is optional. DVID uniquely
resolves the path UUID, derives its root, branch, and raw decimal `VersionID`,
and submits that target to DSG with the service fixed to `clio`. The caller
cannot provide a branch, ordinal, canonical dataset name, or service. DVID uses
the same local-DAG evaluation as direct DSG authorization for any
`service_eval` anchors. A DSG global administrator is still authenticated, but
can receive a scoped capability without an alias-backed decision.

An `allow` response contains the decision roles and an opaque capability. A
`tos_required` response contains only its opaque `tos_url`; deny and all error
responses contain no capability. Broker responses do not expose the resolved
root, branch, ordinal, canonical dataset, or anchors.

clio-store presents the allow value only as:

```text
Authorization: DVID-Capability <opaque>
```

The HMAC-authenticated value binds `service=clio`, the full node UUID, root,
raw `VersionID`, `view|edit` permission, original authenticated principal,
issue and expiry times, and a random nonce. Its lifetime is 60 seconds with at
most five seconds of verifier clock skew. It is intentionally replayable within
that window for the same node and permission: it is neither path-, body-, nor
one-use-bound. Every replay retains the original principal for request context,
activity, and mutation attribution. A view capability cannot authorize a
mutation; an edit capability covers reads and writes.

Capability verification runs before public-read, internal-header, and DSG-token
authorization. Any presented malformed, tampered, expired, cross-node, or
wrong-permission capability fails closed and never falls through. When the
scheme is absent, the existing public, DSG Bearer, and `X-DVID-Internal` flows
are unchanged.

For a rolling signing-secret rotation, first deploy the new value as
`DVID_CAPABILITY_SECRET` and the old value as
`DVID_CAPABILITY_PREVIOUS_SECRET` to every DVID broker/data-serving process.
After all processes mint with the new active key, wait more than one full
60-second lifetime plus the five-second skew allowance, then remove the
previous key. DVID always mints with the active key and verifies against active
then previous. An unset active key disables only the broker/capability path
with HTTP 503; unrelated authorization modes continue to operate.

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
   public flags, grants, and Terms-of-Service documents. Ensure the existing
   `clio` service uses DAG evaluation; for each Clio-backed repository add the
   full root UUID as a name alias and add a `(root UUID, decimal VersionID)`
   alias for every published anchor. Retain Firestore-id aliases used by Clio
   listing and `/dataset-access`. Inspect metadata listings for the known wart
   where alphabetic aliases can be omitted even though direct resolution works.
3. Provision `DVID_DSG_TOKEN`, writer service-account tokens, and the same
   active/previous capability secrets on every DVID process that brokers or
   serves Clio requests.
4. Remove retired settings, configure the native DSG fields, rebuild DVID, and
   deploy it before clio-store. Keep top-level `enforce = "dsg"`; Clio does not
   depend on `enforce_internal`.
5. No Firestore `dvid` URL, nginx, dual-horizon DNS, or internal-header change is
   required. The legacy `X-DVID-Internal` path remains available to other
   clients, but Clio sends neither that header nor a DSG token to data routes.
6. Verify anonymous public/private reads; same- and cross-branch grants;
   sibling-branch denial; TOS acceptance and immediate retry; dedicated-service-
   account write attribution; view-only write denial; administrator access to
   an unregistered repository; filtered repo metadata; Clio dataset- and
   version-grain grants; read-only POST query; deny/admin behavior; capability
   tampering, expiry, and cross-node reuse; and a downstream Clio smoke test.

# Agent sign-in hosted by Flux Drop

Drop is the OAuth authorization server for the Flux Apps MCP resource. Clients
connect to `https://runonflux.com/apps/mcp`, discover Drop, open a browser, and use
Google or a verified email/password Firebase account. There is no agent token
for a person to copy. This requires a primary/standalone **Raft** deployment;
secondary storage apps and the legacy Firestore emulator do not host OAuth.

All URLs below use `D = DROP_PUBLIC_ORIGIN` (normally
`https://drop.app.runonflux.io`). JSON dates are RFC 3339 UTC. New endpoints do
not change the existing browser sessions, agent API keys, or publishing APIs.
The existing `GET /api/config` adds `agentAuthEnabled: boolean`.

## Website integration and flow

1. The website handles a Firebase JWT bearer itself. It forwards every other MCP
   request, with the original method/body and relevant MCP headers, to
   `D/agent/mcp`, adding `X-Flux-Agent-Forwarded: 1`. GET and DELETE must also be
   forwarded. Preserve the gateway's status, headers and streaming response.
2. The website serves **both**
   `/apps/.well-known/oauth-protected-resource` and
   `/apps/.well-known/oauth-protected-resource/mcp` with:

   ```json
   {"resource":"https://runonflux.com/apps/mcp","authorization_servers":["https://drop.app.runonflux.io"],"scopes_supported":["orbit","drop"],"bearer_methods_supported":["header"]}
   ```

3. Without a valid agent token, the gateway returns 401 with:

   ```http
   WWW-Authenticate: Bearer resource_metadata="https://runonflux.com/apps/.well-known/oauth-protected-resource", scope="orbit drop"
   ```

   If any Authorization header was supplied, it adds `, error="invalid_token"`.
4. The client fetches Drop's authorization-server metadata, registers a public
   client (or supplies an HTTPS Client ID Metadata Document), and starts an
   authorization-code flow with mandatory PKCE S256, state and a registered
   callback. Discovery advertises `https://runonflux.com/apps/oauth/authorize`
   by default, so Firebase Google popup sign-in runs on the authorized website
   domain. The resource indicator, when supplied, must match exactly.
5. Drop's browser consent page authenticates the user using its local Firebase
   bundle, verifies the signed ID token against Google's securetoken JWKS,
   exchanges the Firebase refresh token, and checks the returned user ID.
   Allow issues a code; Deny returns `access_denied`. The browser navigates back
   with the original state and Drop's `iss` in either case.
6. The client exchanges the code and retains opaque OAuth access/refresh tokens.
   The gateway validates the access token and substitutes a fresh **Firebase ID
   token**, never an agent token, on the website's upstream MCP request.
7. The website's MCP tools use that live Firebase ID token with the Firebase
   bearer project API below. Do not call `/api/auth/google` to open a browser
   session: refreshed tokens retain their original `auth_time`, while browser
   login deliberately requires a recent sign-in. Website pages also manage
   connections and create upload links with their own live Firebase ID token.
   No website secret or shared website storage is needed.

The website must distinguish its accepted Firebase JWTs from Drop's opaque
43-character base64url access tokens. Never send a Firebase JWT through Drop's
MCP gateway: it is rejected as a loop guard. The forwarded marker is routing
information, not authentication, and is not forwarded upstream.

The website forwards **GET and POST `/apps/oauth/authorize`** to
`D/oauth/authorize`, preserving the raw query string/body, browser `Origin` and
`Sec-Fetch-*` headers. It forwards only the `__Host-drop-agent` cookie in both
directions and preserves Drop's response status and headers, including CSP,
X-Frame-Options, Cross-Origin-Opener-Policy and Location. Do not replace browser
Origin with Drop's origin. Direct GET/POST on Drop's `/oauth/authorize` continues
to work; a deployment using direct Google sign-in must authorize that serving
domain in Firebase. The callback `iss` always identifies Drop, never the website.

## Exact endpoint contracts

All JSON errors below use
`{"error":"<code>","error_description":"<readable message>"}` unless a JSON-RPC
error or an existing publisher error is explicitly described. Sensitive API
responses use `Cache-Control: no-store`; OAuth token responses also set
`Pragma: no-cache`. OAuth POSTs accept **body** parameters, not query parameters;
duplicate parameters are rejected. Only public clients are supported.

### Discovery

`GET D/.well-known/oauth-authorization-server` and
`GET D/.well-known/openid-configuration` return identical 200 JSON:

```json
{
  "issuer":"https://drop.app.runonflux.io",
  "authorization_endpoint":"https://runonflux.com/apps/oauth/authorize",
  "token_endpoint":"https://drop.app.runonflux.io/oauth/token",
  "registration_endpoint":"https://drop.app.runonflux.io/oauth/register",
  "revocation_endpoint":"https://drop.app.runonflux.io/oauth/revoke",
  "response_types_supported":["code"],
  "grant_types_supported":["authorization_code","refresh_token"],
  "code_challenge_methods_supported":["S256"],
  "token_endpoint_auth_methods_supported":["none"],
  "scopes_supported":["orbit","drop"],
  "client_id_metadata_document_supported":true,
  "authorization_response_iss_parameter_supported":true
}
```

The second URL is a discovery alias; Drop does not issue OIDC ID tokens and does
not advertise `openid` scope. Firebase remains the identity provider.

### Client registration and metadata documents

`POST D/oauth/register`, `Content-Type: application/json`:

```json
{"redirect_uris":["http://127.0.0.1:12345/callback"],"token_endpoint_auth_method":"none","client_name":"My agent","client_uri":"https://example.com","logo_uri":"https://example.com/logo.png"}
```

Only `redirect_uris` is required. `token_endpoint_auth_method` defaults to `none`.
Empty or missing `grant_types` defaults to `authorization_code` and `refresh_token`;
empty or missing `response_types` defaults to `code`. Registration responses and
stored metadata contain only the supported types requested, or those defaults.
Unsupported grant and response types are ignored; a client must support authorization_code and code.
Unrecognized RFC 7591 extension metadata is ignored. Client-supplied IDs/lifetimes
are overwritten.

201 JSON (optional URI/logo fields omitted if not supplied):

```json
{"client_id":"flux_<random>","redirect_uris":["http://127.0.0.1:12345/callback"],"client_name":"My agent","client_uri":"https://example.com","logo_uri":"https://example.com/logo.png","token_endpoint_auth_method":"none","response_types":["code"],"grant_types":["authorization_code","refresh_token"],"client_id_issued_at":1791504000,"expires_at":"2027-01-07T00:00:00Z"}
```

Registrations expire after 90 days. Missing names default to `MCP client`.
There are at most 20 redirect URIs; names are at most 200 UTF-8 bytes with no
control characters. Metadata requests are capped at 32 KiB. Invalid registrations
return 400 `invalid_client_metadata`; the shared IP limit is 20 attempts/minute
(429, `Retry-After: 60`). Nginx supplies the client address to the loopback Go
listener; arbitrary forwarded-for headers are not trusted.

Redirects may be HTTPS, HTTP loopback at `127.0.0.1`, `[::1]`, or `localhost`, or
private schemes `cursor`, `vscode`, `vscode-insiders` and reverse-domain schemes
such as `com.example.agent`. Credentials, fragments, opaque URIs, unsafe schemes,
and non-loopback HTTP are refused. Loopback ports may differ during authorize;
every other URI component must match. The token exchange must repeat the **actual
authorized URI**, including the port chosen at authorize time.

Alternatively `client_id` may be an HTTPS URL with a non-root path. Its JSON
metadata must include `client_id` equal to that full URL and valid `redirect_uris`;
it may include the same public-client metadata above. Drop fetches it with a
5-second timeout and 32 KiB cap, caches it for 5 minutes (at most 256 documents),
and refuses redirects, proxies, private/special-purpose IPs and mixed
public/private DNS answers. It dials a validated literal IP to prevent DNS
rebinding, retaining normal HTTPS certificate validation. The consent page shows
the verified URL domain; registered client names are explicitly unverified.
No client-controlled logo is fetched or rendered on the consent page.

### Authorization and consent

Browser authorization uses `DROP_AGENT_AUTHORIZE_URL`; the website forwards to
`GET D/oauth/authorize` with this query unchanged:

```text
response_type=code
client_id=<registered ID or metadata-document URL>
redirect_uri=<registered callback>
code_challenge=<base64url SHA-256(code_verifier)>
code_challenge_method=S256
state=<nonempty opaque value, at most 2048 bytes>
scope=orbit drop                         # optional; empty defaults to both
resource=https://runonflux.com/apps/mcp  # optional; exact configured resource
```

200 renders the embedded consent HTML. Invalid client/redirect requests render
400 HTML without redirection. Once the callback is validated, parameter errors
return 303 with `Location: <callback>?error=<OAuth error>&state=<state>&iss=<D>`
(preserving existing callback query parameters). Supported scopes are `orbit`
and `drop`. Pending requests expire after 10 minutes and may be redeemed once.
Authorization initiation is limited to 600 requests/IP/minute, allowing for
website proxies sharing a small number of server addresses. Registration retains
its separate 20 requests/IP/minute limit.

The consent page uses Google or email/password, shows the signed-in email,
client trust information, redirect host, a loopback warning, and requested
capabilities. Firebase must have the chosen sign-in provider enabled; password
accounts must verify their email first. Reset password uses Firebase's normal
email reset flow. Switching accounts is available before allowing access.

The consent page POSTs JSON to its own `window.location.pathname`, which the
website forwards to `POST D/oauth/authorize`. It requires the bound cookie:

```json
{"handle":"<opaque handle from rendered page>","csrf":"<CSRF from rendered page>","action":"allow","idToken":"<user.getIdToken(true)>","refreshToken":"<user.refreshToken>"}
```

For denial use `{"handle":"...","csrf":"...","action":"deny"}`; no Firebase
credentials are needed. The handler accepts only the exact Drop origin or the
origin of `DROP_AGENT_AUTHORIZE_URL` (not every `DROP_AGENT_WEBSITE_ORIGINS`
entry). The existing same-origin fetch-metadata rule, cookie binding and CSRF
checks still apply; an explicit cross-site or same-site `Sec-Fetch-Site` is
rejected. Allow verifies RS256 signature, issuer,
audience, expiry, verified email, no tenant, and Google/password provider; the
refresh exchange must return the same UID. Connections are capped at 50 active
grants/user. No Firebase credentials are returned to the MCP client.

200 JSON:

```json
{"redirect":"http://127.0.0.1:12345/callback?code=<opaque-code>&iss=https%3A%2F%2Fdrop.app.runonflux.io&state=<state>"}
```

On denial the redirect contains `error=access_denied` instead of `code`. The
embedded consent script navigates to this URL explicitly; the POST does not
make fetch follow a loopback or private-scheme redirect. Binding/expired/used
consent errors return 403; bad identity/session returns 401; a concurrent
redemption returns 409. Failed credential validation does not grant access.

### Token exchange, renewal and revocation

`POST D/oauth/token`, `Content-Type: application/x-www-form-urlencoded`:

```text
grant_type=authorization_code&client_id=<ID>&code=<code>&redirect_uri=<authorized URI>&code_verifier=<43-128 unreserved ASCII characters>
```

Or renewal:

```text
grant_type=refresh_token&client_id=<ID>&refresh_token=<refresh token>
```

Either may include `resource=<exact resource>`. No client secret or Authorization
header is accepted. The request cap is 32 KiB.

200 JSON:

```json
{"access_token":"<opaque>","token_type":"Bearer","expires_in":3600,"refresh_token":"<opaque rotated token>","scope":"orbit drop"}
```

Scopes are returned in the order `orbit`, `drop`, omitting ungranted scopes.
Codes last 60 seconds and require exact client/redirect and PKCE. Invalid grants
return 400 `invalid_grant`. A used code or refresh token revokes the entire grant,
including already-issued access tokens. A wrong PKCE verifier does not consume
a code. Refresh tokens rotate on each successful exchange, with 90-day absolute
and 30-day idle lifetimes measured from the connection/last OAuth renewal.
Refreshing also renews Firebase, storing any rotated Firebase refresh token.
Firebase refusal returns `invalid_grant` and revokes the connection. If a refresh
has been consumed and an external failure makes renewal indeterminate, Drop
revokes the connection and returns 503; reconnect rather than reuse the old token.

`POST D/oauth/revoke`, same form encoding:

```text
token=<access or refresh token>&token_type_hint=refresh_token&client_id=<ID>
```

Only `token` is required; `token_type_hint` and `client_id` are optional. If given,
client_id must match before revocation is applied. 200 has an empty body, including
unknown tokens. Revocation disables the entire grant and all its tokens. No
client secret or Authorization header is needed or accepted.

### MCP gateway

`POST`, `GET`, `DELETE D/agent/mcp` use
`Authorization: Bearer <Drop access token>`. POST accepts one JSON-RPC 2.0 request, notification or response;
GET supports SSE. DELETE forwards streamable-HTTP session deletion. Body cap is
10 MiB; long calls/streams have a ten-minute deadline. The gateway ignores browser
cookies and never treats the forwarded marker as authorization.

Invalid/missing/expired/revoked/wrong-resource tokens, including Firebase JWTs,
return the 401 challenge described above. Backend/refresh outages return 503.
The gateway passes the upstream status and streams response chunks immediately,
without buffering. It forwards `Content-Type`, `Accept`, `Mcp-Session-Id`,
`MCP-Protocol-Version`, `Last-Event-ID`; it replaces Authorization and adds
`X-Flux-Agent-Client: <client name>`. Response headers passed through are
`Content-Type`, `Mcp-Session-Id`, `WWW-Authenticate`. Hop-by-hop headers and
Connection-nominated headers are stripped. Cookies are never forwarded.
Upstream redirects are returned without following them.

For `tools/call`, `drop_*` names require `drop`, all other names require `orbit`.
Missing scope returns HTTP 200 with the original JSON-RPC ID:

```json
{"jsonrpc":"2.0","id":1,"error":{"code":-32003,"message":"Insufficient scope","data":{"requiredScope":"drop"}}}
```

Malformed requests/batches return HTTP 400, JSON-RPC `-32600`; malformed tool
parameters return `-32602`. Batches are refused because streamable HTTP accepts a
single message per POST, preventing a batch from bypassing scope checks.
Member names are case-sensitive, and duplicate envelope/parameter members are
refused to prevent parser differences from hiding a tool call.

### Connections for website pages

Both endpoints require `Authorization: Bearer <live Firebase ID token>` from a
verified Google or password account. Browser/session cookies are not accepted.

`GET D/api/agent-grants` → 200:

```json
{"agents":[{"id":"<grant ID>","name":"My agent","domain":"client.example","scope":["orbit","drop"],"connectedAt":"2026-10-09T00:00:00Z","lastUsedAt":null}]}
```

`domain` is empty for dynamic registrations; `lastUsedAt` is null until the first
MCP use, then a timestamp (updated across primaries at most once/minute). Only
the caller's live connections are listed. UID, email, Firebase credentials and
OAuth tokens are never included in this response.

`DELETE D/api/agent-grants/{id}` → 204, empty body. A missing or another user's
grant returns 404. All associated tokens stop authorizing immediately; an
already-running upstream request cannot be recalled.

Drop's **API keys → Connected agents** panel lists the same records and provides
Disconnect. Firebase identity is kept in memory only. After reloading a page with
an existing Drop session, View connections may require Google sign-in again; the
selected UID must match that Drop session. This does not change account ownership
or exchange a browser cookie for a Firebase credential.

### Upload capabilities for agents

`POST D/api/agent/upload-links`, same Firebase bearer, but provider must be
**google.com** like existing Drop publishing. JSON, all fields optional:

```json
{"name":"my-site","password":"at least twelve characters"}
```

Use `name`/`password` only for a new project; for a new version send `projectId`
alone (`{"projectId":"<owned project ID>"}`). Names follow existing lowercase alphanumeric/hyphen rules (1–48 chars);
passwords are at least 12 Unicode characters and at most 1024 UTF-8 bytes. Updates
retain existing privacy. Non-owned targets return 404; invalid input returns 400;
password-provider accounts return 403.

201 JSON:

```json
{"uploadUrl":"https://drop.app.runonflux.io/api/agent/uploads/<opaque-ticket>","method":"PUT","expiresAt":"2026-10-09T00:30:00Z"}
```

`PUT D/api/agent/uploads/{ticket}`, `Content-Type: text/html` or `application/zip`.
The ticket itself is the capability: no Firebase token, session, idempotency key
or custom publish headers are needed. It is UID-bound in replicated state and
expires after 30 minutes. Existing limits apply (default 50 MiB uploaded, 200 MiB
expanded, 5,000 files), including ZIP safety checks, disk admission and worker
limits. A project is owned by that Firebase user and has no anonymous expiry.
Updates publish a new version of the same owned project at its captured revision.
A concurrent project change returns the existing publisher's revision conflict.

200 JSON, with the existing `Project` JSON object:

```json
{"project":{"id":"<ID>","owner":{"kind":"firebase"},"slug":"my-site","initialSuffix":"","digest":"<SHA-256>","bytes":123,"revision":1,"watermarkDisabled":false,"private":false,"status":"active","createdAt":"2026-10-09T00:00:00Z","updatedAt":"2026-10-09T00:00:00Z","expiresAt":null},"path":"/my-site/","claimPath":"/?claim=<ID>","url":"https://drop.app.runonflux.io/my-site/"}
```

A successful activation atomically stores the original result in the ticket.
Subsequent retries return that receipt, even if their body differs or the project
has since changed, without publishing again. Failed validation/upload does not
consume the ticket. A shared six-minute lease serializes uploads across primaries;
a concurrent request returns 409 `upload_in_progress` with `Retry-After: 2`, and
can retry for the stored result. After a process crash, a retry may wait for that
lease to expire. Unknown or expired tickets return readable 404 `not_found`.
Unsupported media types return 415. Publisher failures keep their existing JSON
error shapes (`{"error":"<publisher code>"}`, sometimes with a duplicate path).
The receipt stops being available when the ticket expires.

## Firebase bearer project API

These server-to-server endpoints authenticate **only**
`Authorization: Bearer <Firebase ID token>`. Drop verifies the RS256 signature
against Google's securetoken JWKS, issuer
`https://securetoken.google.com/fluxcore-prod`, audience `fluxcore-prod`, future
expiry, `iat` no later than the current time, verified email, absence of a tenant,
and provider `google.com`. A custom deployment uses its existing
`FIREBASE_PROJECT_ID` instead. A live refreshed token can have an arbitrarily old
`auth_time`; no recent sign-in is required. Browser login's five-minute recency
requirement remains unchanged.

The actor is the Firebase UID, with access only to that account's owned projects,
including projects created through browser sessions, API keys and upload links.
Browser cookies do not grant access to anonymous projects on these endpoints.
Publishing creates an account-owned project with `expiresAt: null`. Requests
do not create or read a Drop session. Cookies, CSRF headers, Origin and fetch
metadata are ignored, and responses set neither cookies nor CORS headers.
These routes are available wherever project publishing is configured, independently
of `DROP_AGENT_AUTH_ENABLED`; no new setting or port is needed.

The existing `drop_...` agent keys remain valid **only** for
`POST /api/agent/projects`. On every other route in this section they return 403
`account_required`. Opaque OAuth access tokens are not Firebase tokens and cannot
be used here; they belong on `/agent/mcp`.

### Requests and successful responses

`D` is the Drop public origin. Every request requires the bearer header above.
Single-project responses use exactly the existing session API's envelope:

```json
{"project":{"id":"<ID>","owner":{"kind":"firebase"},"slug":"my-site","initialSuffix":"","digest":"<SHA-256>","bytes":123,"revision":1,"watermarkDisabled":false,"private":false,"status":"active","createdAt":"2026-10-09T00:00:00Z","updatedAt":"2026-10-09T00:00:00Z","expiresAt":null},"path":"/my-site/","claimPath":"/?claim=<ID>"}
```

UIDs, storage placement and password hashes are never included. Use `D + path`
for the public URL. Each single-project response has `ETag: "<revision>"`.

| Method and path | Request | Success |
| --- | --- | --- |
| `GET D/api/agent/projects[?cursor=<nextCursor>]` | No body. Omit the cursor for the first page. | 200 `{"projects":[<Project>,...],"nextCursor":"<cursor or empty string>"}`; up to 50 projects, in the same order as the session API. |
| `GET D/api/agent/projects/{id}` | No body. | 200 single-project envelope above. |
| `POST D/api/agent/projects[?name=<name>]` | Multipart `files`, a single HTML document, or a ZIP as described below. Optional `Idempotency-Key` and `X-Drop-Password`. | 200 single-project envelope, owned by the UID with no anonymous expiry. |
| `POST D/api/agent/projects/{id}/versions[?name=]` | Same upload bodies; required `If-Match`; optional `Idempotency-Key`. Existing version rules require `name` to be absent or empty and prohibit `X-Drop-Password`; use PATCH/PUT to rename or change privacy. | 200 single-project envelope with the same ID/slug, a new active digest and incremented revision. Existing privacy is retained. |
| `PATCH D/api/agent/projects/{id}` | `Content-Type: application/json`, `{"name":"new-name"}`; required `If-Match`. | 200 single-project envelope with renamed slug and incremented revision (unchanged when the slug already matches). |
| `PUT D/api/agent/projects/{id}/privacy` | `Content-Type: application/json`, `{"private":true,"password":"at least 12 characters"}` or `{"private":false}`; required `If-Match`. | 200 single-project envelope with updated privacy and incremented revision. |
| `DELETE D/api/agent/projects/{id}` | No body; required `If-Match`. | 204, empty body. |

`If-Match` must contain exactly one quoted positive integer revision, for example
`If-Match: "3"`, taken from the project JSON or ETag. Reads and mutations of another
user's project return 404, just like an unknown project.

Upload media types are `text/html`, `application/zip`, or `multipart/form-data`
with one or more `files` parts, using the same filename/path rules as the session
API. Existing upload/expanded-size/file-count limits, ZIP safety, disk admission,
worker limits, account quotas, rate limits and revision checks all apply. Names
follow the existing pattern `^[a-z0-9](?:[a-z0-9-]{0,46}[a-z0-9])?$` (1–48
characters, starting and ending with a letter or digit). If the chosen name is
already occupied, normal publishing slug allocation applies.

For a private initial publish, `X-Drop-Password` is **unpadded base64url of the
UTF-8 password**, not plaintext; passwords require at least 12 characters. PUT
privacy uses the plaintext JSON password over HTTPS. Versions keep privacy and
cannot set the password through an upload header.

`Idempotency-Key` is optional on the two agent upload routes. If supplied, it must
be a single value matching `^[A-Za-z0-9_-]{8,128}$`. Reuse the same key and request
metadata/content for safe retries, including across Firebase token refreshes:
operations are scoped to the UID. A changed request with the same key returns a
conflict. Without a key, each upload starts a new operation and has no retry
deduplication guarantee. The session API's existing key requirement is unchanged.

### Errors

These routes retain the session project API's JSON errors, normally
`{"error":"<code>"}` (not OAuth's `error_description` envelope):

| Status | Codes |
| --- | --- |
| 400 | `invalid_project` (including invalid cursor, name, upload media, JSON or idempotency key), `invalid_password`, `invalid_if_match` |
| 401 | `authentication_required` (missing, malformed, duplicate, expired or invalid Firebase bearer) |
| 403 | `account_required` (verified non-Google provider, or an agent key outside initial publishing) |
| 404 | `project_not_found` (unknown, deleted or not owned) |
| 409 | `project_conflict` (including stale revision or changed idempotent operation), `project_quota`, `duplicate_content` (also includes `path`) |
| 413 | `upload_limit` |
| 428 | `revision_required` |
| 429 | `upload_busy`, `password_busy` (with `Retry-After`) |
| 503 | `storage_unavailable`, `authentication_unavailable` |

Responses have `Cache-Control: no-store`. Bearer expiry is rechecked inside
authoritative metadata transactions, including upload activation. The Firebase
ID token is verified on each HTTP request (JWKS are cached); it is not persisted
or exchanged for a Drop session. Like other Firebase ID-token APIs, disabling
an account or revoking its refresh token does not invalidate an already minted
ID token before expiry. Disconnecting the OAuth grant stops the gateway issuing
further Firebase tokens; it does not prematurely expire one already issued.

## Cookies and CORS

Consent sets only `__Host-drop-agent`: Secure, HttpOnly, Path=/, **no Domain**,
SameSite=Lax, Max-Age=600, expiry ten minutes. Its random value is hashed in each
pending record; multiple pending requests in the same browser can share the
binding cookie. Each request has its own hashed CSRF token. Only same-origin
`POST /oauth/authorize` uses it. Through the website proxy it is host-only on the
website; direct Drop consent has its own host-only cookie. It needs no Domain or
path rewriting. Consent pages cannot be framed and all inline CSS/JS, including
the local Firebase bundle, are CSP-hashed. `connect-src 'self'` follows the serving
origin, Firebase's script/frame/connect sources are retained, and
`Cross-Origin-Opener-Policy: same-origin-allow-popups` permits Google sign-in.
Brand links always point to Drop's absolute origin. Tokens never appear
in page URLs or callback query strings except the short-lived OAuth code.

Discovery, registration, token and revocation support browser MCP clients from
HTTP(S) origins through reflected, credential-free CORS. Discovery permits
GET/OPTIONS; the three public OAuth POST endpoints permit POST/OPTIONS. Consent
has no cross-origin CORS access.

Connections and upload-link APIs reflect only configured website origins plus
Drop's own origin. They allow `Authorization, Content-Type`, methods
GET/POST/DELETE/OPTIONS, `Vary: Origin`, max-age 600; they never send
`Access-Control-Allow-Credentials` and never authenticate cookies. Untrusted,
null, duplicate Origins, and unsupported preflight headers are refused. Use
`credentials: 'omit'` from website pages. The gateway and ticket PUT endpoint are
server/agent-facing and do not expose cross-origin browser CORS. The Firebase
bearer project API likewise sends no CORS headers and ignores all browser
credentials and Origin headers; it is intended for server-side MCP tools.

OPTIONS returns 204 for the two discovery URLs, `/oauth/register`, `/oauth/token`,
`/oauth/revoke`, `/api/agent-grants`, `/api/agent-grants/{id}` and
`/api/agent/upload-links` (403 for a rejected origin/header/method).

## Optional settings

No new setting is required, and there are no new ports. Existing public ingress
on container port 8080 routes to Go's loopback listener. Defaults:

| Setting | Default |
| --- | --- |
| `DROP_AGENT_AUTH_ENABLED` | `true` |
| `DROP_AGENT_AUTHORIZE_URL` | `https://runonflux.com/apps/oauth/authorize` |
| `DROP_AGENT_RESOURCE` | `https://runonflux.com/apps/mcp` |
| `DROP_AGENT_MCP_UPSTREAM` | `https://runonflux.com/apps/mcp` |
| `DROP_AGENT_RESOURCE_METADATA` | `https://runonflux.com/apps/.well-known/oauth-protected-resource` |
| `DROP_AGENT_WEBSITE_ORIGINS` | `https://runonflux.com` (comma-separated HTTPS origins) |

Resource/upstream/metadata overrides must be HTTPS without credentials/fragments;
`DROP_AGENT_AUTHORIZE_URL` must additionally have no query (including an empty
`?`) or fragment. Its path can be arbitrary, and its origin joins Drop's origin
as an allowed consent POST origin. Only discovery's `authorization_endpoint`
changes; issuer, token, registration, revocation and callback `iss` stay on Drop.
Website origins must have no path/query. Reuse `DROP_PUBLIC_ORIGIN`,
`FIREBASE_PROJECT_ID`, `DROP_FIREBASE_WEB_API_KEY` and
`DROP_FIREBASE_WEB_APP_ID`. Production defaults to `fluxcore-prod` and its bundled
public web identifiers. When publishing is disabled, upload-link creation returns 503
`publishing_unavailable` while OAuth and the MCP gateway remain available.
Custom projects without browser configuration leave agent
auth unavailable, preserving existing API-only deployment behavior. Firebase
service-account credentials are neither needed nor accepted. Enable email/password
in Firebase and verify the user's email to use that sign-in method.

## Storage, revocation and security limits

All client records, pending authorizations, grants, hashed code/access/refresh
lookups, owner indexes, IP budgets, upload tickets, receipts and fallback encryption
keys use the **same Raft metadata store** as existing agent keys. These transactions
always request replicated acknowledgement. Project activation and ticket receipt
are atomic. This does not strengthen the previously accepted asynchronous
Syncthing durability guarantee for uploaded files.

Firebase refresh tokens and optional ticket passwords use AES-256-GCM with fresh
random nonces and record-bound associated data. The encryption key uses HKDF
SHA-256 over `DROP_CLUSTER_PASSPHRASE`, info `flux-agent-grants-v1`. Modes without
that secret create a random 256-bit key once in replicated **private** state.
Initialization waits up to one minute for the local coordinator and cluster
election. After an uncertain initialization result it reads the authoritative
key again; it never replaces an existing key.
Protect Raft state/cluster backups as credentials. Changing the passphrase/key
invalidates previously encrypted connections; plan to disconnect/reconnect them.
No token/secret/body/query/path is added to production access logs.

Opaque credentials use 256 bits of randomness; only SHA-256 lookup hashes are
stored. Used code/refresh lookups remain until one day past the grant's absolute
expiry to detect replays. A bounded minute-by-minute scanner removes old records
and erases expired Firebase ciphertext. Owner-index changes are transactional.

Firebase ID tokens are cached per grant in process memory until five minutes
before expiry; the cache is bounded to 1024 entries and contains no refresh
secret. A 15-second replicated lease serializes external Firebase refresh calls
across primaries. Last-use writes are globally bounded to once/minute. Revocation
is checked before cached tokens authorize requests. Firebase password resets,
disables and revocations are detected at the next Firebase refresh or by the
upstream's own verification; Drop cannot cancel requests already forwarded.
A remote Firebase refresh and a Raft commit cannot be one atomic transaction:
if a process fails between them and Firebase rejects the previously saved refresh
token, the grant fails closed and the user reconnects.

Users revoke from Drop's Connected agents panel, the website's connections page,
or `DELETE /api/agent-grants/{id}`. Clients may use RFC 7009 `/oauth/revoke`.
Revoking existing API keys is separate from revoking OAuth connections.

Orbit/Drop scope enforcement applies to tool calls. The website remains responsible
for its own tool-level user authorization and confirmations before destructive
actions. OAuth scope does not grant permission to charge a wallet or circumvent
upstream authorization. Deploy these endpoints behind the existing HTTPS ingress;
keep staging Basic authentication off for a public MCP sign-in deployment because
that optional gate deliberately protects management/upload APIs.

## Verification

```sh
go test -race ./...
go vet ./...
go build ./cmd/...
(cd web && npm ci --ignore-scripts && npm run build)
```

The Go suites use fake securetoken/JWKS/upstream servers, real RSA signatures and
transactional CAS test storage: binding/expiry, provider/UID checks, rotation and
replay revocation, cross-primary use, encrypted state, CORS, scoped streaming and
idempotent owned uploads. Browser consent tests exercise the real embedded page,
CSP, cookies, denial and escaping across Chromium, Firefox and WebKit. An Allow
test serves consent under `/apps/oauth/authorize` on a different fixture origin,
using a mock popup SDK plus signed fixture JWTs/JWKS/securetoken to exercise real
Go consent and grant transactions. They do not require real Google/password
accounts or automated real-user login.

References: [MCP authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization),
[RFC 8252 native callbacks](https://www.rfc-editor.org/rfc/rfc8252),
[Firebase refresh-token exchange](https://firebase.google.com/docs/reference/rest/auth#section-refresh-token).

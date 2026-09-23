# Flux Drop implementation plan

Status: implementation started; not production-ready. This file is the agreed design, delivery checklist, and record of outstanding security gates.

## Interface polish (2026-09-23)

- [x] Hide empty anonymous workspace navigation and show project cards with
      visibility, expiry, size, link actions, and a settings dialog.
- [x] Move claiming out of settings; a card or publish result now signs in when
      necessary and submits the claim directly.
- [x] Make disabled publishing reasons persistent, validate selections, show
      transfer progress/cancellation, and provide a clear publish-again state.
- [x] Tighten mobile layout, add a practical upload guide, favicon, and accessible
      native-dialog close/focus behavior.
- [x] Browser interaction tests across Chromium, Firefox and WebKit, Go embedded
      page tests, Docker startup/restart smoke test, and refreshed staging image.
- [ ] Live Flux/OAuth acceptance of the refreshed interface.
- [ ] Agent credentials and MCP workflow remain a separate product decision;
      browser-session endpoints are not advertised as agent integrations.

The refreshed Docker Hub staging manifest is
`sha256:925e193db1fab2c31724985a839dc6142862be1985711d9e52427a6d719587a7`.

## Flux-native primary and split durability (latest accepted design)

The user accepted loss of recent uploads/content updates during failover, but
accepted the recommendation to keep ownership, privacy/password changes, deletion,
and session revocation synchronously replicated. This supersedes the requirement
below that every metadata write must await Raft consensus. Automatic mode now uses
a local-durable content journal and ordered asynchronous replication. Raft remains
for established membership/elections and security barriers, not per-content-write
acknowledgements. Initial bootstrap trusts a stable, complete Flux discovery view;
that assumption and total-state-loss limits are explicit in FLUX_DEPLOYMENT.md.

Deployment target remains one image/component, one private
`DROP_CLUSTER_PASSPHRASE`, replicated `/data`, and ALL non-replicated persistent
state/configuration/keys under `/var/lib/drop-cluster`. No per-instance commands
or manually supplied node manifests are required by the automatic source runtime.

- [x] Internal per-transaction acknowledgement requirement, defaulting to replicated.
- [x] Reserve/activate (new uploads and content updates) opt into local durability.
- [x] Mixed policy/content changes promote the entire transaction to replicated;
      session/grant/unknown collections and non-content deletions fail safe.
- [x] Legacy manual mode retains stronger acknowledgements; automatic mode uses split durability.
- [x] Primary fencing, persisted election generations, observed revision/history digests.
- [x] Durable local journal with asynchronous content replication and bounded backpressure.
- [x] Synchronous security barriers and committed-view recovery after uncertain outcomes.
- [x] Coordinator-side validation of typed policy changes before local acknowledgement.
- [x] Passphrase-only self-configuration, staged startup and learner admission.
- [x] Consolidate generated manifests/credentials into one node-local volume.
- [x] Real-TLS staggered runtime startup, partitions, old-primary denial, allowed
      content loss, and security changes surviving promotion covered by Go tests.
- [x] Three-node Docker lifecycle uses asynchronous content mode and two volumes per node.
- [x] Final post-change race/static checks and rebuilt-image verification.
- [ ] Live Flux port/persistence/replication and Google OAuth acceptance.

Detailed durability contract and remaining safety requirements:
[docs/DURABILITY.md](docs/DURABILITY.md). Current deployment settings:
[docs/FLUX_DEPLOYMENT.md](docs/FLUX_DEPLOYMENT.md). Local images have been rebuilt;
no registry image has been pushed for this runtime change.

Final local verification (2026-09-23): `go test -race ./...`, `go vet ./...`,
three consecutive real-TLS staggered-start tests, the three-node Docker lifecycle,
and passphrase-only Docker startup/restart all passed. Final local image:
`flux-drop:raft-test`, image ID
`sha256:7ab4dfc26a7e9173a386aa3d839a1fdc40570dc02190b1bd81f0009b0b5e3824`.
Learner promotion checks the committed metadata/configuration prefix rather than
chasing the fresh coordination barrier created by each promotion attempt.
Docker content storage is shared test storage, not a simulation of Flux's
asynchronous file replication; live deployment acceptance remains outstanding.

Classification verification: `go test -race ./...` and `go vet ./...` passed.
Regression coverage includes content opt-in, default/unknown/mixed-operation
promotion, lifecycle operations (including logout), policy changes inside content
transactions, invalid wire durability, and no replay of ambiguous commit outcomes.

## Auth-only Firebase and coordinated metadata migration (implemented baseline)

The user has now superseded the Firestore authority decision below: Firebase is
to be used only for Google sign-in, without service-account credentials in Drop.
Raft, not uptime/IP ranking or Flux discovery, will establish the metadata writer.
Discovery/status endpoints are observation, not permission to change voters.
The source production runtime now selects Raft and rejects Firebase service-account
credentials. Previously published staging images are unchanged. Final rollout
acceptance remains unfinished; shared-private-CA certificate automation is implemented.

- [x] Durable node-local Raft log/snapshots and a deterministic metadata state machine.
- [x] Core quorum-backed reads/writes, authenticated transport and bounded status endpoints.
- [x] Explicit initial membership, catch-up before promotion, serialized membership methods.
- [x] Periodic discovery/status observations; never evict members merely for being unreachable.
- [x] Credential-free public-key Firebase verifier and token-expiry-bounded sessions selected by production entrypoint.
- [x] Private-credential/mTLS membership enrollment/replacement controller and authenticated leader forwarding.
- [x] Raft session/project/reservation/quota/grant adapters, transactional owner indexes and bounded metadata cleanup.
- [x] Operator selected one image/component, deployment-managed replication exclusions, and private cluster enrollment credentials.
- [ ] Verify Flux node-local persistence in live deployment.
- [x] Shared private cluster CA provisioning/renewal with unique node-local keys.
- [x] Combined image supervises the provisioned coordinator with authenticated liveness checks (not quorum readiness).
- [x] Partition, leader loss, minority denial, restart and membership replacement tests for the coordinator.
- [ ] Full runtime/image lifecycle tests before replacing the published image.
- [x] Credential-free three-node Docker lifecycle: publish/update/rename/private access,
      password revocation, leader loss, quorum loss, recovery/full restart, delete/logout.
- [ ] In-place address changes and live deployment acceptance before calling this turnkey Flux deployment.

Raft files and node identities MUST NOT be in Flux-synchronized `/data`.
Uploaded immutable content remains in `/data` and is separate from metadata
consensus. Quorum loss is an availability failure, never permission to bootstrap
another cluster or serve stale private/public policy. Existing Firestore data
must not be silently discarded or merged into a fresh cluster.

Implemented coordinator: `cmd/drop-cluster`; setup, guarantees and remaining
integration are documented in `docs/CLUSTER.md`. Five consecutive real-mTLS/core
race-test runs, the full Go race suite and vet passed. A runtime TLS-config sharing
race was found and fixed before the repeated run. Local Docker regression checks
do not constitute a Raft-backed publishing acceptance test.

The rebuilt local image `flux-drop:staging-test`,
`sha256:f79f635010c23b1198e2d5ad53810f5ab625141d98eb3787cd9802c6871d818a`,
includes `drop-cluster` and passed the existing final-image publishing lifecycle,
restart persistence, metadata-outage readiness and supervisor tests. It has NOT
been pushed. The application entrypoint and its Firestore backend remain unchanged;
the coordinator is currently an independently runnable, tested integration layer.

Deployment constraint found in local Flux source: Syncthing covers the whole
component volume, including secondary mounts. A separate path inside that same
replicated component is insufficient isolation for Raft state or private keys.
The operator superseded the separate-component decision: one image/component,
with replication exclusions configured by the operator during Flux deployment.
The app does not configure those exclusions. Raft state and keys must remain
unsynchronized; restart durability remains a deployment prerequisite.

Historical coordinator packaging (superseded): `docker build --target coordinator` produced
an independently runnable non-root image with an authenticated local health check.
Local `flux-drop-coordinator:test` image
`sha256:533f6e522cb5566821f789f4624a1e851db9ca7ec60971db83eb9d1dbeb0f55c`
built successfully and its CLI smoke test passed with no network, a read-only
filesystem and dropped capabilities. Full Go race tests and vet passed. This is
not an enrollment or Raft-backed publishing acceptance test; no image was pushed.

Current packaging removes that separate target. `drop-init` supervises the
coordinator when `/run/secrets/cluster-node.json` is provisioned, stops all children
on any unexpected exit, and includes coordinator liveness in the image health
check. The app now defaults to Raft; legacy Firestore can only be selected with
isolated development emulators. No existing Firestore data is automatically imported.

Credential-free migration verification (2026-09-23): full Go race suite and vet
passed. The three-node Docker lifecycle passed with no Firebase credentials,
Firestore/Auth emulators or external networking. It covered publishing, ownership
across nodes, deduplication, update/rename, private access/password revocation,
leader loss, total quorum-loss denial, full restart recovery, deletion and logout.
Local image `flux-drop:raft-test` is
`sha256:88161bbc9fa81d87e8567c060d4b07026431d8ae3388927be195ad11c17ceaec`.
No image was pushed. Disposable Docker test containers/volumes were removed.
The Docker fixture uses shared content only to isolate metadata failover testing;
actual Flux asynchronous content replication and live Google OAuth remain unverified.

The user approved a shared private cluster CA, retaining one component. Managed
TLS now provisions unique local keys, renews seven-day leaves hourly within the
48-hour renewal window, reloads certificates on new handshakes, and derives a
domain-separated admission secret. Initial membership remains explicit. Secrets
must be privately delivered, outside replicated storage; compromise of a replica
compromises cluster identity. Root CA rotation remains an operator procedure.

Shared-CA verification: full `go test -race ./...` and `go vet ./...` passed;
cluster race tests passed three consecutive runs. The actual single-component
image passed the three-node Docker lifecycle using shared CA bundles with no
pre-provisioned leaf keys: publishing, cross-node sessions, private access and
revocation, leader/quorum loss, recovery, full restart, deletion and logout.
Local image tag: `flux-drop:raft-test`. No registry tags were pushed by this step.

## Current staging handoff (supersedes historical disabled-runtime notes)

- [x] Orbit public Firebase defaults with environment overrides; backend secrets
      remain runtime-only. Custom projects never inherit unrelated web defaults.
- [x] Default publishing/production/data/user settings; Flux app-name and hostinfo
      origin discovery with validation/retries and explicit custom-origin override.
- [x] Unit coverage for defaults, credential project binding, origin precedence,
      malformed hostinfo, unavailable discovery and startup retries.
- [x] Opt-in Publisher runtime, protected staging management, quota configuration.
- [x] Data-root/credential validation, storage and Firestore readiness, process liveness.
- [x] Root Dockerfile with Go/Nginx supervision, non-root UID, Compose and health checks.
- [x] Final-image lifecycle test with actual Firestore/Auth emulators, restart,
      metadata-outage readiness and supervisor failure/recovery; CI job added.
- [x] Peer external/listen port separation, optional Flux self-IP injection fallback,
      offline unique certificate issuance and deployment instructions.
- [x] Private runtime Firebase credential delivery option outside replicated data.
- [ ] Operator supplies private credentials and configures Firebase permissions and authorized domain.
- [ ] Operator provisions unique peer identities per Flux replica and verifies volume mode.
- [ ] Live Google popup and actual Flux networking/replication acceptance.

See docs/STAGING.md and docs/PEER_SETUP.md. First-stage testing retains the digest
layout with no automatic file retirement/deletion/refunds. Generation migration
and unrestricted production release remain separate work, not prerequisites to
the controlled local/staging lifecycle. No production Firebase resources were configured.

Defaults-enabled image published to `alihmahdavi/flux-drop:staging`:
`sha256:3f2276f371cb675cac9a9684b538a6047e94686842d86769edfd772189a75f59`.
Local image ID: `sha256:68eeceb9e7ff69e98399a3ed73cc69f3028dcfe42fc71269cde34ebbec4766d7`.
Full Go race tests, vet, and final-image emulator lifecycle passed again for this
revision, including default publishing/username and app-name-derived origin.
The first sandbox-restricted integration invocation could not reach the local
container; rerunning with approved Docker/local-network access passed.
Disposable integration containers and their test-only volume were removed.

Earlier baseline staging verification: the image (Go 1.27.1, Nginx 1.30.5) passed the real
entrypoint/Firestore/Auth-emulator lifecycle, restart persistence, dependency
outage readiness and supervisor failure/recovery. Full Go race tests and vet,
production-config Nginx integration, all 69 HTTPS browser cases, and five repeated
multi-instance mTLS fallback lifecycles passed. Local image:
`flux-drop:staging-test`, ID
`sha256:7acd257ce3caa46a30aab1fef93387df61788fd2aff35d02f5c3920688264c60`.
This is local evidence only, not live Google OAuth or Flux replication acceptance.

## 1. Product contract

- Publish a single HTML file, a ZIP, or a folder without an account.
- Serve prebuilt static HTML/CSS/JavaScript and assets. Never build or execute uploaded server code.
- Initial origin: `https://<appname>.app.runonflux.io`. Project URLs: `/<name>-<six-character-initial-digest>/`.
- Generate an editable random name before publishing. Names use lowercase ASCII letters, digits, and hyphens; reserve application routes.
- Keep the initial URL suffix across content updates. Explicit renaming changes the name portion, retains the suffix, and leaves a protected redirect.
- Anonymous projects expire exactly 30 days after first publication. Updates and renames do not extend expiry.
- Claiming assigns the project to a verified Firebase UID and removes expiry. Logged-in publication automatically claims.
- Claimed projects last until removal, subject to abuse handling and storage quotas. Claiming does not reserve a clean, hash-free name.
- Returning anonymous visitors manage projects using a long-lived opaque cookie. Losing that cookie loses anonymous management access.
- Owners can update, rename, delete, set public/private visibility, and change passwords. Private visitors unlock with a password.
- Initial limits: 50 MiB request/upload, 200 MiB expanded content, 5,000 files, 10 active anonymous projects per session. Make configurable and enforce server-side.
- Provide a documented agent API with structured errors, scoped credentials, and idempotency. Never treat an arbitrary Origin header as agent authentication.

## 2. Agreed architecture and authority

React/TypeScript/Vite frontend; Go service; Nginx static delivery; Firebase Authentication (Google); Firestore authoritative metadata; Flux-replicated files for content. No local database process.

The original file-only session idea is superseded by the user's approval of Firestore metadata and sessions. Files are not a multi-writer authority.

Use Drop-specific collections, never Orbit deployment collections. Existing related login code is `/root/orbit-ui/src/utils/firebase.js`; `/root/deploy-with-git` is the example-app repository. Reuse Google login behavior, not Orbit's FluxCore credential exchange. Choose Firebase project via configuration; do not copy credentials from another application.

### Trust boundaries

1. Browser -> public Nginx: TLS terminates at the Flux ingress; enforce the configured external HTTPS origin, never trust arbitrary forwarding headers.
2. Nginx -> Go: loopback/private internal listener, no public access to internal authorization routes.
3. Go -> Firestore/Auth: verified service credentials; least-privilege IAM; bounded timeouts.
4. Replica -> replica: authenticated encrypted transport; separate peer route/listener; bounded one-hop requests.
5. Uploaded data -> filesystem: extraction into a new private staging directory, then immutable publication. No user-controlled filesystem paths outside validated relative paths.

## 3. Browser isolation (release blocker)

All uploaded documents are hostile, including apparently innocuous or obfuscated JavaScript. Do not use source scanning as an isolation mechanism.

- Enforce response-header CSP sandbox without `allow-same-origin`; baseline `sandbox allow-scripts`. Uploaded markup cannot weaken this header.
- Cover HTML, SVG, direct navigations, errors, conditional/range responses, and fallback responses. No executable documents can escape via alternate routes or MIME sniffing.
- Management cookies are host-only, Secure, HttpOnly, SameSite=Lax, with the `__Host-` prefix in production. Cookies are bearer credentials, not proof of a benign request.
- Browser mutations require a synchronizer CSRF token and exact configured Origin. Reject `null`, missing, and untrusted origins for cookie-authenticated mutations. No state-changing GETs. Add Fetch Metadata checks as defense in depth.
- Management APIs never enable cross-origin credentialed reads. Never reflect arbitrary origins or allow credentialed `Origin: null`.
- CSP sandbox creates opaque origins, so localStorage, cookies, service workers, module loading, font loading, and fetch behavior differ. Document these limitations before claiming framework compatibility.
- Public immutable assets may use non-credentialed CORS after testing. Private assets must not be broadly readable by arbitrary opaque origins. Establish a project-scoped asset capability or restricted compatibility design and test it before enabling private module builds.
- Do not weaken sandboxing to fix framework compatibility. If private module support cannot be isolated, explicitly restrict it until separate origins are available.
- Test malicious projects against management endpoints and other public/private projects in Chromium, Firefox, and WebKit. Include SVG, module scripts, workers, windows/iframes, redirects, and forged peer headers.
- Future wildcard origin mode is an explicit configuration, not a Host-header inference. Retain old path URLs under sandboxing.

## 4. Data model

Firestore schema version 1 (namespaced `drop_*`, server-only writes):

| Collection | Key | Purpose |
| --- | --- | --- |
| sessions | SHA-256 of random 256-bit cookie token | CSRF secret, anonymous owner ID, optional UID, auth expiry, idle/absolute expiry, revocation |
| projects | random stable project ID | owner type/id, slug, initial suffix, active full digest, timestamps, expiry, visibility, policy revision, status |
| slugs | name-suffix | unique project mapping or redirect; reserved/tombstoned names |
| digests | full SHA-256 | transactional current-publication references; no private information in unauthorized responses |
| operations | actor + idempotency key | operation fingerprint, state, result; bounded retention |
| quotas | owner ID | transactional active count and reserved bytes |
| grants | hash of unlock token | project ID, policy revision, expiry; revocable password access |
| gc_jobs | random ID | deletion tombstone, retention deadline, cleanup progress |

Use dedicated UID ownership queries rather than a growing array of projects in a session document. All transactions recheck owner, current revision, quota, and expiry. Firestore rules deny browser access; Go enforces application authorization. Admin SDK bypasses rules, so IAM and server logic are critical.

Files:

```text
/data/
  staging/<random-operation>/public/...
  projects/<stable-project-id>/versions/<full-digest>/
    public/...
    manifest.json
  projects/<stable-project-id>/metadata/<revision>/password.json
  hashes/<full-digest>.json
```

The project marker contains the stable `name-suffix` and lives outside `public/`. It is excluded from content hashing. Digest files and password files are never served. Use stable IDs internally so rename does not move an entire replicated tree. Firestore policy revision selects the password metadata revision; missing/mismatched files fail closed.

## 5. Publishing and updating protocol

1. Authenticate anonymous/account/agent actor; enforce quotas, rate limits, concurrency, and request size before extraction.
2. Stream multipart files or spool a bounded ZIP to a private staging directory. Reject unsafe paths, symlinks, special files, encrypted/unsupported archives, ambiguous paths, duplicate names, excessive nesting, and decompression bombs.
3. Normalize a single ZIP wrapper directory. Single uploaded HTML becomes `index.html`; folder/ZIP must contain a root index after normalization. Reject secrets, unsupported file types, and root dotfiles; do not silently publish arbitrary source directories.
4. Compute canonical digest using sorted relative paths, actual byte sizes, and individual SHA-256 digests with unambiguous serialization. ZIP metadata/order and wrapper name do not affect it.
5. Transactionally reserve slug, quota, and operation. The six-character digest is not a uniqueness or ownership proof; check full digest. Handle initial short-hash collisions by changing the name, never by treating content as identical.
6. Persist immutable content and manifest; sync durable local writes. Complete an existing version only after verifying its manifest/content, never trust a directory merely because it exists.
7. Activate version in Firestore using compare-and-swap revision. A successful publish means at least the accepting replica has a complete version; report replication readiness separately.
8. Other replicas verify the manifest and file digests before marking a version locally ready. Atomic rename on one node is not atomic across Flux replication.
9. Updates keep the slug and initial suffix. Competing updates return a revision conflict instead of silently overwriting one another. Old version retention supports in-flight requests and later cleanup.
10. Maintain current-content digest references atomically with activation. Never direct a duplicate uploader to a URL now serving different content. Duplicate content never grants ownership; private duplicates must not reveal project identifiers.

Firestore and filesystem cannot share a transaction: operations are resumable, activation follows durable content, and a reconciler cleans abandoned reservations/staging. All irreversible cleanup uses narrowly validated internal IDs.

## 6. Sessions and claiming

- Generate 32 cryptographically random bytes; cookie contains only the opaque token; persist its digest, never the raw token.
- Create anonymous session on first landing visit through a bounded/rate-limited endpoint; renew long-lived cookie with explicit server expiry. Browser persistence is not guaranteed.
- Verify Firebase ID tokens server-side (issuer, audience, signature, expiry, revocation as appropriate). Require recent authentication for sensitive account changes.
- Rotate session on login/logout and privilege transitions. Long-lived anonymous recognition must not imply indefinite account authorization.
- Login does not automatically claim every old anonymous project without the intended flow; claim verifies current anonymous ownership and unexpired status transactionally.
- A project URL is not a claim credential. Default claim link works only with the owner's cookie; optional cross-device claiming uses expiring, single-use capability tokens.
- A new browser restores claimed projects by verified UID; never trusts UID supplied by the client.
- Account switch/logout must not leave the next visitor with the previous account's project authority.

## 7. Private access and static delivery

Nginx does not map arbitrary public paths directly into `/data`. Go resolves slug and authoritative policy, validates request path, checks expiry/deletion/private grant, and returns an internal Nginx file redirect only for a verified ready version.

Every asset, HEAD, range request, and conditional request goes through authorization before bytes or a 304 are returned. Nginx internal locations cannot be requested publicly. No FastCGI/CGI/interpreters, autoindex, symlinks, unknown MIME types, or arbitrary filesystem roots.

Passwords use bounded Argon2id parameters and unique salts. Rate-limit password verification to avoid memory exhaustion. Password changes increment policy revision and invalidate grants. Owners may view their own private projects through a deliberately scoped flow; management session cookies must not become general private-asset capabilities.

Initial security policy: authoritative check for every request, fail closed on metadata outage, no shared content caches. Optimize only with an explicitly documented revocation bound. Public-to-private changes cannot retract content already downloaded; prevent future delivery after acknowledged policy changes.

## 8. Replica discovery and fallback

- Read `FLUX_APP_NAME`, `REPLICA_PORT`, and stable instance identity from environment.
- Fetch `https://api.runonflux.io/apps/location/<appname>` periodically with timeout, jitter, body bounds, and schema validation.
- Parse IPv4 and IPv6 safely; remove any advertised port; use configured peer port. Allow only validated public unicast addresses from discovery; exclude self, deduplicate, retain last valid set with maximum age.
- When the active version/file is not ready locally, query a bounded number of peers and stream from a ready peer. Do not cache files on fallback.
- Use TLS with authenticated peer identity (mTLS preferred), one-hop marker, timeouts, request cancellation, and a strict method/path allowlist. Strip external peer headers. Do not forward management cookies to peers unnecessarily.
- Peer handlers do not recurse. They validate authoritative project policy or a narrowly scoped, short-lived signed delivery authorization, including project/version/path/policy revision.
- All peers confirm absence -> 404. Unreachable peers prevent deciding -> 503. Do not leak private existence through different unauthorized responses.
- Keep discovery/routing in Go to avoid repeated Nginx reloads. Never make an arbitrary user-supplied URL a proxy destination.

## 9. API outline

```text
GET    /api/config                 public browser configuration
POST   /api/session                establish anonymous session (origin checked)
POST   /api/auth/google            verify token, rotate session
POST   /api/auth/logout            revoke/rotate
GET    /api/projects               authorized project list
POST   /api/projects               multipart publication
GET    /api/projects/{id}          owner metadata
POST   /api/projects/{id}/claim    transactional claim
POST   /api/projects/{id}/versions upload update, revision required
PATCH  /api/projects/{id}          rename/visibility, revision required
PUT    /api/projects/{id}/password change password, revision required
DELETE /api/projects/{id}          tombstone then GC
POST   /api/unlock                 password -> scoped grant
GET    /healthz                    process liveness
GET    /readyz                     ability to handle supported traffic
```

Version API errors, bound bodies, avoid logging secrets, expose request IDs, document idempotency and retry behavior in OpenAPI. Frontend must distinguish local upload completion from publish activation and replica readiness.

## 10. User experience

Landing: “Drop your files. Publish on Flux.” Explain static-only hosting, no account required, expiry, and claiming. Prominent drop zone; accessible file/folder pickers; generated editable name; limits and compatibility help; progress/cancel/retry states.

Success: open/copy project URL, claim action, expiry, management access explanation. Dashboard: owned projects, visibility, expiry, version state, rename/update/delete/password controls. Login uses Google; no unrelated Orbit projects. Agent section includes working curl examples and API reference.

## 11. Operations and deployment

- Multi-stage Docker build; pinned tool/dependency versions and lockfiles; Nginx plus supervised Go with signal forwarding and failure propagation.
- Non-root runtime, read-only root filesystem where possible, writable content and temporary volumes, dropped capabilities, resource limits.
- Firebase configuration and ADC/service account mounted at runtime; no secrets in images or frontend bundles. Public Firebase client configuration is distinct from server credentials.
- Health/readiness, structured logs without tokens/passwords, request metrics, upload/error/storage/peer/Firestore latency metrics.
- Quotas across sessions and actors, per-IP controls at trusted ingress, storage watermarks, abuse reporting and takedown process.
- Scheduled expiry evaluated in serving path as well as worker; claim-versus-expiry is transactional. Firestore TTL is cleanup, not access control.
- Tombstones prevent stale replicas resurrecting deletes. Back up metadata/content; document and test restore. Replication is not backup.
- Verify actual Flux replication behavior, peer port reachability, TLS provisioning, and persistent-volume configuration in staging before launch.

## 12. Delivery checklist

### A — foundations and content integrity (completed)

- [x] Record agreed requirements and security gates.
- [x] Implement bounded ZIP/folder/HTML ingestion and canonical manifests.
- [x] Add attack-focused unit tests and fuzz seed coverage.
- [x] Add Go process/config scaffolding and fail-closed Nginx configuration.
- [x] Establish reproducible local verification commands.

Foundation verification: `go test -race ./...`, `go vet ./...`, and `go build -o bin/drop ./cmd/drop` passed. The installed compiler required execution outside the filesystem sandbox to access its standard library. Project/session repositories and durable activation are now implemented in milestone C. Ingestion currently accepts ASCII paths only and strips at most one wrapper directory.

### B — browser isolation validation (fixture milestone completed)

- [x] Browser test harness with actual Nginx response headers.
- [x] Public React/Vue module fixtures, private module rejection fixture, and compatibility decision.
- [x] Malicious same-origin, cross-project, SVG, worker, and null-origin tests.
- [x] No private-asset CORS bypass in the tested fixture; document supported private build behavior.
- [ ] Repeat these attacks against the final Firestore-backed product and replica fallback before release.

Verification: 39 integration cases passed (13 each in Chromium, Firefox, and WebKit) using Playwright 1.58.2. Both test and production Nginx configurations passed `nginx -t` under Nginx 1.28.0. Public relative-path React/Vue modules were interactive. Private inline scripts worked; private separate-file module loading remained blocked. Test-only sessions and unlock routes are explicitly excluded from the product entrypoint.

Compatibility decision: initial same-domain private support is self-contained HTML, inline styles/scripts, and embedded assets; private separate-file resources remain blocked by CORP/no-CORS. See `docs/STATIC_COMPATIBILITY.md`. Before enabling privacy in the product, explain and enforce this restriction; general private multi-file apps require project-scoped asset capability design or isolated origins. Never fix compatibility by allowing credentialed null-origin CORS.

CI configuration now runs Go checks and the browser harness. It has been written but has not been executed on GitHub. The local test stack was torn down after validation; fixtures and instructions remain reproducible.

### C — authoritative publishing and identity (in progress)

- [x] Firestore session repository and emulator transaction tests; scoped server-only session rules.
- [x] Backend session/CSRF/Google token verification and atomic rotation.
- [x] Google popup UI, session exchange and explicit owner claim controls.
- [ ] Live Google OAuth end-to-end and authorized-domain/CSP staging validation.
- [x] Firestore project repository, index configuration, and project transaction tests.
- [x] Backend publication transactions, project-count quotas, digest reservations, idempotency, and expired-reservation recovery.
- [x] Claim/update/delete backend lifecycle and owner-filtered listing.
- [ ] Production publisher wiring and scheduled reservation recovery.
- [x] Conservative per-instance upload byte/inode admission and concurrent reservations.
- [x] Conservative persistent owner byte budgets, retry-safe reservations and claim transfers.
- [ ] Quota configuration/usage UX, reviewed schema backfill, old-version GC, and disk-full/replication stress validation.
- [ ] Rename/redirect and private-password lifecycle.
- [ ] Nginx internal delivery authorization on every request.

Session implementation: 256-bit opaque host-only cookies, hashed Firestore keys, separate anonymous identities and 12-hour account authority, recent Google login verification, revocation checks, CSRF/origin gates, atomic login/logout rotation, account-switch isolation, shared session creation budgets, rotation chain budgets, and production emulator guards. SDKs now require Go 1.25. See `docs/SESSIONS.md` for configuration and limitations. Session APIs are enabled only when Firebase is configured; publishing and project serving stay disabled.

Session verification uses ordinary Go security tests plus an isolated localhost Firestore emulator. Google identities in transaction tests use a test verifier; the real Firebase SDK is separately tested against malformed/unsigned tokens. These tests do not constitute a live Google OAuth round-trip. CI now includes a Firestore transaction job.

Recorded verification: Go race-enabled suite, vet, and binary build passed; all four Firestore integration tests passed, including concurrent bootstrap quotas and concurrent session rotation. No production Firebase credentials or records were used. The temporary emulator is removed after the run.

Before production, configure Firestore TTL for session/budget cleanup and add ingress limits for invalid authentication attempts. Anonymous cookies currently have a fixed one-year lifetime between rotations, not sliding renewal on every visit. Logout starts a new anonymous identity; the future UI must warn users to claim projects they want to keep and must sign out of the Firebase browser SDK.

Publishing backend verification: eight project Firestore integration tests and one combined session/project HTTP integration test passed, along with the full Go race suite, vet, and build. Coverage includes actual manifest/file verification, durable installation, deduplication, short-hash collisions, stable URLs/expiry on updates, ownership and claiming, concurrent quotas/updates, revoked-session activation denial, failed-install recovery, lost activation responses, and expired reservations. Multipart path preservation/traversal and request-size handling have unit coverage.

The command entrypoint deliberately does not inject a Publisher yet: public links must not be presented as live before authorized Nginx delivery exists. See `docs/PUBLISHING.md` for the implemented handler contract and remaining gates. Recovery is callable/tested but not scheduled. Repository count defaults are 10 anonymous and 100 claimed projects, configurable in the repository; production environment configuration and disk budgets remain outstanding. No real Firebase indexes/rules/data were deployed. The test-only emulator is removed after verification.

### D — replication and maintenance

Unlock UI increment: added the built-in responsive password page with accessible labels/status feedback, strict hash-based script/style CSP, existing session bootstrap/CSRF submission, password clearing on errors, and fixed validated project navigation. Canonical private root navigations without grants now redirect to the form; aliases/assets/non-navigation remain denied. The renderer never queries project existence. Firestore HTTP tests passed the navigation change. Browser tests exercise the real page with mocked API responses; a combined real-grant/Firestore browser test and invalid-session recovery UX remain release gates.

Verification: all 45 browser tests passed across Chromium, Firefox and WebKit (including six unlock-page cases); the Firestore HTTP integration, Go race suite and vet also passed. The temporary browser and emulator containers/networks were removed after testing. Publishing remains disabled in the product entrypoint.

Browser-unlock increment: integrated `POST /api/unlock` with session/Origin/CSRF checks, bounded JSON, shared hash workers, and Secure/HttpOnly/SameSite=Strict __Host project-grant cookies (tokens never in JSON/URLs). Local private delivery now validates session-bound grants on each request and only permits top-level root HTML navigation, using a separate internal Nginx alias without asset CORS. Private assets, frames/fetches and peer fallback remain denied. Password rotation/session mismatch invalidate access. The Firestore-backed API test passed; unlock form UI, full cross-browser validation and production publisher wiring remain unfinished.

Verification: extended Firestore HTTP integration passed cookie flags, session isolation, null-Origin denial, private fetch/asset denial and password-rotation revocation. Production-config Nginx integration passed private GET/HEAD bytes and sandbox/CORP/no-CORS headers using a test authorization callback; real grants were exercised separately in the Firestore test. Full Go race suite and vet passed. Temporary emulator/container networks were removed.

Unlock/grant increment: added shared transactional per-minute password-attempt budgets (global/session/project), charged before hash work, including global/session charges for unknown/public project lookups. Successful verification issues a hashed opaque grant bound to browser session, project and exact policy for at most one hour. Issuance rechecks password/policy after hashing; validation rechecks session, current project policy, expiry and deletion. No unlock HTTP/cookie flow or private delivery is enabled yet. `drop_unlock_budgets` is included in browser-deny Firestore rules; TTL deployment and ingress abuse limits remain operational gates.

Verification: both grant lifecycle and concurrent shared-budget emulator tests passed, including wrong password, session/project binding, hashed-token storage, expiry, password rotation, stale verification rejection, session revocation, window reset and unknown-project budget charging. Full Go race suite and vet passed. Temporary emulator cleanup followed; no production resources were changed.

Privacy-policy increment: added the owner-only revision/Origin/CSRF-checked `PUT /api/projects/{id}/privacy` API and service that durably prepares password records before transactional activation. Transactions recheck session/owner/live state and pending operations, increment project/policy revisions, and clear password references on public transitions. A separate password-record revision survives rename policy changes. Password metadata is never serialized in project responses. Hash work is bounded per router. Visitor grants, shared attempt limits, orphan metadata GC and private serving are still outstanding; publishing remains disabled.

Verification: the Firestore privacy lifecycle integration test passed ownership denial, durable password binding/verification, stale revision rejection, rename compatibility, rotation, concurrent policy changes and clearing private authority. Full Go race tests and vet passed. The temporary emulator was removed after verification; no production metadata was changed.

Password foundation increment: added fixed-parameter Argon2id hashing with unique salts, constant-time verification, input bounds and two-worker capacity per shared Hasher. Immutable password records are stored outside public trees with rooted filesystem writes, no-overwrite installation, durability syncs, and digest/project/policy binding. The planned revision directory is refined to a content-addressed record directory to avoid concurrent policy preparations overwriting one another. Hash/storage security tests passed. No password API, unlock grant or private delivery is enabled yet; see `docs/PASSWORDS.md` for remaining integration gates.

Rename increment: added owner/session/revision-checked transactional renames and the bounded JSON PATCH API. The six-character initial suffix, active content and expiry are preserved. Prior URLs stay reserved as direct project aliases and public delivery redirects them to the current name after current policy checks. A safety cap limits additional permanent aliases to 100 per project. The on-disk publication marker remains original; Firestore is authoritative for current name. Emulator tests passed rename/update/prior-name reuse/deletion and ownership/stale-revision cases; delivery tests cover path/query-preserving redirects and private alias non-disclosure. Private-password management is still pending.

Multi-instance lifecycle increment: two actual runtime listeners now have an isolated test with separate data roots/certificate files, real mTLS, recorded-shape discovery, and loopback-only transport mappings through private test hooks. The public delivery handler falls back to the remote runtime with no local cache. Coverage includes missing/partially replicated versions, recovery, stale ingress metadata versus revoked remote visibility/policy, and shutdown/closed listeners. Five consecutive race-enabled lifecycle runs passed. Metadata remains a test resolver; Flux/Firestore-backed staging is still outstanding.

Runtime wiring increment: opt-in `DROP_PEERS_ENABLED=true` validates app/instance/self-IP/port/data/credential settings, verifies local certificate chain and both EKUs, rejects secrets inside replicated data, starts the bounded mTLS listener and discovery, and injects fallback into HTTP dependencies. Shutdown closes workers/listeners before Firestore; listener errors terminate the process. Publishing remains disabled. Configuration tests, Go race tests and vet passed. Multi-instance runtime lifecycle tests, certificate provisioning/rotation and staging remain outstanding; internal/external peer ports currently must match. See `.env.example` and `docs/REPLICAS.md`.

Fallback increment: public delivery now accepts an optional discovery-selected fallback dependency for locally unavailable versions. The implementation bounds concurrency, peer attempts and total time; strips browser credentials; validates response version/policy/length; generates security headers locally; and streams without disk caching or retries after output begins. Real mTLS transfer tests and failure-selection tests passed with the full Go race suite. Full responses only are supported during fallback; validator/range unification and runtime certificate/listener/discovery injection remain pending. Publishing is still disabled in the entrypoint.

Local peer delivery increment: added an mTLS-wrapped, local-only handler with no fallback dependency. It checks authoritative public/live status, exact active digest and policy revision, verifies local version completeness and the selected open file, and handles GET/HEAD/ranges/conditional requests only after those checks. Eight concurrent requests are allowed. Tests over real local mTLS cover successful transfers, stale versions/policies, private/deleted/expired denial, metadata outages and corruption. Public delivery remains Nginx-backed; only the separate peer transport uses Go file streaming. Runtime listener wiring and discovery-selected fallback remain outstanding.

Peer transport increment: implemented mutually authenticated TLS 1.3 configurations with application/instance URI identities, verified shared server DNS SAN, and bounded no-redirect/no-environment-proxy HTTP clients. Added a TLS-only one-hop boundary rejecting browser credentials, request bodies, unsafe paths and management routes. Public Nginx strips the hop marker. Local handshake/boundary tests exercise valid and rejected identities. This remains a library boundary, not working replica fallback: certificate provisioning/rotation, listener wiring, local peer delivery and response streaming are still required. See `docs/REPLICAS.md`.

Production-config delivery verification: `tests/delivery` now runs the unchanged production Nginx configuration plus the real Go delivery handler in a non-root, read-only container with private tmpfs content. The suite passed actual GET/HEAD delivery, range 206, conditional 304, sandbox/no-store/public CORS headers, external internal-alias denial, metadata/encoded-path denial, and GET/HEAD denial after private/expired/deleted/reserved states or metadata outage. Corrupted content returns 503. This exposed and fixed default Nginx temp directories outside writable `/tmp`. The suite uses an in-memory metadata resolver, not live Firestore or Google auth, and is now a separate CI job. Full Go race tests and vet also passed. Peer fallback and product-entrypoint enablement remain outstanding.

Maintenance increment: `DROP_MAINTENANCE_ENABLED=true` enables a cancellable worker in the Go entrypoint (requires Firebase). Each minute it attempts bounded recovery and expiry batches with a 45-second deadline. Expiry rechecks anonymous ownership, active status, deadline and pending updates transactionally, releases quota/digest reservations once, and retains slug tombstones. Claimed projects are protected. Deploy the new `drop_projects(status, expiresAt)` index before enabling. This is metadata cleanup only; replicated-file GC and backlog monitoring remain release gates.

Verification: all ten project Firestore emulator integration tests passed, including repeated expiry cleanup and claimed-project protection; full Go race tests and vet passed. The pinned Nginx container validated production configuration syntax. Actual production-config request-path integration is still outstanding. Temporary test containers/networks were removed after verification.

Delivery increment: added a public-project delivery handler that resolves authoritative metadata on every GET/HEAD, rejects private/inactive/expired projects, verifies the complete installed version, and emits an internal Nginx redirect only for manifest-listed files. Nginx now has an internal-only public-content alias with symlink denial, opaque-origin sandbox headers, and noncredentialed public asset CORS. Handler tests cover traversal/alternate encoding, metadata outages, corrupted content, conditional/range requests passing through authorization, and private/expiry/deletion denial.

This increment is not a production enablement: the command entrypoint still does not inject a Publisher. Full version verification on every request is intentionally conservative and needs a bounded readiness strategy before release. Go and Nginx must share read permissions for installed files. Production-config Nginx integration, runtime wiring, scheduled recovery/expiry, and peer fallback remain outstanding; handler tests alone do not validate actual Nginx range/304 behavior.

- [x] Discovery parsing with recorded schema fixtures.
- [x] Connect discovery worker/environment configuration to authenticated peer runtime.
- [ ] Peer TLS/authentication, one-hop streaming fallback, readiness checks.
- [ ] Multi-instance integration tests with delayed/incomplete replication.
- [ ] Expiry, tombstones, abandoned-operation recovery, version GC.

### E — product and release

Management UI increment: project cards now expose rename, private/password/public transitions, file/folder content replacement and typed-confirmation deletion. Mutations send CSRF and the displayed project revision, serialize management actions, disable stale controls until refresh, clear password inputs, and preserve update idempotency keys for unchanged retries. Public visibility and replacement use explicit confirmation. Google sign-in/claiming, recovery UX and full backend-connected browser validation remain outstanding.

Verification: all 63 browser tests passed across Chromium, Firefox and WebKit, including nine new management cases with intercepted APIs. The full Go race suite and vet passed. The temporary browser fixture containers/network were removed. These UI tests complement rather than replace the real Firestore transaction/API suites.

Landing/publishing UI increment: added a responsive built-in page with file/folder selection, editable generated names, size/count feedback, multipart upload/CSRF/idempotent retry handling, duplicate/success links and paginated session-owned project listing. The current shell follows the embedded strict-CSP unlock-page implementation; the planned full React/TypeScript dashboard is not implemented. Folder drag currently directs users to the folder picker. Config now reports publishing enabled only when a Publisher is injected; the production entrypoint still leaves it disabled. Google login/claim and full management controls are explicitly unfinished. See `docs/FRONTEND.md`.

Verification: all 54 browser tests passed across Chromium, Firefox and WebKit, including disabled publishing, stable retry keys, folder path preservation and duplicate-content links. UI APIs are mocked in these browser tests. Go race tests/vet and the landing renderer test passed; the desktop layout was visually inspected. Temporary browser containers/networks were removed after verification.

Discovery increment: added a bounded HTTPS discovery client and jittered cancellable refresh worker, with public-IP-only validation, advertised-port replacement, self exclusion, deduplication, record expiry, and a five-minute maximum snapshot age. Tests use two recorded explorer API records plus synthetic adversarial cases. See `docs/REPLICAS.md`. This is a library milestone: discovery is not yet started by the command entrypoint and no content is proxied. Authenticated TLS identity/provisioning and one-hop fallback remain outstanding.

- [ ] Frontend and API documentation, agent credentials/idempotency.
- [ ] Docker image/Compose fixtures, CI, vulnerability/dependency checks.
- [ ] Rate limits, observability, abuse controls, backup/restore runbook.
- [ ] End-to-end tests across browsers, staging deployment, release review.

## 13. Verification requirements

Content tests: traversal (including backslashes/encoded paths), duplicate/case-conflicting paths, file/directory collisions, symlinks, archive bombs, missing index, forbidden extensions/secrets, identical content in different ZIP order/wrappers, content/path changes affecting digest, failed-upload cleanup.

Service tests: unauthorized reads/mutations, CSRF/origin rejection, Google token errors, ownership isolation, session rotation, concurrent claims/updates/quotas/dedup, private duplicate disclosure, password revocation, expiry races, identifier collisions.

Integration: Nginx traversal and internal-route denial; private HEAD/range/304; fallback policy headers; spoofed peer headers; peer loops; peer outages; half-replicated versions; stale deletion/password/visibility metadata; process restarts and disk-full behavior.

Release acceptance: every checked feature has passing relevant tests; all remaining limitations explicitly documented. A scaffold or passing ingestion tests alone must not be presented as a production-ready service.

## References

Generation-storage foundation: added validated independent generation paths and
an immutable generation installer sharing the existing durable install logic.
Different generations can hold identical digests without sharing installed files;
same-generation retries verify existing content and reject rebinding/corruption.
The stable project marker remains private. This is not a layout migration:
Publisher, Nginx/local delivery and peers still use the legacy digest layout.
Operation metadata, generation-bound serving/peer protocol and retirement scoping
must switch together before new-layout publication is enabled. No files moved
or deleted; see docs/RECLAMATION.md for the integration checklist.
Verification: full Go race suite and vet passed; generation tests passed ten
consecutive race-enabled runs; the production-config Nginx delivery integration
passed, confirming the shared installer refactor preserves legacy delivery.

Publication-retirement increment: internal RetireVersion atomically records an
immutable project/digest fence after checking current revision, historical
operation provenance and retention eligibility. Reserve/retry/Activate reject any
existing record, including malformed records. The audit reports recorded targets
as held. No scheduler, public endpoint, deletion or refund is enabled. Reuse of a
retired digest in the same project is intentionally blocked until generation-aware
storage exists. This fences metadata activation only, not filesystem I/O; see
docs/RECLAMATION.md for mixed-writer deployment and physical cleanup prerequisites.
Verification: full Go race suite and vet passed, and all Firestore-backed project,
HTTP and session suites passed with the retirement regression coverage. No
production Firestore rules or records were changed.
The retirement-versus-restoration race also passed five consecutive race-enabled
emulator runs.

Reclamation audit increment: added a bounded, read-only Firestore snapshot audit
for explicit project/version identifiers. Active, pending, reserved, expired but
not finalized, missing and incomplete metadata are held. Other versions are only
reported as candidates, never authorized for deletion. Restoring an old digest
demonstrates why a snapshot cannot fence cleanup. docs/RECLAMATION.md records the
remaining generation fencing, Nginx read coordination, durable replica membership,
replication deletion validation and idempotent refund requirements. No deletion
worker or refunds are enabled; no files or production records were removed.
Verification: full Go race suite and vet passed; emulator-backed retention,
retained-byte, concurrent-byte, claim-transfer, legacy-quota and expiry regressions
passed. This verifies audit decisions, not physical replica reclamation.

Persistent quota increment: publication reservations charge payload bytes with a
1 MiB per-operation floor, using default 1 GiB anonymous / 10 GiB account budgets.
Retries do not double-charge; deletion, expiry and aborted installs retain charges
until verified replica cleanup. Claim transfers all project charges atomically.
Legacy quota documents fail closed; no production data or migration was changed.
See docs/STORAGE.md for schema rollout requirements and remaining abuse/GC gates.
Verification: Go race tests and vet passed. Emulator-backed HTTP/session suites
passed; the project suite passed on rerun after an existing concurrent unlock
budget test initially hit an emulator transaction lock timeout. New byte-quota
tests cover concurrent limits, retry accounting, abort/deletion/expiry retention,
claim transfers and legacy schema rejection. No replicated files were deleted.

Storage admission increment: both creation and version replacement check available
filesystem bytes/inodes before reading bodies. Shared local reservations include
raw/expanded limits and path/spool overhead, retain a 1 GiB safety margin, and are
released after the publication attempt. Probe failures fail closed with retryable
503. This is not a hard filesystem quota and cannot prevent concurrent replica
writes from consuming space. See docs/STORAGE.md for thresholds and remaining
runtime/GC gates; product publishing remains disabled.
Verification: full Go race suite and vet passed, as did the Firestore-backed
HTTP, project transaction and session suites. No production Firebase was used.

Google UI increment: optional validated Firebase Web App configuration enables a
locally bundled, pinned Google popup client. Firebase credentials use memory-only
persistence and are cleared after token extraction; server exchange rotates the
cookie and in-page CSRF token. Owned anonymous projects expose an explicit claim
action, and post-upload claim links only open owned controls. Logout warns before
discarding anonymous ownership. Auth operations block publishing and management
mutations. CI rebuilds the bundle for reproducibility. Browser coverage uses a mock
token provider, not real Google OAuth; production publishing and live-auth staging
remain release gates. Configuration and scope are documented in docs/FRONTEND.md.
Verification: Go race tests and vet passed; all 69 browser cases passed across
Chromium, Firefox and WebKit, including claim CSRF rotation, logout confirmation
and cancelled-popup preservation. Live OAuth was not exercised.

- https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Content-Security-Policy/sandbox
- https://firebase.google.com/docs/admin/setup
- https://firebase.google.com/docs/auth/admin/verify-id-tokens
- https://firebase.google.com/docs/firestore/manage-data/transactions
- https://nginx.org/en/docs/http/ngx_http_core_module.html#internal
- https://docs.syncthing.net/users/syncing.html

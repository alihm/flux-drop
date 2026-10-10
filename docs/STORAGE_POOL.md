# Primary and secondary storage apps

This deployment uses a fresh primary app and fresh secondary apps. The same
image runs either role, selected with `DROP_ROLE`. There is no data migration or
mixed-protocol rollout. Leaving the role unset preserves the existing standalone
runtime; storage-pool settings without a role are rejected.

## Guarantees and boundaries

- The primary owns sessions, user authentication, ownership, access policy,
  projects, quotas, project-to-app placement, and app-wide storage accounting.
- A complete project stays on one secondary app, including future versions.
  A full/unavailable assigned app rejects updates. New projects can use others.
- A secondary independently verifies the canonical manifest and file bytes,
  installs immutable content, and fsyncs it before acknowledging an upload.
  Only **one secondary instance** must acknowledge. Flux/Syncthing propagates the
  replicated volume asynchronously; Drop does not wait for a second copy.
- Losing that instance before replication can lose a recent upload. Complete
  loss of a secondary app is not covered by another app's storage.
- App placement and retained storage allocation are committed with the primary's
  metadata reservation through Raft. This accounting barrier does not wait for
  secondary replication. Public activation retains the existing local-durable
  acknowledgement policy; security changes retain replicated acknowledgement.
- A directory arriving through Syncthing is not automatically a ready version.
  Reads verify the manifest and actual content. Failed or partial replicas are
  skipped; unavailable content returns 503, never an older version.
- Each read checks locally replicated primary serving metadata/access policy before using cached
  bytes, and rechecks policy, version and any private grant after a remote fetch.
  An immediate verified cache hit uses the initial per-request authorization.
  Private hosting retains the existing self-contained HTML restriction.
- Operations are idempotent. Lost responses may have succeeded: retry the same
  original upload/key, not a different operation. Pending operations expire after
  15 minutes. Retries never move a project or charge the same operation twice.
- Delete/expiry update authoritative metadata; serving observes deletion when
  replicated locally, with no maximum delay, and checks known expiry immediately.
  Retained app and
  owner byte charges are not refunded on abort/delete/expiry. Physical retirement,
  replica-wide garbage collection, and automatic app draining/migration are not
  exposed by this protocol. Files might exist on offline/returning replicas;
  inventory is not deletion authorization. This preserves the existing
  conservative reclamation policy in RECLAMATION.md.

The primary is still an app with several instances, not one designated machine.
Its existing coordinator elects and fences the metadata leader. A secondary has
no user database, Firebase login, metadata coordinator, or public project API;
its operation receipts/reservations and random instance identity are node-local.

## Primary deployment

Deploy three primary instances using the same private cluster passphrase and
storage configuration. Private settings (example values are placeholders):

```dotenv
DROP_ROLE=primary
DROP_CLUSTER_PASSPHRASE=<strong-random-private-cluster-secret>
DROP_STORAGE_APPS_JSON=[{"appName":"dropstoragea","port":35447,"keyId":"v1","apiKey":"<random-private-secondary-a-key>"},{"appName":"dropstorageb","port":36447,"keyId":"v1","apiKey":"<random-private-secondary-b-key>"}]
DROP_STORAGE_ADMIN_KEY=<separate-random-private-admin-key>
DROP_CACHE_BYTES=268435456
```

`DROP_STORAGE_ADMIN_KEY` is optional; without it the storage administration routes
return 404. `DROP_CACHE_BYTES` defaults to 256 MiB; zero disables retention and
uses verified temporary spooling to proxy content. No storage secret is a browser
credential. Use at least 32 bytes of strong random secret for each key, e.g. a
separate `openssl rand -hex 32` result. Do not publish keys or the cluster
passphrase in public Flux app specifications. Deliver them via private/Enterprise
configuration. Never reuse a key for browser Basic access.

Flux supplies the app identity; existing local hostinfo discovery is the fallback.
Public origin and Firebase browser settings keep their existing defaults.

Container Data:

```text
r:/data|ml:state:/var/lib/drop-cluster
```

The primary's `/data` holds small replicated deployment/password records; full
project versions are remote. Raft state, staging, and cache stay in the private
child of the nonreplicated state mount. The default primary requires its existing
upload staging headroom, plus the configured cache and bounded concurrent
200 MiB response spools. Provision several GiB of node-local writable space.
Both directories must be actual dedicated persistent mounts. See
[FLUX_DEPLOYMENT.md](FLUX_DEPLOYMENT.md) for the existing primary coordinator's
bootstrap, persistence, trust, and recovery assumptions.

Map port 8080 through the public HTTPS ingress. Map 34445 and 34446 directly to
identical external port numbers on every primary instance for coordination and
metadata. The primary storage role disables the old same-app content fallback
listener on 34444. Do not expose loopback Go port 8081.

Primary `/readyz` requires local storage, metadata authority, and a currently
writable secondary offer. `/healthz` remains liveness. Reads can still use a
verified cache under local serving metadata during a secondary outage.

## Secondary deployment

Deploy at least two instances per secondary, normally three. All instances in
one secondary share that secondary's primary app name, key set, port, and capacity.
Secondary A:

```dotenv
DROP_ROLE=secondary
DROP_PRIMARY_APP_NAME=dropprimary
DROP_STORAGE_PORT=35447
DROP_STORAGE_CAPACITY_BYTES=107374182400
DROP_STORAGE_API_KEYS_JSON={"v1":"<same-secondary-a-key-held-by-primary>"}
```

Secondary B uses its own key and `DROP_STORAGE_PORT=36447`. The configured port
is both the container listener and the direct external port; map those numbers
identically. Choose different ports for apps that might share a Flux host.
The image declares 34447 as its default private storage port; specifying a
custom port changes the listener and requires the corresponding Flux mapping.
Port 8080 exposes only liveness/readiness through Nginx. Storage APIs are never
served through the public Nginx listener.

Do not supply `DROP_CLUSTER_PASSPHRASE` to a secondary. Secondary startup refuses
primary state/content. Persist and replicate the volumes as follows:

```text
r:/data|ml:state:/var/lib/drop-cluster
```

- `/data`: Flux/Syncthing replicates immutable versions and the storage app identity.
- `/var/lib/drop-cluster`: node-local storage identity, operation receipts,
  reservations, and staging under `private/`; exclude this mount from Syncthing.

`DROP_STORAGE_CAPACITY_BYTES` is required and must match the usable storage budget
for **one instance** specified in Flux. It is not the sum of replica disks.
Drop measures filesystem availability too, but a backing filesystem's `statfs`
may include host space beyond the app's provisioned budget; the explicit budget
prevents counting that as app capacity. Leave at least 1 GiB headroom. The accepted
configuration range is greater than 1 GiB and at most 1 TiB.

Staging is on a separate local volume and needs the upload/expansion allowance,
filesystem overhead, and 1 GiB free-space headroom. Four simultaneous maximum
uploads can require several GiB. Admission can reject work conservatively even
with nominal free capacity. A filesystem read-only error, byte/inode exhaustion,
failed probe, or unavailable discovery must not result in successful installation.
The supervisor restores private-state permissions after Flux permission sweeps,
with the host trust limitations described in FLUX_DEPLOYMENT.md.

## Capacity, discovery, and transport

The primary discovers each configured secondary with the existing bounded Flux
app-location client. Each secondary independently discovers the primary app's
addresses. Discovery refreshes every 45–75 seconds; a failed refresh preserves the
last validated list for at most five minutes or the advertised address expiry,
whichever comes first. A valid empty response removes all eligible addresses.
Stale/uninitialized discovery cannot authorize a request or a new remote dial.

Storage requests require BOTH the primary source IP and the correct key ID/API
key. Forwarding headers do not establish identity. Direct requests must preserve
the Flux node's actual source IP; verify NAT/egress behavior on Flux before live
acceptance. The application rejects a proxy's unrelated source address.

TLS 1.3 authenticates the secondary before the primary sends its API key. Each
high-entropy shared key derives a deterministic Ed25519 storage TLS identity,
scoped to the exact secondary app, primary app, key ID, and protocol domain.
The primary trusts that certificate through a private trust store; it does not
skip certificate verification, rely on public node-IP certificates, send secrets
in URLs, follow redirects, use ambient proxies, or forward browser credentials.
The storage TLS identity cannot authenticate to the primary's cluster CA.
The v1 storage certificate has fixed validity from 2025 through January 2045;
protocol/certificate rotation before that deadline is an explicit deployment task.

Health/capacity probes run every 12–18 seconds, with at most eight concurrent
probes per primary instance and five-second probe deadlines. Node observations
expire after 45 seconds. Capacity includes actual free bytes/inodes, pending
local reservations, writability, configured budget, and instance ID. At least
one healthy writable instance allows an upload; replication lag/failures must
still be monitored through Flux/Syncthing. Drop does not claim to measure the
Syncthing backlog or prove replica completeness from capacity alone.

A primary metadata transaction charges the selected app from the verified
manifest: rounded file blocks, actual directory entries, manifest and small parent
metadata allowance. Upload scratch reservations remain temporary. Legacy charges
(expanded bytes plus 8 MiB and worst-case directory/block overhead) are audited
against operation history before being reduced; unverified usage remains reserved.
It also tracks retained inode reservations. These charges persist across primary
failover and prevent competing primary instances from allocating the same
logical capacity independently. Budgets are per-app, never multiplied by the
number of replicas. Lower observed budget limits cannot later be increased
implicitly by a different instance's report. A larger filesystem block size
raises the retained overhead estimate for prior allocations too. Owner quotas remain separate.
See [storage accounting and optional replica-verified cleanup](STORAGE_ACCOUNTING.txt)
for dashboard meanings, the backward-writer fence, generation protocol and rollout.

Observed physical free space and local reservations add another admission gate.
External filesystem consumers and Syncthing can change space after probing;
checks cannot reserve disk extents or eliminate every ENOSPC failure. Errors
leave publication pending/unavailable rather than reporting success.

For key rotation, add a new key ID to every secondary instance first (up to four
active keys), update the primary's configured key ID/key, then remove the old key
after the primary rollout and in-flight requests have settled. Restart applies
configuration; changing the primary app identity on existing secondary volumes
is rejected. Deploy additions consistently across primary instances. Setting
`"drain":true` on an app stops new uploads and updates there, while existing reads
continue. Keep apps with retained projects in the configuration; removing an app
makes cache misses unavailable and does not erase its placement or accounting.

## Added endpoints

Every `/internal/storage/v1/` route below exists ONLY on the secondary's direct
TLS storage port, and requires primary IP authorization plus these headers:

```http
Authorization: Bearer <this-secondary-key>
X-Drop-Key-ID: v1
```

| Method | Path | Contract |
|---|---|---|
| GET | `/internal/storage/v1/status` | App/instance identity, protocol, discovery freshness, local durability policy |
| GET | `/internal/storage/v1/capacity` | Configured budget, filesystem free bytes/inodes, block size, writability, local reservations, headroom |
| PUT | `/internal/storage/v1/operations/{operationId}` | Reserve exact project/digest/size/file-count/initial-slug with expiry; same identity/input is retryable |
| GET | `/internal/storage/v1/operations/{operationId}` | Node-local operation state; installed trees are rechecked |
| DELETE | `/internal/storage/v1/operations/{operationId}` | Cancel unfinished idle reservation; stored/busy work returns 409; never deletes installed files |
| PUT | `/internal/storage/v1/operations/{operationId}/content` | Bounded ZIP transfer of the canonical staged tree; independent validation, verification, installation and fsync |
| POST | `/internal/storage/v1/operations/{operationId}/commit` | Confirm exact locally installed content and fsync before primary activation |
| GET | `/internal/storage/v1/versions/{projectId}/{digest}/manifest` | Canonical manifest after complete version verification |
| GET, HEAD | `/internal/storage/v1/versions/{projectId}/{digest}/files/{path...}` | Verified exact file; supports normal conditional/range serving |
| GET | `/internal/storage/v1/inventory?cursor=...&limit=...` | Observational directory inventory, at most 100 entries/page; cursor is `projectId/digest` |

Reservation JSON:

```json
{"projectId":"<32-lowercase-hex>","digest":"<64-lowercase-hex>","slug":"original-name-abcdef","bytes":1234,"files":1,"expiresAt":"2026-10-08T20:00:00Z"}
```

The operation ID is 64 lowercase hex characters. Input must not set `state`.
The expiry must be in the future and at most 16 minutes away, allowing a small
clock margin around the primary's 15-minute operation deadline. Accurate clocks
are required. Transfer is `application/zip`, bounded at 216 MiB to accommodate
uncompressed transport of the already validated 200 MiB expanded tree. Browser
upload limits remain unchanged at 50 MiB input / 200 MiB expanded / 5,000 files.

Receipts expire and are bounded to 8,192 per node. Stored bytes survive receipt
expiry. Inventory does not verify or certify an entry's bytes, so partially
replicated versions can appear there. It bounds each directory listing at
100,000 entries and returns unavailable beyond that bound rather than performing
an unbounded scan. Pagination is observational during concurrent replication;
repeat reconciliation scans rather than treating one scan as a snapshot.

On the primary, these GET routes require a separate administrator bearer key;
ordinary user/session credentials do not grant access:

| Method | Path | Contract |
|---|---|---|
| GET | `/api/storage/apps` | Per-app/per-instance health and capacity, discovery freshness, drain state, primary retained allocations |
| GET | `/api/storage/operations/{operationId}` | Primary operation state, project/digest, assigned app, expiry; excludes user credentials |

Existing browser/agent publishing and project URLs remain unchanged. No physical
retirement endpoint was added: safe replica-wide deletion/refunds require the
separate reclamation lifecycle, not a caller-supplied list of files.

## Verification

```sh
GOCACHE=/tmp/drop-go-cache go test -race ./...
GOCACHE=/tmp/drop-go-cache go vet ./...
docker build -t flux-drop:storage-pool .
node tests/storagepool/smoke.mjs
```

The Go integration tests open real local TLS listeners with private test-only
in-memory discovery sources; production has no environment bypass for discovery,
IP checks, TLS authentication, or content verification. Tests exercise key/IP
boundaries and rotation, lost-response retry, mismatched content, cancellation,
restart identity, partial replication/read failover, cache integrity/access
revocation/eviction, concurrent accounting, sticky placement, and retained charges.
The final-image smoke test also exercises both production roles in disposable,
network-isolated containers, including public-route isolation, supervision,
fail-closed readiness, persistent secondary identity and private-state ownership.
Its temporary containers and volumes are removed after the test. Local tests
simulate content arriving between directories; they do not run a real Flux
Syncthing deployment. Live acceptance must verify private env delivery,
node-local volume exclusion/persistence, HTTPS/direct port mappings, NAT/egress
source IPs, discovery changes, actual Syncthing propagation, and node loss before
and after propagation. No production deployment is performed by these checks.

### Bounded serving fetches

Primary reads share verified downloads by app/project/digest/path and parsed
manifests by app/project/digest (256 entries, approximately 64 MiB). Each reader
opens its own descriptor. A disconnected initiating reader does not cancel its
peers; shared work has a two-minute deadline and is canceled at pool shutdown.
Fetching has a separate budget from the ten-second metadata lookup.

Optional primary settings: `DROP_STORAGE_FETCH_CONCURRENCY=32` (1–128),
`DROP_STORAGE_FETCH_QUEUE=1024` (0–8192), `DROP_STORAGE_FETCH_BYTES=838860800`
(1 MiB–1 TiB), `DROP_CACHE_ENTRIES=4096` (1–65536). Queue zero admits no extra
waiting readers. Distinct fetches wait at most 20 seconds for a slot. Duplicate
readers also consume bounded admission; slow readers release the fetch slot but
retain their file reference. Admission failures return 503 and Retry-After.

The byte limit covers block-rounded active spools, including uncached responses
when `DROP_CACHE_BYTES=0`. Disk admission subtracts outstanding reservations from
statfs available bytes and leaves 1 GiB/1024 inode headroom; this deliberately
counts partially materialized reservations conservatively. Retained cache bytes
remain subject to the separate cache limit. Manifest/hash failures try another
healthy replica, never another content digest. Read ordering uses shuffled,
least-loaded replicas; body close releases transport load. Install ordering still
uses free space. TLS pinning, discovery and no-proxy/no-redirect rules are unchanged.

### Cache ownership and nginx

The retained cache uses an unpinned-entry LRU. `DROP_CACHE_BYTES` (default 256 MiB)
includes block-rounded files and one conservative directory-entry block per file.
Retired files count against the same byte and entry budgets. Downloads transfer
ownership atomically from reserved spools into retained entries; uncached files
remain charged until their final reader removes them. Failed unlink attempts
remain charged and are retried before future spool admission and on shutdown.
Resource-pressure fetches wait at most 20 seconds with bounded checks, then 503.

The supervised image hands verified retained files to separate internal nginx
public/private aliases; original paths determine MIME, not `cached-*` names.
Branded HTML and temporary responses stay in Go. Each handoff protects the name
for at least 60 seconds after its latest handoff. This assumes nginx opens within
60 seconds; arbitrary process suspension cannot be solved by a timer. Grace and
failed-unlink bytes cannot be evicted early to admit more content: admission falls
back to bounded spooling. Restart cleanup assumes the supervisor has stopped the
previous nginx process. Private responses retain no-store and same-origin CORP;
public responses retain their validators/cache/CORS policy. No public cache paths.

For several-GiB caches, budget additional 800 MiB active spools by default,
1 GiB filesystem headroom, upload staging, preview storage and free inodes; do not
size the cache to the volume's full advertised capacity. Retired bytes are within
the cache budget. Entry count defaults to 4096 independently of byte capacity.
Secondary descriptor hash caching is bounded to 4096 stable identities containing
digest/hash, device/inode, size, nanosecond mtime and ctime. Version and descriptor
verification slots default to eight; `DROP_VERSION_VERIFY_CONCURRENCY` accepts
1–128, applied at startup in either role and standalone primary runtime.

Protected storage status includes local serving counters/gauges even when
allocation metadata is unavailable (`partial: true`, `metadataAvailable: false`).
The loopback-only profiling listener also exposes `/debug/serving`; never publish
port 6060. Metrics have no path/user/token labels. Slow-request logs are bounded
by a process-wide rate limiter and omit query strings and upload ticket paths.

## Approximate page-view analytics

Primary/standalone Raft deployments collect page views by default. Secondaries do
not collect analytics. `DROP_ANALYTICS_ENABLED=false` disables collection and the
Manage page's **Page views** tab. No new required settings, keys or ports exist.
`GET /api/config` includes `analyticsEnabled`.

A page view is a successful authorized HTML GET (200 or 304), including nested
HTML pages and unlocked private pages. HEAD, partial/range requests, redirects,
failures, assets, API calls, thumbnails and the internal screenshot renderer do
not count. Browser requests with a non-document/non-iframe `Sec-Fetch-Dest` and
explicit prefetch/prerender requests are excluded. Clients without fetch metadata,
including bots/curl, count. No IPs, cookies, query strings, paths, emails, UIDs or
visitor identifiers enter analytics. These are page requests, not unique people.
Browser/upstream cache hits that bypass Drop are invisible. Go records successful
nginx handoffs before nginx finishes transferring bytes; a later nginx/client
failure can therefore overcount. No JavaScript is injected into deployed sites.

Each primary increments sharded in-memory counters. An independent worker captures
cumulative hourly snapshots approximately every ten seconds (small random jitter
avoids synchronized retries), fsyncs a private outbox, and submits via the existing
membership-checked mTLS coordinator RPC to the leader. Separate clients keep
analytics HTTP pools/routing independent of metadata traffic. Analytics performs
no Raft transactions and never participates in site authorization or readiness.
Worker failure, full queues, quotas and missing leaders must not block serving.

Snapshots have a fresh random producer ID per process boot, stable node ID, UTC
day/shard, monotonic sequence, timestamp and project-ID -> 24 hourly counters.
Within one producer/day/shard, only the highest sequence is used; different
producers add. Thus retries, lost ACKs, restarts and delayed lower generations
cannot double-count. Cumulative snapshots replace the initially suggested reset
and mutable-leader-total design: concurrent old/new leaders never overwrite a
shared authoritative total. Two immutable generations are retained per source.
The leader aggregates these snapshots for owner queries, with a bounded five-
second result cache; every query still performs management ownership checks.

The coordinator writes immutable JSON files under
`/data/analytics/YYYY-MM-DD/<hex-shard>/`, named
`<node>.<producer>.<sequence>.json`, on the primary's existing replicated `/data`
volume. Files contain aggregated counts only. Grouping projects into 16 shards
avoids creating one file per project per flush. Private unacknowledged snapshots
are under `/var/lib/drop-cluster/private/analytics-outbox` and survive local
restart, without being copied between node identities. Acknowledgement requires
local file and directory fsync, not a second synced primary. Complete node loss
before volume replication can lose acknowledged data. Replication/leader changes
can temporarily reduce visible counts; analytics are explicitly approximate.

Retention is **180 UTC days including today** (a precise bounded approximation
of six months). Old snapshots are excluded immediately by query validation and
removed by an hourly background sweep, up to 4096 old files per pass. Expired
retry batches are acknowledged and discarded. Project renames/version updates
retain history by stable project ID. Deleted projects cannot be queried; a new
owner after transfer can see the project's retained history.

Fixed bounds protect delivery: 1024 projects per shard/day, 16 shards, two days
of active counters; 256 private pending files/32 MiB plus one bounded atomic-write
temporary; snapshot size at most 768 KiB. A worker tick has a five-second budget,
32 send attempts, fair shard ordering, and a two-second best-effort final flush.
Old uncaptured counters and views beyond admission bounds may be dropped.
The archive admits at most 1 GiB/65536 tracked allocated files, checks 1 GiB disk
and 1024 inode headroom before writes, and limits writes to 1 MiB/s. External
volume replication can temporarily exceed the tracked quota until the next
accounting sweep; filesystem quotas remain an operator concern. Reads are limited
to two concurrent queries and 4 MiB/s, 64 MiB/4096 snapshot files per query, with
at most 128 producers per day/shard and 512 directory entries. Heavy or unavailable
analytics return retryable 503; they do not change site response behavior. These
bounds can make analytics incomplete under sustained pressure. Loopback-only
`/debug/serving` includes collection/drop/error/pending counters. The archive
folder must remain outside nginx aliases and public API paths.

### Owner endpoints

| Method | Endpoint | Authentication |
| --- | --- | --- |
| GET | `/api/projects/{id}/analytics` | Existing browser owner session; read-only, no CSRF |
| GET | `/api/agent/projects/{id}/analytics` | Live Google Firebase ID bearer; no session/cookies/CSRF/CORS; `drop_` keys receive 403 |

Both accept optional `from` and `to` RFC3339 timestamps (inclusive/exclusive,
aligned to UTC hours), and `interval=hour` or `day`. Daily ranges start at UTC
midnight; the final day can be partial. Defaults: the last seven UTC dates,
including today, with daily buckets. The maximum range is the retained 180 days;
`to` cannot exceed tomorrow midnight UTC. Empty hours/days return zero buckets.
The response is the same on both endpoints, with no-store caching:

```json
{"projectId":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","timezone":"UTC","approximate":true,"interval":"day","from":"2026-10-10T00:00:00Z","to":"2026-10-11T00:00:00Z","updatedAt":"2026-10-10T13:00:00Z","pageViews":42,"buckets":[{"start":"2026-10-10T00:00:00Z","pageViews":42}]}
```

`updatedAt` is the latest included producer snapshot, not a completeness promise;
it is null before any snapshot for the project. Errors: existing authentication
401/provider or key-policy 403, non-owner/deleted project 404, invalid range 400
`invalid_request`, or 503 `analytics_unavailable` with `Retry-After: 5`. An unknown
project is never exposed through analytics. The UI fetches only when its tab is
opened; the hourly chart/table and 7/30/180-day ranges are rendered locally using
CSP-hashed assets without third-party analytics services.

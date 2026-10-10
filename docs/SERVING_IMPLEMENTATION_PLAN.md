# Serving performance implementation and verification plan

Status: workstreams 0–4 implemented on `serving-plan-implementation`; verification passed;
measurement results and practical limits are recorded below. Optional workstream 5 is omitted.

Reviewed checkout: `ea095fb`. Pulled baseline: `15f8309`. Find code by function/type
when paths or details have moved.

## Objective and execution rules

Make primary/secondary file serving tolerate bursts without immediate download-slot rejection, unnecessary leader traffic, or unbounded resource consumption. Preserve content integrity and the explicitly accepted serving consistency model below.

- Inspect repository instructions, the current branch, and uncommitted changes first. Preserve unrelated work.
- Implement on a new branch off `main`. Carry this plan into that branch without discarding local changes. Record the baseline SHA; do not assume the reviewed checkout and `main` are identical.
- Commit each workstream separately after its relevant tests pass. Do not push or deploy.
- Establish baseline measurements before implementation. Run comparable measurements after implementation.
- Use existing tests and documentation. This document is the authorized implementation handoff; do not create additional documentation files unless needed and approved.
- Do not add production bypasses for TLS, discovery, authorization, or integrity to make tests easier.
- Match surrounding Go style. Add comments for security and non-obvious ownership/concurrency rules.
- Do not mark a workstream verified unless its tests actually ran and passed. Report unavailable checks and deviations explicitly.

## Accepted consistency model

The user explicitly accepts eventual metadata consistency for page/file serving, thumbnails, and Explore. Replication may take hours or a day. There is no maximum replication-delay guarantee.

Existing locally replicated records determine the response, including existing denials. Contact the leader only when a required metadata record is absent locally. Do not implement `DROP_METADATA_STALENESS`, a `LastContact()` gate, or freshness proofs for serving reads.

| Condition | Behavior |
| --- | --- |
| Required local records exist and allow access | Serve without contacting the leader |
| Required local records exist and deny access | Deny locally without contacting the leader |
| A required record is absent locally | Attempt a bounded leader lookup for the complete logical lookup |
| Leader confirms absence or denial | Return 404/deny |
| Leader lookup fails | Return 503 with `Retry-After` |
| Missing-record confirmation admission is exhausted | Fail closed with 404/deny; count this separately |
| Local coordinator is unavailable or its state cannot be read | Return 503 with `Retry-After`; do not silently turn every outage into leader traffic |

An old public policy may keep allowing access after a revoke. An old denial or pending record may delay new access. An old active digest may select an older version. Those delays are accepted. Evaluate locally known expiry timestamps using the current time.

Never substitute an arbitrary older content version after a fetch/integrity failure. Serving a digest selected by accepted local metadata is distinct from falling back to another digest.

All writes, security changes, owner/management APIs, unlock-grant creation, and their existing durability remain unchanged. Local serving reads must be explicit at call sites, not a global backend switch.

## Invariants

- Authorize every request against the selected serving metadata before body bytes or an authorized 304 are sent. Never cache authorization decisions with files or manifests.
- Preserve private hosting restrictions: self-contained HTML and only `index.html` reachable through private project delivery.
- Verify every downloaded byte against the manifest's expected SHA-256 before serving or retaining it. Parse manifests with `content.ParseManifest(data, digest)`.
- Preserve storage TLS pinning, peer identity/key/IP checks, no redirects, no environment proxies, and no forwarding of browser credentials.
- Read-only local transactions must use one coherent snapshot. Eventual consistency permits old state, not incompatible records assembled across snapshots.
- Expected overload and dependency failures return 503 with `Retry-After`, never 500. Missing-record confirmation-cap denial is the explicit fail-closed exception above. Disconnects need cleanup, not a guaranteed response to a vanished client.
- Bound active work, queues, metadata caches, temporary bytes, retired bytes, and detached background work. Every acquired resource has a defined release path.

## Code and existing verification entry points

- Delivery and private access: `internal/httpserver/delivery.go`, `unlock.go`, `server.go`, `preview.go`.
- Primary pool/cache/config/admin: `internal/storagepool/{pool,cache,config,admin,dashboard}.go`.
- Secondary serving: `internal/storagepool/server.go`.
- Integrity cache: `internal/content/version_cache.go`.
- Metadata transactions and RPCs: `internal/metadata/transaction.go`, `internal/cluster/{transaction_reads,rpc,node,async,state}.go`.
- Repositories and serving dependencies: `internal/project/raft_rules.go`, `internal/session/raft.go`, `internal/preview/{service,explore_cache}.go`.
- Runtime: `cmd/drop`, `cmd/drop-cluster`, `cmd/drop-init`, `deploy/nginx.conf`, `deploy/snippets/project-headers.conf`, `Dockerfile`.
- Performance: `scripts/serving-load.mjs`, `internal/httpserver/serving_benchmark_test.go`, `serving_performance_test.go`, `docs/PERFORMANCE.txt`.
- CI: `.github/workflows/test.yml`. Existing Docker suites include `tests/raft`, `automatic`, `storagepool`, `delivery`, `staging`, and `browser`; inspect their actual commands before running.
- Existing docs to update at the appropriate workstream: `.env.example`, `docs/STORAGE_POOL.md`, `CLUSTER.md`, `PERFORMANCE.txt`, `PREVIEWS.md`, and other existing pages whose guarantees change.

## Workstream 0: baseline and instrumentation

1. Record SHA, Go/image versions, CPU quota/affinity, memory limit, disk type/space, topology, secondary latency, cache limits, file count, and file-size distribution.
2. Prepare an isolated primary/secondary/Raft fixture. Include nginx in end-to-end measurements. Never reset production caches or load-test production without separate authorization.
3. Extend the load driver so the exact same driver can measure baseline and implementation. Use explicit fixture paths for cold runs; current HTML discovery warms content before measurement.
4. Preserve existing per-status counts; add elapsed time, total and successful rps, actual transport-error latency, bytes, and configurable request timeout. The current 15-second timeout is shorter than the proposed queue wait.
5. Measure cold burst, warm mixed assets, slow clients, and sustained overload. Use fixed resource limits and repeated runs. Separate end-to-end results from handler microbenchmarks.
6. Count metadata RPCs by method/destination at a test or internal instrumentation boundary. Report leader RPCs per request. Do not expose a public diagnostic API.
7. Save the baseline method and measured numbers in `docs/PERFORMANCE.txt`. If execution is unavailable, record that limitation; do not invent measurements.

Commit baseline/instrumentation independently. Reuse its harness on the baseline SHA with an isolated worktree/container where necessary.

## Workstream 1: bounded primary fetching

### Admission and disk ownership

- Add validated `DROP_STORAGE_FETCH_CONCURRENCY` (default 32) and `DROP_STORAGE_FETCH_QUEUE` (default 1024). Suggested ranges: concurrency 1–128; queue 0–8192, where zero means no queued downloads. Reject malformed/overflowing values.
- Prefer immediate admission when a slot is available. Otherwise wait in a bounded queue for at most 20 seconds, or until caller cancellation. Queue-full and wait-timeout responses are 503 with `Retry-After` and distinct counters.
- Admission applies to distinct downloads; also bound duplicate callers waiting on shared work so singleflight does not create an unbounded bypass.
- Replace the hard-coded four-spool free-space assumption in `download`. Atomically reserve expected file bytes before creating a spool. Account for filesystem allocation overhead, free inodes, upload staging, and existing headroom.
- Bound temporary/active spool bytes independently of download slots and retained cache bytes. Choose and document a finite default based on the existing four-by-200-MiB allowance; expose a validated byte setting if operational sizing requires it. This must work when `DROP_CACHE_BYTES=0`.
- Account for a downloaded file exactly once as its ownership changes: reserved download, retained cache, temporary response, or retired file. Release physical-byte accounting only when the file is actually removed, or transfer it atomically to its next owner.
- When resource admission cannot proceed, use the bounded admission policy; do not spin or allow concurrent `statfs` checks to oversubscribe disk.

### Shared downloads and manifests

- Singleflight key: storage app, project ID, active digest, normalized file path. Recheck the cache inside the flight before downloading.
- Shared fetching uses `context.WithoutCancel` plus its own bounded timeout and pool-shutdown cancellation. An initiating caller's disconnect must not abort other callers.
- Each caller opens its own descriptor. Do not share an `*os.File` or rely on `dup` for independent offsets.
- Register ownership before publishing a flight result. Release it on cancellation, failed opens, response completion, and shutdown. Remove temporary spools only when no caller still needs to open/read them.
- Release the fetch slot once verification and result ownership are established, before client streaming. Keep disk ownership until cleanup.
- Add a parsed-manifest LRU keyed by app/project/digest, bounded by entry count and approximate memory (target 64 MiB including the path map and parsed objects). Coalesce fetches, parse/verify before insertion, and index files by path.
- Define bounded handling for a manifest larger than the cache budget. Do not retain it indefinitely or repeatedly fetch it for callers already sharing the same work.

### Deadlines and transport

- Keep metadata resolution/private authorization under their own 10-second bounds. Derive fetching from the original request, not that metadata context; use a two-minute overall fetch budget.
- Ensure local integrity verification also has an explicit budget after removing the blanket delivery context. Preserve request instrumentation/context values.
- Size connection pools for configured fetching rather than retaining the hard-coded four-connection transport cap.
- Balance reads across healthy discovered secondary instances using random selection or power-of-two choices. Track in-flight load through response-body close, not just receipt of headers.
- Bound each attempt within the total fetch budget; fail over on unavailable, partial, or corrupt replicas. Keep install/write ordering by free space unchanged.

### Required verification

- 200 concurrent cold requests over 50 files: all 200 OK, one manifest fetch per version, one download per file in a healthy no-retry fixture with sufficient resources.
- Slow clients no longer hold fetch slots; temporary bytes remain bounded.
- Queue full, timeout, cancellation, zero-queue configuration, and resource pressure behave as specified; no expected 500s.
- Fetch longer than the metadata deadline succeeds. Inject internal clocks/timeouts so tests remain fast.
- Initiating request disconnects while other callers succeed; all abandoned spools and references are cleaned up.
- Independent descriptors deliver correct complete content concurrently, including ranges.
- Manifest hit/eviction/size limits/digest mismatch and secondary failover tests.
- Env validation, TLS/no-proxy/no-redirect checks, and existing storage access/integrity tests pass.

## Workstream 2: remove redundant serving checks

1. Remove the second project authorization lookup from immediate cache hits. Initial delivery authorization still runs for every request, including HEAD and conditional requests.
2. Keep per-caller post-fetch authorization after shared remote work. Never share a successful authorization result through singleflight.
3. Introduce an explicit pure snapshot-read transaction facility or equivalent scoped optimization. Skip final `Check` only for read-only, coherent stamped snapshots selected through that facility. Legacy fenced reads retain `Check`; writes retain CAS and existing durability.
4. Preserve generic management transaction semantics. Do not globally weaken `metadata.Tx.finish` just to optimize serving.
5. Preserve the existing preview final-validation behavior for now. If local preview serving later needs a new validation path, implement it explicitly in Workstream 3 rather than silently deleting the existing contract.
6. Update documentation about immediate cache hits versus post-fetch checks.

Required tests: one project resolution on public cache hits; post-fetch recheck for every caller; no final Check RPC for opted-in pure snapshot reads; legacy Check retained; management/write behavior and existing preview policy-race tests retained.

## Workstream 3: explicit local serving metadata

### Coordinator and client

- Add an authenticated local snapshot RPC answered by any usable coordinator from its own state. No replication-age check; an isolated follower remains eligible to serve its coherent applied state.
- A leader uses its existing async view when its existing authority rules allow it. Do not weaken leader authority or speculative-write fencing. A failed/unusable leader view returns unavailable.
- Preserve TLS, caller membership, destination identity, cluster-ID validation, request limits, and strict decoding.
- The client always targets `c.local` for this RPC and never follows hints. Do not record a successful local follower response as the cached leader.
- Include a snapshot identity sufficient to detect changed snapshots across dependent reads. Restart/restore/term changes cannot accidentally validate an incompatible stamp. Retry the complete logical lookup on conflict, with existing bounded retry discipline.
- Keep existing leader-forwarded RPCs unchanged for management and writes. An older coordinator rejecting the new RPC should fail serving clearly rather than silently enabling a global consistency switch; document rollout behavior.

### Serving repository boundary

- Provide explicit serving methods/adapters, for example `ResolveForServing`, serving session reads, and serving grant validation. Keep `Resolve`, `GetOwned`, ordinary session reads, and write repositories unchanged for management callers.
- Distinguish absent records from existing denial. Existing `Resolve` maps missing, non-live, and pending conditions into `ErrNotFound`; that result alone cannot drive missing-only confirmation.
- For an absent required record, rerun the whole logical lookup against the leader. Do not stitch leader records into a partially read follower snapshot.
- For existing local policy/digest mismatches in post-fetch validation, deny or return retryable unavailability as appropriate. Do not contact the leader solely because of the mismatch.
- Wire serving adapters only into delivery, its post-fetch callback, private serving access/session checks, thumbnail reads, and Explore. Audit private thumbnail owner checks so management authorization is not globally rerouted.
- Target at most two loopback snapshot reads for ordinary public project resolution. Private access may need more calls unless related reads are batched; report actual counts.

### Missing-record confirmation protection

- Coalesce equivalent missing-record leader lookups and bound active confirmations, distinct keys, and waiting callers. Start with a cap around 64 per primary; document that this is per-primary and measure aggregate leader load across replicas.
- Use a bounded negative LRU with approximately one-second TTL for confirmed missing slugs. This delay is accepted; immediate publish visibility is best effort.
- Do not negative-cache transport errors. Do not cache authorization results across sessions. Include relevant identity/project/token digests in private lookup keys, and never log secrets.
- Ordinary in-flight sharing is allowed under the accepted eventual model; no request-arrival freshness fence is required.
- Missing-record cap exhaustion fails closed; existing local positive answers bypass confirmation admission entirely.
- Local coordinator unavailability returns 503. Do not add unrestricted fallback traffic that bypasses confirmation limits.

### Preview and documentation

- Preserve explicit local post-I/O snapshot validation for thumbnail preparation where required by the existing contract. That check need not consult the leader when records exist.
- Existing Explore server/browser TTLs are compatible with unbounded eventual consistency. Document them separately; do not promise immediate gallery changes or five-second revoke propagation.
- Update existing cluster, storage, preview, session/password, and performance guarantees wherever necessary. Explain that server authorization uses local replicated metadata and browser caches can persist independently.

### Required verification

- Existing follower records serve with zero leader RPCs, including while disconnected from the leader and after long simulated replication delay.
- Existing denial/pending/expired/deleted records do not trigger confirmations; known expiry is checked against current time.
- Missing slug/project/session/grant triggers leader lookup; coherent leader success is used. A new publish or grant works when confirmation is admitted and no applicable negative cache exists.
- Negative TTL and confirmation-cap denial behave exactly as documented. Leader failure returns 503 with `Retry-After`.
- No mixed snapshots; conflict/restart/restore tests; concurrent policy changes do not corrupt authorization assembly.
- Local RPC cannot poison leader routing. Management APIs and all writes still use their original leader paths.
- Existing privacy/grant/isolation tests pass or are deliberately updated only for the accepted serving consistency change, with the reason recorded.

## Workstream 4: cache, nginx, secondary, and runtime hardening

### LRU and nginx delivery

- Replace oldest-entry map scans with `container/list` and efficient selection of unpinned evictable entries. Make entry count configurable (default 4096).
- Add separate internal public/private nginx aliases restricted to the cache root. Preserve `disable_symlinks on`, project security headers, CORS/CORP split, and direct-access rejection.
- Only hand off verified retained files, after authorization. Branded HTML stays in Go. Temporary responses stay in Go unless an equally safe ownership protocol is explicitly implemented and tested.
- Set Content-Type using the original requested path/verified bytes before handoff; extensionless `cached-*` names must not determine MIME type.
- Preserve validators, no-store for private files, HEAD, ranges, and conditional requests. Do not expose filesystem paths or accept browser-supplied internal redirect targets.
- Retain evicted files for at least 60 seconds after the latest handoff. Define the practical nginx-open timing assumption; a timer alone is not proof of open completion.
- Bound retired bytes and entries. If safe retention/admission is impossible, stop new cache admission and use bounded spooling rather than deleting a handed-off file early. Account for failed unlink attempts and restart/close cleanup.
- Keep the 256 MiB cache default. Document several-GiB sizing with spool, retirement, inode, and upload headroom included.

### Secondary verification

- Bound and coalesce verified-hash caching. Include expected manifest hash/digest plus open-descriptor device, inode, size, nanosecond mtime, and ctime in identity.
- Hash the descriptor that is served; compare fstat before/after hashing and only cache successful stable results. Preserve immutable-content assumptions and primary-side verification.
- Make VersionCache verification slots configurable, default 8. Apply configuration deliberately to the relevant runtime, without breaking standalone serving or constructors used in tests.

### Runtime

- Set nginx worker connections to 8192 and upstream keepalive near 64, subject to measured descriptor capacity. Validate inherited soft/hard nofile limits; do not assume a non-root process can raise them.
- Derive worker count from effective cgroup CPU quota and affinity. Generate config under `/tmp` and launch nginx with `-c`; keep the image read-only/non-root compatible. Test both roles and healthcheck invocation.
- If `GOMEMLIMIT` is explicitly set, preserve it. Otherwise discover effective cgroup v2 memory limits with v1 fallback, including applicable ancestor limits; handle unlimited/unreadable cases safely.
- Budget memory across `drop` and `drop-cluster` together, leaving headroom for nginx, browser/preview children, page cache, and non-Go allocations. Do not assign each Go process 70% of the same container limit. Record the selected split and method; a soft Go memory limit is not an OOM guarantee.
- Go 1.25 already uses container CPU bandwidth for default GOMAXPROCS unless overridden. Avoid redundant CPU tuning.

### Observability

- Expose local counters/gauges on the existing protected admin storage status surface: queue-full, wait-timeout, disk-admission failure, confirmation attempts/cap denials, local coordinator errors, retries, cache hits/misses, shared downloads, and active/temp/retired bytes.
- Add equivalent bounded internal visibility where serving exists without the storage role. Do not add public debug endpoints.
- Return available local counters even when leader-backed admin details fail; include a clear partial/unavailable indication.
- Rate-limit logs and bound metric label cardinality. Never log query strings, credentials, grant tokens, or API keys.

Required tests: real nginx MIME/header/HEAD/range/304/private/branded behavior; alias traversal and direct access; LRU pins, retirement timing/budget, restart cleanup; hash-cache replacement and mutation; env validation; non-root startup with read-only root; CPU/memory/nofile detection and explicit overrides; protected status output under leader failure.

## Workstream 5: optional measured prefetch

- Implement after foreground serving is verified. Enable by default only if controlled measurements show benefit without foreground regressions; report the decision.
- Trigger at most one bounded attempt per app/project/digest within a bounded tracking structure. Set an explicit retry/backoff policy rather than retriggering on every miss.
- Use four low-priority background slots and a per-version cap around 64 MiB. Bound global queued versions and bytes; skip under foreground backlog or disk/cache/retirement pressure.
- Use the same verification, disk ownership, and singleflight machinery. A foreground caller can join/take priority over prefetch work.
- Account for transport contention: separate slots alone do not prevent competition for a 32-connection pool, disk, or secondary capacity.
- Cancel on shutdown; do not log URLs with secrets or retain browser credentials in detached work.

Required tests: per-version/global bounds, shutdown, duplicate suppression, foreground joining/priority, pressure suppression, integrity mismatch, and stable foreground latency under background load. If omitted because measurements do not justify it, report that explicitly rather than marking it implemented.

## Final verification and measurements

- Use Linux for the full suite: storagepool imports `golang.org/x/sys/unix`. Inspect current Docker/CI versions rather than assuming a tag is available.
- Run `go test -race ./...`, `go vet ./...`, production binary builds, and CI's repeated cluster race tests. Alpine race testing requires CGO plus a C compiler/musl development packages; production's CGO-disabled build command is not the race-test procedure.
- Run relevant existing Docker suites using isolated projects/volumes: raft, automatic, storagepool smoke, delivery, staging, and browser isolation as affected. Use their documented cleanup commands only for their disposable resources.
- Add focused benchmarks beside existing serving benchmarks for warm cache and controlled cold misses. Do not label an nginx redirect-header microbenchmark as end-to-end file throughput.
- Repeat baseline scenarios using the same driver, dataset, limits, topology, and measurement duration. Include concurrency sweeps and sustained overload, not only one successful burst.
- Report total/successful rps per configured vCPU, p50/p95/p99, per-status and transport-error counts, leader RPCs/request, bytes, CPU/memory, queue depth, disk occupancy, and cleanup after the run.
- A fixed closed-loop driver does not establish maximum sustainable capacity; describe the offered-load method and its limits.
- Update `docs/PERFORMANCE.txt` with actual measurements and reproduction commands, separating isolated tests from production observations. Do not fabricate results or hide failed trials.

## Handoff completion report

The implementing agent must provide:

1. Branch, baseline SHA, and commits by workstream.
2. What changed and the resulting behavior, including the accepted eventual-consistency consequences.
3. Exact validation commands with exit status and useful output; skipped checks with reasons.
4. Before/after measurements and resource limits, or an explicit statement that measurements could not run.
5. Config defaults/ranges, resource ownership decisions, and deviations from this plan with reasons.
6. Remaining practical limits, including nginx-open grace assumptions, aggregate confirmation load, and available disk/memory capacity.
7. Confirmation that nothing was pushed or deployed.

Do not claim full completion while required checks remain unrun or failing. Keep implementation, verification, and measurement status distinct.


## Implementation record (2026-10-10)

Branch: `serving-plan-implementation`, created after a clean fast-forward pull of
`main` to `15f8309`. This document's reviewed checkout predates that baseline.
Nothing has been pushed or deployed; Docker images and fixtures are local only.

| Workstream | Commit | Result |
| --- | --- | --- |
| 0 | `d15a501` | Baseline, explicit cold paths, closed-loop load/error reporting |
| 1 | `24c3e61` | Bounded shared fetching, verified manifest LRU, spool ownership |
| 2 | `183a5a6` | Explicit pure coherent snapshots, immediate cache-hit optimization |
| 3 | `1267bc7` | Local authenticated snapshots, explicit repositories, bounded confirmations |
| 4 | `58f4460` | Cache retirement, nginx aliases, secondary hashes, runtime, protected metrics |
| 4 correction | `036cd5d` | Positive nginx connection count at minimum supported nofile |
| 3 correction | `651286d` | Per-caller preview IO validation after leader confirmation |
| 5 | Omitted | No measured evidence that prefetch improves foreground performance safely |

Implementation choices and limits:

- Fetch concurrency/queue/spool defaults are 32/1024/800 MiB. Retained cache stays
  256 MiB and 4096 entries. Parsed manifests are bounded to 256 entries/64 MiB.
  See STORAGE_POOL.md and .env.example for validated ranges and ownership.
- Serving uses the local coordinator on followers; HTTP traffic is not redirected
  to the leader. Normal public resolution uses two local snapshot RPCs and zero
  leader RPCs. Management/writes keep their original leader paths. Private access
  adds serving session and coherent session/grant/project reads; preview IO adds
  a final local validation. Missing-record preview replay validates against the
  confirming leader separately for each caller.
- Confirmation bounds are per Store/process: 64 distinct active lookups and 256
  callers, including duplicate waiters. Aggregate admitted work scales with the
  primary instance count; it is not a cluster-wide 64-work limit.
- The nginx handoff requires nginx to open its verified retained file within 60
  seconds. Retirement is charged to the same bounded cache budget. Arbitrarily
  suspended nginx cannot be made safe by a timer alone. Temporary and branded
  responses stay in Go; cache-disabled operation retains a separate spool bound.
- Hash caches rely on immutable published content. Descriptor identity includes
  ctime as well as device/inode/size/mtime; before/after hashing is checked.
- Memory splits are 45% HTTP + 20% coordinator, or 55% without a coordinator,
  unless GOMEMLIMIT is explicit. Remaining memory includes Chromium, nginx and
  page cache. Hidden cgroup ancestors cannot be inferred; these are soft limits.
- Existing Raft Docker assertions assumed immediate follower visibility and
  quorum-loss serving denial. The first run failed on a locally pending record.
  Tests now poll only healthy-fixture site convergence (20-second test budget)
  and explicitly verify isolated applied-state serving while management fails.
  This is the accepted consistency change, not a production freshness guarantee.
- No production network/load test, actual Syncthing replication, hidden cgroup
  ancestor verification or maximum sustainable capacity certification was done.
  Closed-loop measurements and local Docker fixtures cannot establish these.

Validation and measured results: see PERFORMANCE.txt. Existing Docker fixture
cleanup removes only each disposable project's resources. No auth bundle rebuild
is needed because web/auth.js was unchanged.


### Verification results

All commands below exited 0 on Linux. Docker suites used isolated Compose project
names and disposable volumes, with `down -v` cleanup for their own projects.

| Check | Command / useful output |
| --- | --- |
| Full final race suite | `go test -race ./...` — all packages passed |
| Static checks | `go vet ./...` — no findings |
| Production commands | `go build ./cmd/...` — passed |
| Repeated cluster races | `go test -race ./internal/cluster -count=5` — 478.415 seconds |
| Admission/caller policy repeats | `go test -race ./internal/storagepool -run 'TestFetchQueueFull\|TestSharedFetchChecks' -count=5` — passed |
| Final production image | `docker build -t flux-drop:serving-plan .` — passed |
| Final role startup | `node tests/storagepool/smoke.mjs --image flux-drop:serving-plan` — both roles, supervision, identity persistence, isolation passed |
| Final Raft image | `node tests/raft/run.mjs` after isolated Compose startup — cross-node OAuth/session/project lifecycle, leader loss, isolated local serving, management denial, restart passed |
| Final automatic image | `node tests/automatic/run.mjs` after isolated Compose startup — stable identity, private files, no bootstrap on discovery outage passed |
| Final staging image | `node tests/staging/run.mjs` after isolated Compose startup — publishing/auth/privacy lifecycle, metadata outage, supervisor restart passed |
| Real nginx | Docker tests/delivery fixture — MIME, headers, HEAD, ranges, 304, public/private cache, branding, internal alias rejection passed |
| Browser isolation | `cd tests/browser && npx playwright test` with its Compose fixture — 147 passed in Chromium/Firefox/WebKit |
| Repeated fixed-resource nginx loads | Same baseline/implementation driver, five pairs, quiet pairs 3–5 reported in PERFORMANCE.txt — every fixture exited 0; baseline HTTP 503 counts are retained |

The first final-image rerun used a new Compose project name without tagging its
already-built fixture image and failed before app startup. The disposable stack
was cleaned up, fixture/Auth image aliases were corrected, and all final-image
suites reran successfully. No production behavior was bypassed.

The tests cover bounded downloads, independent readers/cancellation, per-caller
policy checks, cache-disabled ownership, manifest/hash corruption/failover,
coherent local reads/restore/conflicts, confirmation admission/negative TTL,
post-IO preview validation, unchanged management paths, retirement/LRU accounting,
protected local metrics during leader failure, and runtime resource detection.
Unperformed production acceptance measurements and the nginx-open/immutability
assumptions are explicit above and in PERFORMANCE.txt; no capacity guarantee is
inferred from these local checks.

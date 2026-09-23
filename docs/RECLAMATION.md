# Replica-aware reclamation: audit first

## Implemented

`FirestoreRepository.AuditRetention` accepts 1–100 explicit project/version IDs
and returns one read-only transactional metadata snapshot, with a 10-second
deadline. It reports observed project revision, time, classification and reason.
It is an internal maintenance primitive, not a browser API or deletion worker.
It does not enumerate filesystem paths, verify local bytes, check other replicas,
delete files, change metadata or refund quota. No new Firestore index is needed.

| Observed metadata | Result |
| --- | --- |
| Current active digest | Hold: active version |
| Any pending operation, including an expired reservation | Hold: publication pending |
| Reserved project | Hold: publication reserved |
| Expired but not tombstoned | Hold: expiry not finalized |
| Missing, unknown or incomplete metadata | Hold: manual investigation |
| Existing retirement record | Hold: retirement recorded; physical cleanup not proven |
| Other digest of a live project | Candidate: superseded version |
| Tombstoned project with no pending operation | Candidate: deleted project |

A candidate is only an unreferenced-at-this-snapshot observation. It does not
prove the version ever existed or that its bytes are reclaimable. In particular,
an upload can restore an old digest immediately after the audit. Any read error
discards the whole report; callers must not act on a partial result.

### Durable publication retirement

`RetireVersion` is now an internal privileged transaction primitive. It accepts an
explicit project/digest, a matching completed or aborted source operation, and the
expected current project revision. It rejects active versions, pending operations,
stale revisions, missing provenance and incomplete metadata. A successful call
creates an immutable `drop_retirements` record keyed by project and digest. Exact
retries return the original record without changing its timestamp or quota.

Reserve (including idempotent replays) and Activate read this record in their
publication transaction and reject the retired target. Concurrent restoration and
retirement cannot both commit: restoration changes the project pending state,
while retirement creates the record that restoration read. Any existing record,
including a malformed one, blocks publication. A project-local retirement does
not prohibit publishing those bytes as a different project.

There is no public route, CLI, scheduler or deletion worker calling retirement.
It does not change the project's public revision, active version or byte charges.
Records must not be removed or given TTL expiry. With today's digest-only storage
paths, retirement permanently blocks reusing that digest in that project. Do not
enable a retirement scheduler before generation-aware reuse is implemented.

This closes a metadata race, not a filesystem race: an installer paused after its
reservation can still write bytes after recovery and retirement, but activation
will fail. Retirement does not prove the bytes are absent or safe to unlink.
Deploy the scoped browser-deny rules and ensure every writer runs fence-aware
code before creating records. Mixed-version writers would bypass this protection.

## Why deletion is not enabled

### Generation installer foundation

The content package now provides `InstallGeneration` and `GenerationPath`.
Generation installs use `projects/<project-id>/generations/<generation-id>`;
generation IDs are exactly 64 lowercase hexadecimal characters, independent of
the content digest. Verification still checks the canonical manifest against the
expected content digest. The shared project `hash` marker remains outside public
content and keeps the original URL identifier.

An identical retry accepts an existing verified generation. Reusing a generation
for different content, a corrupted destination or a different project marker
fails without overwriting that generation. Identical bytes can be installed in
two distinct generations with independent files. Concurrent identical retries
share the same complete destination, using the existing durable installation
and verification path.

This is a storage primitive only. The caller must allocate an operation-stable
generation, persist its binding and ensure retired identities are never reused.
The installer cannot infer a removed generation's historical binding from disk.
Tests cover path validation, restore isolation, conflicting content, marker
protection, corruption rejection and concurrent retries.

**Publication and delivery have not switched layouts yet.** They continue using
legacy digest directories. Before switching, persist generations on operations
and active project metadata; update local/Nginx path selection and authenticated
peer request/response binding; scope retirement to generations; and cover the
combined lifecycle and legacy migration. Do not partially roll out new writers
with old delivery or retirement code. No existing directories are moved, and
generation installation itself does not authorize cleanup.

Current paths reuse `projects/<id>/versions/<digest>`. A read-then-delete worker
could remove a restored digest, or remove the file between Go authorization and
Nginx opening it. An expired reservation also does not prove its installer has
stopped. A paused installer may resume after a maintenance timeout. File age and
mtime are not authoritative under replication.

Discovery reports reachable locations, not a durable replication membership set
or evidence that every replica has applied a deletion. An offline replica may
later return old bytes. Therefore neither a successful local unlink nor an empty
discovery response permits returning persistent byte allowance.

## Required before automated cleanup

1. Define authoritative replica membership and lifecycle epochs independently of
   location discovery. Document how decommissioned or stale replicas rejoin, and
   ensure stale replicated state cannot be introduced into a new epoch.
2. Extend the implemented publication retirement records with generation-aware
   storage and write/read coordination. Reserve and Activate reject retired
   project/digest pairs today. Reusing a content digest
   needs a new storage generation, so deletion of an old generation cannot target
   a restored one. An expiring database lease alone does not fence filesystem I/O.
3. Coordinate local installation and delivery with cleanup. Account for the
   Nginx internal-redirect/open gap as well as direct peer streams. A grace period
   without enforced request bounds or read coordination is insufficient.
4. Establish replication deletion semantics in Flux staging: delayed writes,
   offline return, conflict files, interrupted synchronization and tombstone
   propagation. Do not assume that renaming a directory to quarantine is local;
   that rename may itself propagate to other replicas.
5. Delete only validated, fenced generation targets using rooted filesystem
   access, rejecting symlinks. Record idempotent per-member acknowledgements tied
   to a retirement ID and membership epoch. Retry safely after process crashes.
6. Refund charges transactionally once reclamation is proven, with a durable
   per-charge refund ledger. Distinct upload operations can charge the same
   digest; claiming can transfer those charges. Digest size alone is not enough
   to calculate refunds or decide which owner receives them.

Until these conditions are implemented and verified, keep conservative charges
and use storage alerts/admission limits. Any manual reclamation requires a
separately reviewed maintenance procedure that stops writers and serving across
the relevant replication cohort and verifies synchronization state. This document
does not authorize a deletion command or prescribe a fixed wait as proof of safety.

## Validation

Unit tests cover decision ordering, malformed/legacy state, expired metadata and
bounded input validation. The Firestore regression exercises publish, pending
update, activation, restoration of a previously superseded digest and deletion.
It checks that audits retain byte charges and that cancellation yields no report.
Replica deletion, physical I/O fencing, membership acknowledgement and quota refund tests remain
future work; restricted staging uses the existing digest layout with automated
retirement, physical cleanup and refunds disabled.

Retirement regressions cover stale revision/provenance rejection, active and
pending-version holds, idempotent record creation, malformed-record rejection,
publication and activation replays, project-local scope, unchanged quota, and
retirement racing restoration. Physical installer fencing remains future work.

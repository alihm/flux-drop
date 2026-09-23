# Local storage admission

Every new upload and content replacement acquires disk reservations on both the
replicated content volume and node-local staging volume after session/CSRF
validation and before reading the request body. This supplements the four-upload
concurrency limit and Raft project counts.

The Linux filesystem probe uses available blocks (not privileged reserved blocks).
Admission keeps at least 1 GiB free and, where inode accounting is supported,
1,024 free inodes. Each upload reserves its maximum raw and expanded byte limits,
8 MiB for its manifest, and block/inode overhead for the allowed path depth,
multipart spooling and installation metadata. Reservations remain held through
publication activation and are released on all returned error/success paths.

Outstanding reservations are deducted in full from each fresh capacity reading,
even if some reserved bytes are already written. This deliberately rejects some
uploads conservatively. Missing storage, failed probes, low bytes or low inodes
return `503 storage_unavailable` with `Retry-After: 30`; no body is consumed and
no publication reservation is created by that rejected request.

This is not a hard disk quota: Flux replication, another process, or another Go
process on the same volume can consume space after the check. Run one Go process
per instance, monitor bytes/inodes and replication backlog, and provision volume
limits and operational headroom. Admission does not reserve disk extents, prove
writability, reclaim old versions, or prevent all
ENOSPC failures. Those remain separate release gates. The data root must already
exist on the intended writable volume; this guard never creates a missing mount.

Tests cover concurrent reservations, idempotent release, byte/inode exhaustion,
probe failures, filesystems without inode accounting, and authenticated HTTP
rejection before reading the upload body. Real disk-full installation and
replication stress testing remain outstanding. Public publishing is enabled by
default in the passphrase-provisioned image; optional protected staging is
described in STAGING.md. The safeguards above remain in force.

## Persistent owner byte budgets

Firestore publication reservations now atomically charge the owning identity as
well as the project. Repository defaults are 1 GiB per anonymous owner and 10 GiB
per Firebase account, independently of the 10/100 project-count limits. These are
initial safety defaults, not a finalized pricing policy. The runtime accepts
`DROP_ANONYMOUS_BYTE_LIMIT` and `DROP_ACCOUNT_BYTE_LIMIT` (1 MiB–1 TiB); usage
display remains pending.

Every new operation charges the larger of its expanded payload size and 1 MiB.
The floor bounds tiny-version churn. Replaying the same idempotent operation does
not charge again. A distinct operation is charged even if it reuses an old digest;
this conservative ledger is not a precise measurement of filesystem blocks.

Activation does not refund the previous version. Abort/recovery, deletion and
expiry release applicable project counts but retain byte charges, because files
may already exist on one or more replicas. Claiming atomically transfers the
project's entire charge (including previous/aborted updates) to the account and
rejects a claim that would exceed the account budget. Deleted-project charges
remain with their owner until a future verified reclamation workflow exists.

Quota documents now use `schema: 1` and `chargedBytes`; projects also record
`chargedBytes`. Existing quota documents without this schema fail closed for
quota mutations rather than silently assuming zero retained usage. This is a
development schema change, **not an automatic migration**: before deploying over
existing data, stop all writers/maintenance, settle pending operations and audit
all retained versions, then run a separately reviewed backfill. Mixed-version
writers must not run because old writers would overwrite the new accounting.

Tests cover concurrent admission at the byte boundary, idempotent retries,
retained aborted/deleted/expired charges, claim-limit rejection and transfer, and
legacy-schema rejection. These owner budgets do not stop anonymous identity
cycling, bound all metadata/password revisions, or replace ingress abuse controls,
local disk admission and replica-aware garbage collection. No files are deleted.

A read-only retention audit is now available for bounded explicit version lists.
See [reclamation design and remaining safety gates](RECLAMATION.md). Its candidates
are observations only, not deletion authorization or evidence for quota refunds.

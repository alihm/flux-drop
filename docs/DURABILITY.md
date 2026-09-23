# Split acknowledgement policy

The user accepts possible loss of recently published content after primary loss.
That acceptance does not extend to rolling back ownership or access restrictions.
Automatic passphrase mode implements this split. Legacy manually configured
clusters retain synchronous behavior unless `asyncContent` is explicitly enabled.

## Required behavior

| Operation | Minimum successful acknowledgement |
|---|---|
| New upload reservation/activation | Durable on the current fenced primary |
| Content update reservation/activation | Durable on the current fenced primary |
| Ownership claim, rename, privacy/password change | Durably replicated security barrier |
| Project deletion/expiry | Durably replicated security barrier |
| Session creation/rotation/revocation, unlock grants/budgets | Durably replicated security barrier |
| Mixed content/security transaction, unknown operation | Durably replicated security barrier |

Local acknowledgement still requires authentication, authorization, CAS checks,
exclusive unexpired write authority, atomic persistence and fsync. It is not an
in-memory write or background goroutine returning success before persistence.
Replication is scheduled asynchronously after local durability. A bounded queue
must apply backpressure rather than exhaust disk/memory or silently drop updates.

Security acknowledgements must come from a durable majority of the agreed voting
membership, not whichever peers happen to be reachable. A singleton fresh startup
cannot promise replication to another node; security operations must wait for the
required replicas. Peer acknowledgements cover the exact generation, revision and
history digest, not a bare counter. Elections, membership changes and primary
fencing must preserve acknowledged security history across failover.

A security barrier must include its dependent metadata, including earlier local
content transactions when necessary. It cannot acknowledge only a password record
while losing the project/owner record on which that policy depends. Referenced
password material must either be included in replicated protected state or its
absence must deny access. Missing content may return unavailable; it must never
cause fallback to an older, less restrictive policy. Out-of-order asynchronous
updates must not overwrite newer ownership, privacy, deletion or revocation state.

Timeouts can mean an unknown commit outcome. Never automatically retry a write
after an ambiguous transport/leadership error. Read-only authorization checks
must remain fenced, and an old primary may not serve stale private content after
losing authority. Votes/leases are separate from per-write data replication.

## Data-loss reporting

Track election generation, local durable revision, replicated revision, committed
security barrier and history digest. Prefer an eligible candidate with the newest
compatible history, then use deterministic IP ranking for ties. A larger counter
alone does not prove a compatible or safer history.

Log potential loss when a promoted node lacks a known content revision. If a
primary disappears before telling any peer about a locally acknowledged revision,
the loss cannot always be detected. Do not claim otherwise. Timestamps are useful
for operations, but not authoritative ordering or election evidence.

## Implementation status

Implemented: an internal transaction durability field, strict validation, default
replicated acknowledgements, and explicit content-only opt-in in the repository.
Typed project comparison promotes policy/identity/status changes to replicated;
session/grant/unknown collection writes promote mixed transactions too. This field
is not a public upload option and cannot be selected by a browser header/body.
It specifies a minimum guarantee; backends may provide a stronger one.

Automatic mode uses a bounded, fsynced node-local content journal and leader-only
speculative view. Content acknowledgement does not await replication. A single
ordered worker submits the journal to Raft; security commands wait for durable
consensus after all earlier dependencies. The coordinator rechecks typed policy
changes before honoring a local hint. Unknown collections remain replicated.

Authority is refreshed periodically, not with a replication round trip on each
content write. All futures remain tracked even after request deadlines. Unknown
security outcomes fence the writer; recovery rebuilds from committed state only
after outstanding futures settle and quorum authority is re-established. Same-term
revision counters never reset during recovery. Unconfirmed local content is not
blindly replayed into a different leadership history.

Wire compatibility: omitted durability means replicated. Older strict protocol
decoders reject the new content durability field; this is not a supported mixed-
version rolling upgrade. A rollout/migration procedure must be tested before release.

Raft is retained for established membership, elections and security barriers. The
initial discovery bootstrap assumptions and total-state-loss recovery limits are
explicit in [FLUX_DEPLOYMENT.md](FLUX_DEPLOYMENT.md); discovery/history alone do
not guarantee exclusive authority against arbitrary incomplete initial views.

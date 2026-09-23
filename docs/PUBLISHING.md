# Transactional publishing backend

Implemented and tested: Firestore project transactions, durable immutable file
installation, upload/list/get/claim/update/delete handlers, and expired-reservation
recovery. The product entrypoint now wires the Publisher when
`DROP_PUBLISHING_ENABLED=true` and valid Firebase, storage and staging credentials
are configured. It remains disabled by default. See [STAGING.md](STAGING.md).

The handlers can be exercised through the Firestore integration suite without
production credentials. Google identity is mocked there; session/project writes
and file installation are real.

## API contract

All browser mutations require the management Origin, a session cookie and
`X-CSRF-Token`. List/get require an authorized session. Owner identifiers are not
returned in JSON. Identifiers from another owner's project return 404.

| Endpoint | Additional input |
| --- | --- |
| POST `/api/projects?name=example` | `Idempotency-Key`; HTML, ZIP or multipart body |
| POST `/api/projects/{id}/versions` | `Idempotency-Key`, `If-Match: "<revision>"`; upload body |
| GET `/api/projects?cursor=...` | Optional pagination cursor |
| GET `/api/projects/{id}` | None |
| POST `/api/projects/{id}/claim` | Authenticated Google session; `If-Match` |
| DELETE `/api/projects/{id}` | `If-Match` |

Upload content types: `text/html`, `application/zip`, or `multipart/form-data`.
For multipart uploads, each file part is named `files`, with its original relative
path in the filename parameter. No absolute paths, backslashes, or traversal.
A lone HTML file becomes index.html; a lone ZIP is extracted. Folder paths are
preserved, and one common enclosing directory may be stripped. The 50 MiB request
limit includes multipart overhead; expanded content is capped at 200 MiB/5,000 files.

Names are lowercase ASCII letters, digits and hyphens, 1–48 characters, without
leading/trailing hyphens. When omitted, the backend derives a friendly name from
the operation identity so retries retain it. The UI may generate a preview name
and send it explicitly. Idempotency keys are 8–128 ASCII letters/digits/underscores/
hyphens; scope is the current anonymous or Firebase owner identity.

Responses contain `project`, project `path`, and a management `claimPath`, with a
quoted revision ETag. A claim path conveys no ownership credential. Mutations on
existing projects require the last known revision in If-Match; missing/invalid
headers return 428 and stale revisions return 409.

Lists include both the current browser's unclaimed projects and authenticated UID
projects. Pagination scans at most 50 records. Expired/reserved/deleted records
are filtered; an empty page can still have a next cursor.

## Two-phase publication

1. Validate and stage files. Recompute the canonical manifest from real bytes.
2. Transactionally recheck the session, reserve slug/full digest/quota, and create
   a pending operation with a 15-minute deadline. New projects are not visible.
3. Verify and fsync files, atomically install under
   `projects/<project-id>/versions/<full-digest>/`, and fsync parent directories.
   A stable `name-suffix` marker lives at `projects/<project-id>/hash`, outside public/.
4. Recheck the session, owner, operation, digest reservation, revision and expiry
   in a transaction; activate the version and update the digest index together.

Staging and final content must be on the same filesystem. An existing version is
fully verified before reuse; corrupted or incomplete versions are not overwritten
or accepted. Filesystem failures never activate a new version. Previously active
content stays active throughout an update.

Lost responses or transient installation failures leave operations retryable with
the same key and body. An activation error is not automatically rolled back: the
commit may already have succeeded, or another replica may be completing it.
Retries of a completed operation return the project's current state and never
reapply an old version. Reusing a key for different input returns 409.

`RecoverExpired` rechecks expired pending operations transactionally and releases
their name/digest/quota reservations. It never rolls back completed publication.
This maintenance method is implemented/tested but not scheduled by the current
entrypoint. It deliberately leaves filesystem garbage for the future GC worker.

## Ownership, expiry and quotas

Anonymous expiry is 30 days from activation, rounded to Firestore's microsecond
precision. Updates retain the original expiry, name and six-character suffix.
Claiming requires both the anonymous ownership identity and a verified Google
UID; it moves quota accounting and removes expiry atomically. Authenticated
publication is claimed automatically. Another device can recover those projects
through the same Firebase UID.

Repository defaults: 10 reserved/active projects per anonymous owner and 100 per
account; the repository limits are configurable. These are count limits, not disk
budgets. Pending reservations count toward quota. Delete writes a tombstone,
releases quota and removes the active digest reference, but retains the slug so
stale replicas cannot resurrect it. Expiry is enforced in lookup/claim, not by TTL.
Automatic expiry cleanup and its quota release still require the maintenance worker.

## Duplicate behavior

Only full SHA-256 digests establish equality. The six-character suffix is an
initial URL label. A short-hash/slug collision returns a name conflict; another
name can be chosen. Public duplicate errors include a project path. Private
duplicate conflicts do not reveal a path or owner; generalized privacy/dedup
behavior must be reviewed when private hosting is enabled.

When an update activates, the old digest reference is removed. Re-uploading the
old bytes can create a new project under an available name; it must not link to a
URL now serving different content. An old URL's name remains reserved even if its
current content has changed.

## Still required before exposure

- Authorized Nginx delivery, verified replica readiness, and fallback.
- Production runtime wiring, scheduled recovery/expiry/GC, disk watermarks and
  byte/rate budgets. Old versions currently remain on disk.
- Rename/redirect and private/password lifecycle.
- File-backed digest index mirrors; Firestore is currently the digest authority.
- Agent credentials and agent API ergonomics; these handlers use browser sessions.
- Frontend publishing/management and live Google login testing.

The integration suite does not prove multi-replica failover or production IAM/index
configuration. No Firestore records, indexes, or rules have been deployed to a real
Firebase project by this work.
# Rename lifecycle

`PATCH /api/projects/{id}` accepts `application/json` with only `{"name":"new-name"}`
and requires the session cookie, exact Origin, CSRF token and `If-Match` project
revision. The transaction rechecks ownership, expiry, revision and absence of a
pending upload. The original six-character suffix and expiry are preserved;
content is not moved or rehashed. Conflicting names return 409.

Old slug mappings remain reserved and resolve directly to the current project.
Public old URLs return a no-store 307 preserving the file path and query. Private,
expired or deleted projects do not disclose a redirect. Returning to a prior name
reuses its alias. There is a per-project safety cap of 100 additional reserved
aliases; reaching it prevents new aliases but permits prior-name reuse.

The on-disk marker deliberately remains the original publication slug. Firestore
is authoritative for current naming; this avoids cross-replica marker rewrites and
allows subsequent content updates after rename. The immutable `initialSlug` is
recorded on first rename, including for projects created before this feature.

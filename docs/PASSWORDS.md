# Private-project password foundation

## Current integration status

The built-in `/unlock/<slug>` page is now implemented. Top-level navigation to a
canonical private root without a valid grant redirects there; unauthorized aliases,
assets and non-navigation requests still return 404. The page bootstraps the
existing browser session, submits the password with CSRF, clears it after attempts,
and navigates only to the validated project path. Arbitrary return URLs are ignored.
Its embedded CSS/JS are authorized with exact CSP hashes; no uploaded code is used
and no token/password is stored in browser storage. The page does not look up a
project or reveal its existence. Invalid-session recovery UX remains unfinished.

Cross-browser UI tests use the actual built-in page with intercepted test API
responses; real session/grant cookies and revocation are covered separately by the
Firestore HTTP suite. A fully combined Firestore-backed browser flow remains a
release gate. Older notes below about the absent form are now historical.

The injected project router now exposes `POST /api/unlock` and local private HTML
delivery. The product entrypoint still does not inject publishing dependencies,
so these features remain unavailable in the production binary configuration.
Historical notes below about missing endpoint/private delivery are superseded
by this section; browser UI and release validation are still unfinished.

Unlock requires the management session cookie, exact Origin, synchronizer CSRF,
and an at-most-8-KiB JSON body containing `slug` and `password`. Unknown fields and
opaque `Origin: null` requests are rejected. Owner changes and unlock verification
share one bounded Hasher per router. Successful unlock sets a project-specific
`__Host-drop-grant-<project-id>` cookie: Secure, HttpOnly, SameSite=Strict, Path=/,
no Domain, expiring with the grant. The token is absent from JSON and URLs.
Path=/ is required by the __Host prefix; project scope is enforced by the cookie
name and authoritative session/project/policy binding, not the cookie path.

Private serving requires the session and grant cookie plus top-level navigation
Fetch Metadata (`Sec-Fetch-Mode: navigate`, `Sec-Fetch-Dest: document`). Only the
root `index.html` is eligible. Assets, frames, fetches and private peer fallback
remain denied; missing local content returns 503. Every request revalidates the
grant, including HEAD/range/conditional requests, before using the separate
Nginx internal private alias. It enforces opaque-origin sandboxing, no-store,
nosniff, CORP same-origin and no CORS. Password rotation invalidates old grants.

The management unlock form, browser tests against this Firestore-backed flow,
grant-cookie capacity/cleanup behavior and private multi-replica metadata/content
fallback remain release gates. No password prompt UI exists yet; denied private
requests currently return 404. Non-browser clients must explicitly supply the
navigation metadata for this restricted profile.

Private delivery is still disabled. `internal/password` provides hashing and
immutable storage; owner policy changes are now integrated as described below.

Schema 1 uses Argon2id with fixed 64 MiB memory, two iterations, one lane, a random
16-byte salt and a 32-byte output. These exceed the minimum parameters in the
[OWASP password storage guidance](https://cheatsheetseries.owasp.org/cheatsheets/Password_Storage_Cheat_Sheet.html).
Parameters cannot be overridden by a stored record. Passwords must be valid UTF-8,
at least 12 code points and at most 1,024 bytes, without trimming or normalization.
Verification compares derived keys in constant time. Each shared Hasher allows
two concurrent operations and rejects excess work rather than queueing it.
This bounds active hashing work, not total process RSS or online guessing.

Records live at `projects/<id>/metadata/<record-digest>/password.json`, outside
public versions. This refines the plan's revision-directory layout: content-
addressed metadata allows simultaneous attempts at the same policy revision
without overwriting one another. Each record embeds the project ID and policy
revision. The future Firestore transaction must select the winning digest only
after durable installation; unreferenced files confer no authority.

Writes use rooted filesystem operations, symlink checks, owner-only permissions,
fsync and atomic no-overwrite linking. Reads are size-bounded and verify the
authoritative digest/project/revision before accepting hash data. Corrupt existing
records are not overwritten. Hash records contain no plaintext password and must
never appear in API responses or logs. Privileged local modification during I/O
is outside this storage layer's threat model.

Remaining gates: authoritative visibility/password-change transactions; shared
verification attempt limits; policy-revision-bound opaque unlock grants and
revocation; missing-password-metadata behavior across replicas; management UI;
private Nginx/peer authorization and cross-browser attack tests. Reuse one Hasher
per process when integrating it. The first private profile remains self-contained
HTML, not general cross-origin private asset access.

Tests cover unique salts, correct/wrong passwords, input/capacity/cancellation
bounds, immutable installation/retry, wrong revision, tampered files, owner-only
permissions and symlink escape prevention.

## Owner password and visibility API

When project APIs are injected, `PUT /api/projects/{id}/privacy` accepts JSON
`{"private":true,"password":"a long secret password"}` or `{"private":false}`.
It requires the session cookie, exact Origin, CSRF token and quoted `If-Match`
project revision. The body is limited to 8 KiB and unknown fields are rejected.
Every private request supplies a new password; public requests reject a password.
This unified route replaces the planned separate password/visibility mutations.

Ownership is checked before hashing. The immutable password record is installed
before a Firestore transaction rechecks session, ownership, live status, expected
revision and absence of a pending upload. The transaction selects its digest and
increments both project and policy revision. Stale attempts cannot overwrite a
concurrent change; their unselected files are inert GC candidates. Hashing uses a
single two-worker Hasher per project router; saturation returns 429.

Password-record revision is tracked separately from current access-policy revision
so renames can invalidate grants without rewriting password files. Public transitions
clear the authoritative password reference. Old immutable files are not deleted yet.
Password references/hashes are excluded from project JSON. Project content, suffix,
ownership and expiry are unchanged.

This API does not unlock content: visitor grants, shared attempt budgets, metadata
fallback and private Nginx authorization remain required. Production publishing
and private serving remain disabled. Owner mutation rate limits and orphan-file
GC are also release gates.

## Shared unlock budgets and visitor grants

`UnlockService` now performs shared-budget admission, loads the authoritative
password record and verifies its hash before issuing a grant. Firestore fixed
one-minute windows currently allow 60 attempts globally, 10 per session and 10
per private project across all replicas. Unknown/public project lookups consume
global/session budgets too. Limits are deliberately conservative and need load
testing; they are not a replacement for trusted ingress IP limits or abuse controls.
Project-wide limits can cause temporary denial of service under targeted attack.
Configure TTL on `drop_unlock_budgets.expiresAt` for cleanup; counters use explicit
window keys, so delayed TTL deletion cannot extend a window.

Tokens are 256 random bits and only their SHA-256 digest is stored in
`drop_grants`. Grants bind the exact session digest, project ID and policy revision,
expire after one hour (or project expiry, whichever comes first), and confer no
ownership. Validation reads the current session and project transactionally.
Session rotation/revocation, policy changes, expiry and deletion invalidate access.
Issuance rechecks the password digest/revision and current policy after hashing,
closing the password-change-during-verification race. Grant expiry is enforced
synchronously; configure `drop_grants.expiresAt` TTL separately for cleanup.

These are backend primitives, not yet a visitor unlock HTTP endpoint or cookie
flow. Private delivery still denies all visitors. Integrating the endpoint must
retain exact-Origin/CSRF protection, share the hashing worker pool with owner
operations, avoid tokens in URLs/logs, use secure project-scoped grant cookies,
and enforce the self-contained private HTML profile. Missing local password
metadata currently fails closed rather than fetching it from peers.

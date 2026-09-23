# Browser sessions and Google authentication

Implemented backend endpoints (enabled only when Firebase is configured):

| Method/path | Required input | Result |
| --- | --- | --- |
| POST `/api/session` | Exact configured Origin | Reads a valid session, or replaces a missing, malformed, expired or revoked cookie with a fresh anonymous session; storage failures fail closed |
| GET `/api/session` | Session cookie | CSRF token and current authentication state |
| POST `/api/auth/google` | Cookie, exact Origin, `X-CSRF-Token`, JSON `idToken` | Verifies a recent Google Firebase login and atomically rotates the session |
| POST `/api/auth/logout` | Cookie, exact Origin, `X-CSRF-Token` | Revokes the old session and issues a fresh anonymous identity |

The embedded Google popup UI and restricted staging publishing are implemented.
The planned React dashboard is not required for the current embedded interface.
The backend is intended to run behind the configured HTTPS Nginx/Flux ingress;
its loopback HTTP port does not make Secure cookies usable over public HTTP.

## Configuration

Production derives its public origin from the Flux application name and uses
Raft-backed sessions. It needs no Firebase service-account credentials; setting
`GOOGLE_APPLICATION_CREDENTIALS` or `DROP_FIREBASE_CREDENTIALS_B64` is rejected.
The optional `DROP_SESSION_CREATIONS_PER_MINUTE` defaults to 60. Public Firebase
configuration enables Google browser login, whose ID token is verified against
the configured project. Google is the only accepted provider.

If Firebase project ID is omitted, production uses the public default project
identifier. Invalid configuration fails startup. Setting emulator variables in a
production process is forbidden: the runtime requires `DROP_ENV=development`,
a `demo-` project ID, and both emulator endpoints before enabling them.

## Credentials and ownership

- Cookie: `__Host-drop-session`, 256 random bits, Secure, HttpOnly, host-only,
  Path=/, SameSite=Lax. It contains no UID or project data.
- Session record key: SHA-256 of the raw cookie token. Raw cookie values are
  never written to metadata. The CSRF token is a separate random synchronizer
  token stored in the session record and returned only by the management API.
- Anonymous record lifetime: 365 days. Cookie persistence is best-effort; users
  and browsers may delete it earlier. This increment does not implement rolling
  renewal on ordinary visits; login/logout create a fresh lifetime.
- Account authority: 12 hours, with Firebase disabled/revoked-user checks on
  authenticated session reads. The long-lived cookie alone does not extend it.
- Login requires Firebase `auth_time` within five minutes (one minute future
  skew allowed). After account expiry, the UI must perform a new Google sign-in,
  not just refresh an old ID token.
- Login preserves the anonymous ownership ID so those projects can be claimed.
  It does not claim projects automatically; that transaction is a later stage.
- Switching Firebase accounts requires logout. Logout creates a different
  anonymous ownership ID. The UI must explain that users should claim projects
  they want to retain before logout, and must also sign out of Firebase's client
  SDK so it cannot silently log the previous account back in.
- `GET /api/session` and protected mutations reject stale or revoked cookies.
  Only the same-origin `POST /api/session` bootstrap replaces them with a fresh
  anonymous identity, including after a lost rotation response. A late bootstrap
  response from another tab can replace a newer browser cookie, so users may
  need to sign in again; it cannot grant another account's projects. Anonymous-only
  access tied to the lost token may be unrecoverable. Cross-device recovery
  requires claiming projects.

## Authority and failure handling

Rotation is an atomic Raft metadata transaction that rechecks expiry, revocation, and CSRF,
creates the new record, and revokes the old record together. Concurrent rotations
have one winner. Failed Google verification leaves the existing session intact.

Firebase verification or metadata outages fail closed with 503. They never downgrade an account
request into successful anonymous authorization. Logout can still operate when
the Google provider is unavailable, provided metadata is available.

All cookie-authenticated mutations require exact Origin checks. Missing and
`null` origins fail. Login and logout additionally require the synchronizer CSRF
token; bootstrap creates no account privilege and requires the Origin gate.
Duplicate session cookies and CSRF headers are rejected. Identity comes only
from verified tokens; a client-supplied UID is rejected as an unknown JSON field.

These endpoints are browser endpoints, not the future agent-credential API.

## Limits and cleanup

Session creation uses a shared Raft per-minute budget across replicas;
default 60/minute, configurable. Token rotations have a 20/minute chain budget
that carries across replacements. These bounds do not replace ingress rate
limits or limits on invalid login attempts; those remain a production gate.

Expiry checks happen on every session read and do not depend on external TTL.
Revoked records cannot become valid again. The isolated Firestore emulator
backend remains for regression tests, not production authority.

## Verification limits

Unit tests cover ownership isolation, CSRF, account switching, expiration,
revocation/provider failure, malformed/unsigned Firebase tokens, cookie flags,
request limits, and stale requests. Emulator tests exercise legacy Firestore
transactions; live Google popup/sign-in remains a deployment acceptance test.

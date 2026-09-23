# Browser sessions and Google authentication

Implemented backend endpoints (enabled only when Firebase is configured):

| Method/path | Required input | Result |
| --- | --- | --- |
| POST `/api/session` | Exact configured Origin | Creates an anonymous session if no cookie exists; otherwise reads the existing session |
| GET `/api/session` | Session cookie | CSRF token and current authentication state |
| POST `/api/auth/google` | Cookie, exact Origin, `X-CSRF-Token`, JSON `idToken` | Verifies a recent Google Firebase login and atomically rotates the session |
| POST `/api/auth/logout` | Cookie, exact Origin, `X-CSRF-Token` | Revokes the old session and issues a fresh anonymous identity |

The embedded Google popup UI and restricted staging publishing are implemented.
The planned React dashboard is not required for the current embedded interface.
The backend is intended to run behind the configured HTTPS Nginx/Flux ingress;
its loopback HTTP port does not make Secure cookies usable over public HTTP.

## Configuration

```text
DROP_PUBLIC_ORIGIN=https://appname.app.runonflux.io
FIREBASE_PROJECT_ID=<Firebase project used for Drop identities>
GOOGLE_APPLICATION_CREDENTIALS=/run/secrets/firebase-service-account.json
DROP_SESSION_CREATIONS_PER_MINUTE=60
```

Use application-default credentials or a mounted service account with access to
the intended Firestore database and Firebase user records. Never embed service
credentials in frontend config or commit them. The Firebase SDK verifies token
audience against the configured project. Google is the only accepted provider.

If Firebase project ID is omitted, the foundation runs with authentication
disabled. Invalid configuration fails startup. Setting emulator variables in a
production process is forbidden: the runtime requires `DROP_ENV=development`,
a `demo-` project ID, and both emulator endpoints before enabling them.

## Credentials and ownership

- Cookie: `__Host-drop-session`, 256 random bits, Secure, HttpOnly, host-only,
  Path=/, SameSite=Lax. It contains no UID or project data.
- Firestore document ID: SHA-256 of the raw cookie token. Raw cookie values are
  never written to Firestore. The CSRF token is a separate random synchronizer
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
- Stale or revoked cookies return 401 without setting/deleting cookies. This
  prevents a delayed old request from overwriting the new cookie after login.
  If a rotation response is lost, signing in again after explicitly clearing the
  invalid browser session can restore claimed projects; anonymous-only access
  may be lost. Cross-device recovery requires claiming.

## Authority and failure handling

Rotation is a Firestore transaction that rechecks expiry, revocation, and CSRF,
creates the new record, and revokes the old record together. Concurrent rotations
have one winner. Failed Google verification leaves the existing session intact.

Firebase/Firestore outages fail closed with 503. They never downgrade an account
request into successful anonymous authorization. Logout can still operate when
the Google provider is unavailable, provided Firestore is available.

All cookie-authenticated mutations require exact Origin checks. Missing and
`null` origins fail. Login and logout additionally require the synchronizer CSRF
token; bootstrap creates no account privilege and requires the Origin gate.
Duplicate session cookies and CSRF headers are rejected. Identity comes only
from verified tokens; a client-supplied UID is rejected as an unknown JSON field.

These endpoints are browser endpoints, not the future agent-credential API.

## Limits and cleanup

Session creation uses a shared Firestore per-minute budget across replicas;
default 60/minute, configurable. Token rotations have a 20/minute chain budget
that carries across replacements. These bounds do not replace ingress rate
limits or limits on invalid login attempts; those remain a production gate.

Enable Firestore TTL on `expiresAt` for `drop_sessions` and
`drop_session_budgets` before production. Expiry checks do not rely on TTL timing.
Revoked records remain until expiry so they cannot become valid again. Do not
apply Firestore TTL to live claimed project records.

Use Drop-specific server-only rules. An existing broad allow rule can override
an apparent deny match because rules are additive: use a dedicated database or
audit the combined rules before sharing a Firebase project. Admin SDK calls use
IAM and bypass Firestore client security rules.

## Verification limits

Unit tests cover ownership isolation, CSRF, account switching, expiration,
revocation/provider failure, malformed/unsigned Firebase tokens, cookie flags,
request limits, and stale requests. Emulator tests exercise real Firestore
transactions. A real Google popup/sign-in flow awaits frontend/configuration;
these tests do not claim to validate that live integration.

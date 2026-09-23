# Browser isolation integration suite

This harness runs real Nginx over HTTPS, a **test-only** Go authorization fixture,
and pinned React/Vue static bundles. It imports the production mutation-origin
gate and the production Nginx sandbox-header snippet. It does not replace later
end-to-end tests against real Firestore sessions and production routing.

## Run

From this directory (requires Node 20+, npm, OpenSSL, Docker Compose):

```sh
npm ci --ignore-scripts
npm run fixtures
npx playwright install chromium firefox webkit
mkdir -p certs
openssl req -x509 -newkey rsa:2048 -nodes -keyout certs/key.pem -out certs/cert.pem -days 2 -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1
docker compose up --build -d
docker compose exec -T nginx nginx -t
docker compose exec -T nginx nginx -t -c /etc/nginx/production-nginx.conf
npm test
docker compose down
```

Linux browser runtime libraries must be available; Playwright reports missing
dependencies at startup. The test server binds only to `127.0.0.1:18443`.
On a dedicated test host, `npx playwright install-deps chromium firefox webkit`
installs those system packages; it requires administrator privileges.
Certificates are test-only, short-lived, and ignored by Git. Renew before a run
if expired. Playwright deliberately accepts this local self-signed certificate.
No production credentials, Firestore records, or real project files are used.

## Coverage

- Positive control: management mutations require the fixture session and CSRF.
- Opaque-origin HTML runs inline JavaScript but cannot read cookies/storage.
- Obfuscated JavaScript cannot read management responses or mutate state,
  even when the test supplies the correct CSRF value.
- Null/missing/foreign Origin and forged peer headers are rejected.
- SVG navigation is sandboxed; workers, service workers, and management iframe
  access are blocked.
- Public React/Vue ES module builds work using relative paths and public asset CORS.
- Private self-contained HTML remains interactive after unlocking.
- Private documents, scripts, and images cannot be read/embedded by another project.
- Private separate-file module builds stay blocked: no credentialed null-origin CORS.
- Authorization precedes HEAD, range, conditional, and internal redirect delivery.

The fixture's `/manage` creates an in-memory test session, and `/api/unlock`
unlocks fixture content without a password after origin/CSRF checks. Neither
endpoint is a product implementation or may be shipped in the runtime image.

## Interpretation

Passing these tests validates the selected primitives in the fixture, not all
possible browser attacks or the unfinished product. Repeat them against the real
delivery/session implementation as it replaces the fixture. Additional gates
include renames/redirects, replica fallback, expiry, and policy revocation.

Recorded local result: 39 passed (13 cases per engine) with Playwright 1.58.2,
Nginx 1.28.0, React 19.2.0, and Vue 3.5.25. Both Nginx configurations passed syntax
validation. These pinned test versions should be updated and rerun before release.

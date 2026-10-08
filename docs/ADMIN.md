# Primary administration

The current build was published on 2026-10-08 as `alihmahdavi/flux-drop:staging`, registry digest `sha256:9ffcc23ba708a4abf55f59c8975577c07d8265c453b6b2263b1440335214b9dd`. It includes the public claim sign-in dialog, seven-day expiry for new anonymous projects, and [320 × 180 project thumbnails with a claimed-public-only Explore gallery](PREVIEWS.md). Verification includes the full Go race suite and focused follow-up race checks, Go vet, twelve admin browser checks from the admin build, twenty-four authentication/claim/gallery checks across Chromium/Firefox/WebKit, production-renderer and sandbox image checks, and isolated primary/secondary startup tests. Two WebKit checks exceeded their timeout during concurrent compilation; all four WebKit authentication checks passed when rerun without the compiler. Browser screenshots use sample data, not deployed capacity.

Open `https://drop.app.runonflux.io/admin/` on the primary. The authorized ZelID defaults to `15c3aH6y9Koq1Dg1rGXE9Ypn5nL2AbSJCu`. `DROP_ADMIN_ZELID` optionally overrides it with one valid P2PKH ZelID. Secondaries do not expose this page. No extra ports or API keys are needed for browser administration.

Choose **Open Zelcore**, approve the login message, and leave the browser open for automatic login. The manual fallback lets you copy the message into Zelcore and paste its signature. Challenges expire after five minutes; sessions expire after eight hours. Challenges, approval, sessions, revocation, and app controls are stored in quorum-backed primary metadata, so load balancing between primary replicas works. Browser-bound challenge cookies, origin checks, synchronizer CSRF tokens, and one-time redemption protect login and mutations. Native wallet callbacks verify the exact stored message and the configured wallet locally. No wallet private keys or storage credentials enter the browser.

Before authentication the page shows only a standalone login form; the sidebar and dashboard appear after login. The configured ZelID stays on the backend: it is absent from the page, signing message, challenge response, and session response. Manual login submits only the challenge ID, polling token, and signature; the server checks that signature against its configured wallet.

The **Projects** tab lists records across all users, including owner IDs, storage placement, active-version size, privacy, status, creation time, and expiry. App allocation also includes retained versions and storage overhead. Search matches project ID, slug, owner ID/type, storage app, and status. Results are paginated over bounded batches of 100 project records; use Next even when a filtered page has no matches. Password digests, authentication tokens, and transfer credentials are never returned. Opening a private site still requires its visitor password. This initial tab is read-only; users manage their own content, sharing, and passwords from **My sites** on the main page.

The dashboard searches configured secondary app names and instance addresses. It shows replica health, conservative logical capacity, retained accounting, and available capacity. A replicated app contributes capacity once. Offline or stale instances do not contribute current capacity estimates. Availability respects allocation limits, filesystem reserves, and one GiB headroom. Upload admission separately checks temporary space for both incoming and installed content; the displayed available capacity is not divided by two. It is an estimate; upload-time capacity and inode checks remain authoritative.

**Drain** stops all new uploads and version updates on the app while preserving serving. **Remove** excludes an empty app from placement and pool totals. Removal is refused if either retained bytes or retained inodes are nonzero, including abandoned uploads and deleted projects awaiting verified cleanup. Removal never retires the Flux deployment or deletes volumes. **Restore** reenables an app still present in `DROP_STORAGE_APPS_JSON`; a static `drain: true` environment setting still takes precedence. Controls persist across primary restarts and apply across all replicas. Allocation and removal check the same metadata records atomically, so stale placement offers cannot bypass a removal.

Removing a configured app from `DROP_STORAGE_APPS_JSON` bypasses dashboard safeguards; keep entries for any apps holding projects. The dashboard deliberately does not edit Flux deployments or hold wallet spending credentials.

Roll out the new image to every primary replica before relying on consistent dashboard availability. Drain/removal also sets an allocation marker already rejected by the preceding image, preventing older replicas from ignoring the new controls during an update. Such older replicas can temporarily reject uploads rather than select another app; restoring an app restores its original accounting block size.

| Method | Endpoint | Purpose |
| --- | --- | --- |
| GET | `/admin/` | Login and dashboard |
| POST | `/admin/api/challenge` | Issue a browser-bound signing challenge |
| POST | `/admin/api/login` | Redeem a manual wallet signature |
| POST | `/admin/api/wallet-callback` | Receive a native Zelcore signature |
| POST | `/admin/api/wallet-status` | Redeem wallet callback approval |
| GET | `/admin/api/session` | Current wallet session and CSRF token |
| POST | `/admin/api/logout` | Revoke the admin session |
| GET | `/admin/api/apps` | App capacity, accounting, controls, and replica health |
| GET | `/admin/api/projects` | All-user project records; optional `q`, `app`, and `cursor` filters |
| POST | `/admin/api/apps/{appName}` | Apply `{"action":"drain"}`, `{"action":"remove"}`, or `{"action":"restore"}` |

All data and action endpoints require wallet authentication. Browser POSTs require the exact primary public origin. Logout and app actions additionally require `X-CSRF-Token`. The native callback accepts JSON or URL-encoded form data with `message` (or `loginPhrase`) and `signature`; it grants no session directly. Login requests are limited to 20 challenges per minute across the primary app. Durable storage uses 256 challenge slots and 2,048 session slots; overwritten records fail closed.

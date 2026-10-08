# Flux deployment and update tool

`scripts/flux-deploy.mjs` is a permanent, dependency-free Node.js 20+ tool for
both primary and secondary Drop apps. It uses the wallet signing and Enterprise
encryption formats from Orbit, checked against the official Flux handlers.
It never signs with a private wallet key and never makes a payment.

Private login sessions, decrypted specifications, proposed changes, exact signing
messages and submission receipts are saved under `secrets/flux-deploy/`, excluded
from Git and Docker build context. Files are created exclusively with mode 0600
inside directories created with mode 0700. Back up that private directory securely
if you need the originals after replacing this workspace. Keep the script and
this guide in Git; do not commit decrypted specifications or session signatures.

## Login and specification backup

```bash
node scripts/flux-deploy.mjs login
```

Sign the printed login phrase with the app owner's wallet. Save only the
signature in a private text file, then use the printed login-file path:

```bash
node scripts/flux-deploy.mjs authenticate --login secrets/flux-deploy/login-TIMESTAMP.json --zelid YOUR_WALLET_ADDRESS --signature-file secrets/login-signature.txt
node scripts/flux-deploy.mjs snapshot --app dropstoragea --session secrets/flux-deploy/session-TIMESTAMP.json
```

The snapshot command reads the deployed specification, verifies wallet ownership,
decrypts Enterprise compose/contacts, and saves `original.decrypted.json` and
`original.redacted.json` in a new timestamped app directory. The redacted version
retains public settings, resource types, ports and mounts. Unknown environment
values, contacts, commands and repository credentials are removed. You can copy
that reviewed redacted version into `deploy/flux/templates/` to retain a template
in Git; all placeholders must be filled privately before registration.

`deploy/flux/templates/dropstoragea.json` records the original deployed secondary
specification with its secret replaced by a placeholder. It is a source template,
not an immediately deployable configuration: the original had `r:/data` and no
mapped ports. For a new deployment set the complete Container Data string below
and map its configured direct TLS storage port to the same container port. The
original full decrypted backup remains in the private snapshot directory.

`deploy/flux/templates/dropstoragea.configured.json` records the confirmed update:
`r:/data|ml:state:/var/lib/drop-cluster`, direct port `36447` mapped to container
port `36447`, and two instances. It retains a placeholder for the storage API key.
For new deployments replace the app name/key/available port and choose a fresh
subscription duration; use `--enterprise true` when preparing registration.

## Update Container Data

```bash
node scripts/flux-deploy.mjs prepare --action update --app dropstoragea --session secrets/flux-deploy/session-TIMESTAMP.json --container-data 'r:/data|ml:state:/var/lib/drop-cluster'
```

This backs up the current spec first and changes only the selected component's
Container Data, plus the remaining subscription block count needed to preserve
the existing expiry. Multiple components require `--component COMPONENT_NAME`.
It preserves environment variables, image, resources, ports, owner and Enterprise
protection. For other updates, pass `--spec PRIVATE_EDITED_SPEC.json`, copied from
the decrypted snapshot, preserving `_wasEnterprise`. The tool fetches the live
spec again, verifies its identity, and derives expiry from that live spec.

Preparation encrypts Enterprise secrets **before** sending the specification to
Flux verification and pricing. It saves the verified payload unchanged in
`prepared.json`, the literal wallet message in `message-to-sign.txt`, and both
private and redacted proposed specs. Review the redacted diff and quoted price;
then sign the exact contents of the message file without adding a newline.

```bash
node scripts/flux-deploy.mjs submit --session secrets/flux-deploy/session-TIMESTAMP.json --prepared secrets/flux-deploy/APP/RUN/prepared.json --signature-file secrets/update-signature.txt
```

Submission checks the message hash, ownership, freshness, pending messages, and
the deployed spec's hash/height. Temporary messages whose hash exactly matches
the confirmed deployed specification are recognized as already confirmed; other
messages for the app block preparation/submission. It saves the accepted transaction before looking
up any required payment address. Paid submissions print the Flux amount, address
and transaction hash to use as the payment memo. Payment is a separate wallet
action. Do not blindly retry a submission after a network timeout: inspect Flux
temporary/permanent messages for the app first. The server may have accepted it.
The tool blocks another submission if it already recorded a receipt.

## Register a new primary or secondary

Create a private complete v8 spec from the saved template. Set the new app name,
wallet owner, instance count, available external ports, container ports, resource
allocation, subscription duration (`expire` in blocks), role/environment settings
and Container Data. Use `docs/STORAGE_POOL.md` for the role-specific configuration.
Use a different storage port/key for each secondary; its primary name is `drop`
for this deployment. Then:

```bash
node scripts/flux-deploy.mjs prepare --action register --session secrets/flux-deploy/session-TIMESTAMP.json --spec secrets/new-secondary.json --enterprise true
```

Review, sign and submit using the same workflow as updates. Registration does not
copy another app's node-local state or volumes. Drop secrets require Enterprise
protection. The tool supports v8 only and fails if template placeholders remain.

## Request formats and verification

- Raw JSON request bodies retain numeric ports/resources/version/timestamp and
  boolean flags. They are serialized once, not nested URL-encoded forms. Flux
  handlers parse the raw stream via `ensureObject`; the media type matches Orbit
  and FluxUI (`application/x-www-form-urlencoded`).
- `zelidauth` is a URL-encoded header with `zelid`, `signature`, `loginPhrase`.
- `/apps/getpublickey` uses a form-encoded `name` and `owner` body.
- The backend that issued the login phrase is retained for authentication,
  encryption, validation and submission; redirects are rejected.
- Enterprise uses RSA-OAEP/SHA-256 wrapping the base64 AES-256 key, AES-GCM with a
  12-byte nonce and 16-byte tag, and Flux's base64 binary framing.
- Wallet messages are exactly `fluxappregister1` or `fluxappupdate1`, followed by
  `JSON.stringify(verifiedSpec)` and the integer millisecond timestamp.
- Chain-only `height`/`hash` and local helper fields are excluded from submitted
  specs; v8 `datacenter`, `staticip` and all compose settings are retained.

```bash
node --test tests/flux-deploy/tool.test.mjs
```

Official handlers: [validation](https://github.com/RunOnFlux/flux/blob/master/ZelBack/src/services/appRequirements/appValidator.js),
[registration](https://github.com/RunOnFlux/flux/blob/master/ZelBack/src/services/appDatabase/registryManager.js),
[updates](https://github.com/RunOnFlux/flux/blob/master/ZelBack/src/services/appLifecycle/advancedWorkflows.js),
[key requests](https://github.com/RunOnFlux/flux/blob/master/ZelBack/src/services/appMessaging/cryptographicKeys.js).

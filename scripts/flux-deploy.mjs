#!/usr/bin/env node
// Standalone Flux wallet/Enterprise deployment workflow. No external packages.
import crypto from 'node:crypto';
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

export const CONTAINER_DATA = 'r:/data|ml:state:/var/lib/drop-cluster';
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const PRIVATE = path.join(ROOT, 'secrets', 'flux-deploy');
const API = 'https://api.runonflux.io';
const APP = /^[a-zA-Z][a-zA-Z0-9]{2,31}$/;
const FIELDS = ['version', 'name', 'description', 'owner', 'contacts', 'geolocation', 'expire', 'nodes', 'staticip', 'datacenter', 'enterprise', 'instances', 'compose'];

export function authHeader(session) {
  return new URLSearchParams({ zelid: session.zelid, signature: session.signature, loginPhrase: session.loginPhrase }).toString();
}
export function stickyBackend(header) {
  const ip = header?.split('_').at(-1)?.trim();
  if (!ip || !/^(?:\d{1,3}\.){3}\d{1,3}$/.test(ip) || ip.split('.').some(n => +n > 255)) throw new Error('Flux did not return a valid sticky backend');
  return `https://${ip.replaceAll('.', '-')}-16127.node.api.runonflux.io`;
}
function trustedBackend(base) {
  if (base !== API && !/^https:\/\/(?:\d{1,3}-){3}\d{1,3}-16127\.node\.api\.runonflux\.io$/.test(base)) throw new Error('Untrusted Flux API endpoint');
  return base;
}
export function requestOptions({ session, body, headers = {}, timeout = 30000 } = {}) {
  // Flux handlers read the raw stream and ensureObject() JSON-parses it first.
  // Orbit/FluxUI send raw JSON under this media type. Do not form-encode nested
  // specs, double-stringify them, or turn numbers/booleans into strings.
  return {
    method: body == null ? 'GET' : 'POST', redirect: 'error',
    headers: { Accept: 'application/json', 'x-apicache-bypass': 'true', ...(body == null ? {} : { 'Content-Type': 'application/x-www-form-urlencoded' }), ...(session ? { zelidauth: authHeader(session) } : {}), ...headers },
    ...(body == null ? {} : { body: typeof body === 'string' ? body : JSON.stringify(body) }),
    signal: AbortSignal.timeout(timeout),
  };
}
async function request(base, route, options = {}) {
  const response = await fetch(`${trustedBackend(base)}${route}`, {
    ...requestOptions(options),
  });
  let json;
  try { json = await response.json(); } catch { throw new Error(`Flux ${route.split('?')[0]} returned non-JSON (HTTP ${response.status})`); }
  // Upstream errors can echo secrets. Never include arbitrary response bodies.
  if (!response.ok || json.status !== 'success' || json.data == null) throw new Error(`Flux ${route.split('?')[0]} failed (HTTP ${response.status}); response details withheld`);
  return { data: json.data, headers: response.headers };
}
async function read(file) { return JSON.parse(await fs.readFile(file, 'utf8')); }
async function secretText(file) { const value = (await fs.readFile(file, 'utf8')).trim(); if (!value) throw new Error('Signature file is empty'); return value; }
async function save(file, value) {
  await fs.mkdir(path.dirname(file), { recursive: true, mode: 0o700 });
  const handle = await fs.open(file, 'wx', 0o600);
  try { await handle.writeFile(typeof value === 'string' ? value : JSON.stringify(value, null, 2) + '\n'); await handle.sync(); } finally { await handle.close(); }
}
function appName(name) { if (!APP.test(name || '')) throw new Error('Invalid Flux app name'); return name; }
function wrapKey(publicKey, aesKey) {
  return crypto.publicEncrypt({ key: crypto.createPublicKey({ key: Buffer.from(publicKey.replace(/\s/g, ''), 'base64'), format: 'der', type: 'spki' }), padding: crypto.constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256' }, Buffer.from(aesKey.toString('base64')));
}
export function encryptPayload(publicKey, payload) {
  const key = crypto.randomBytes(32), nonce = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', key, nonce);
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(payload)), cipher.final()]);
  return Buffer.concat([wrapKey(publicKey, key), nonce, ciphertext, cipher.getAuthTag()]).toString('base64');
}
export function decryptPayload(encoded, key) {
  const bytes = Buffer.from(encoded, 'base64');
  if (bytes.length < 29) throw new Error('Invalid encrypted specification');
  const decipher = crypto.createDecipheriv('aes-256-gcm', key, bytes.subarray(0, 12));
  decipher.setAuthTag(bytes.subarray(-16));
  return JSON.parse(Buffer.concat([decipher.update(bytes.subarray(12, -16)), decipher.final()]).toString());
}
async function publicKey(spec, session, owner = spec.owner) {
  return (await request(session.stickyBackend, '/apps/getpublickey', { session, headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ name: spec.name, owner }).toString() })).data;
}
async function fetchSpec(name, session) {
  const spec = (await request(session.stickyBackend, `/apps/appspecifications/${appName(name)}`, { session })).data;
  if (spec.name !== name || spec.owner !== session.zelid) throw new Error('Specification identity or wallet ownership mismatch');
  if (!spec.enterprise) return spec;
  const owner = (await request(session.stickyBackend, `/apps/apporiginalowner/${name}`, { session })).data;
  const key = crypto.randomBytes(32);
  const wrapped = wrapKey(await publicKey(spec, session, owner), key).toString('base64');
  const encrypted = (await request(session.stickyBackend, `/apps/appspecifications/${name}/true`, { session, headers: { 'enterprise-key': wrapped } })).data;
  const payload = decryptPayload(encrypted.enterprise, key);
  if (!Array.isArray(payload.compose) || !payload.compose.length || !Array.isArray(payload.contacts)) throw new Error('Incomplete decrypted Enterprise specification');
  return { ...spec, compose: payload.compose, contacts: payload.contacts, enterprise: '', _wasEnterprise: true };
}
export function wireSpec(spec) {
  if (spec.version !== 8) throw new Error('Only Flux specification v8 is supported; convert explicitly before use');
  return Object.fromEntries(FIELDS.filter(k => Object.hasOwn(spec, k)).map(k => [k, structuredClone(spec[k])]));
}
export function changeContainerData(spec, componentName) {
  const result = structuredClone(spec);
  if (!Array.isArray(result.compose) || !result.compose.length) throw new Error('A decrypted compose specification is required');
  const selected = componentName ? result.compose.filter(c => c.name === componentName) : result.compose;
  if (selected.length !== 1) throw new Error('Select exactly one component with --component for multi-component apps');
  selected[0].containerData = CONTAINER_DATA;
  return result;
}
export function remainingBlocks(spec, height) {
  if (![spec.height, spec.expire, height].every(Number.isSafeInteger) || spec.height < 1 || spec.expire < 1 || height < spec.height) throw new Error('Cannot establish subscription expiry');
  const remaining = spec.height + spec.expire - height;
  if (remaining <= 0) throw new Error('App subscription has expired; an explicit renewal is required');
  return remaining;
}
export function signingMessage(action, spec, timestamp) {
  if (!['register', 'update'].includes(action) || !Number.isSafeInteger(timestamp)) throw new Error('Invalid signing parameters');
  return `fluxapp${action}1${JSON.stringify(spec)}${timestamp}`;
}
export function redactSpec(spec) {
  const result = structuredClone(spec);
  result.contacts = [];
  delete result.enterprise;
  for (const c of result.compose || []) {
    if (c.repoauth) c.repoauth = '<PRIVATE_REPO_AUTH>';
    c.commands = (c.commands || []).map(() => '<PRIVATE_COMMAND_REQUIRED>');
    // Preserve only explicitly known public settings, never unknown env values.
    const publicEnv = /^(DROP_ROLE|DROP_PRIMARY_APP_NAME|DROP_STORAGE_PORT|DROP_STORAGE_CAPACITY_BYTES|DROP_CACHE_BYTES|FLUX_APP_NAME|DROP_ENV|DROP_DATA_DIR|DROP_PUBLIC_ORIGIN)$/;
    c.environmentParameters = (c.environmentParameters || []).map(entry => {
      const index = entry.indexOf('='), name = index < 0 ? entry : entry.slice(0, index);
      return publicEnv.test(name) ? entry : `${name}=<PRIVATE_VALUE_REQUIRED>`;
    });
  }
  return result;
}
function options(args) {
  const result = {};
  for (let i = 0; i < args.length; i += 2) {
    if (!args[i].startsWith('--') || !args[i + 1] || args[i + 1].startsWith('--') || Object.hasOwn(result, args[i].slice(2))) throw new Error('Options must be unique --name value pairs');
    result[args[i].slice(2)] = args[i + 1];
  }
  return result;
}
function requireOption(o, key) { if (!o[key]) throw new Error(`Missing --${key}`); return o[key]; }
async function sessionFor(o) {
  const session = await read(requireOption(o, 'session'));
  if (!session.zelid || !session.signature || !session.loginPhrase || !session.stickyBackend) throw new Error('Incomplete session');
  trustedBackend(session.stickyBackend);
  return session;
}
export function unconfirmedMessages(messages, name, confirmed) {
  if (!Array.isArray(messages)) throw new Error('Cannot check pending app messages');
  return messages.filter(m => {
    if (![m.appSpecifications, m.zelAppSpecifications, m.appSpecification].some(s => s?.name === name)) return false;
    // Flux retains temporary messages after chain confirmation. Only the exact
    // hash of the confirmed spec can be dismissed without guessing by age.
    return !(confirmed?.name === name && Number.isSafeInteger(confirmed.height) && confirmed.height > 0 && confirmed.hash && m.hash === confirmed.hash);
  });
}
async function assertNoPending(name, session, confirmed) {
  const messages = (await request(session.stickyBackend, '/apps/temporarymessages', { session })).data;
  if (unconfirmedMessages(messages, name, confirmed).length) throw new Error('App has a pending registration/update; wait for confirmation before preparing another');
}
function assertComplete(spec) {
  if (JSON.stringify(spec).includes('<PRIVATE_') || JSON.stringify(spec).includes('<REPLACE_')) throw new Error('Template still contains unfilled placeholders');
  if (!Array.isArray(spec.compose) || spec.compose.length < 1) throw new Error('Decrypted compose is required');
}
async function snapshot(name, session) {
  const spec = await fetchSpec(name, session);
  const dir = path.join(PRIVATE, name, `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`);
  await save(path.join(dir, 'original.decrypted.json'), spec);
  await save(path.join(dir, 'original.redacted.json'), redactSpec(spec));
  return { spec, dir };
}
export async function main(args = process.argv.slice(2)) {
  const [command, ...rest] = args, o = options(rest);
  const allowed = {
    login: [], authenticate: ['login', 'zelid', 'signature-file'],
    snapshot: ['session', 'app'],
    prepare: ['session', 'action', 'app', 'spec', 'container-data', 'component', 'enterprise'],
    submit: ['session', 'prepared', 'signature-file'],
  };
  if (!allowed[command] || Object.keys(o).some(k => !allowed[command].includes(k))) throw new Error('Unknown command/option; see docs/FLUX_APP_TOOL.md');
  if (o.enterprise && !['true', 'false'].includes(o.enterprise)) throw new Error('--enterprise must be true or false');
  if (command === 'login') {
    const { data: loginPhrase, headers } = await request(API, '/id/loginphrase');
    if (typeof loginPhrase !== 'string' || loginPhrase.length < 40) throw new Error('Invalid Flux login phrase');
    const file = path.join(PRIVATE, `login-${Date.now()}.json`);
    await save(file, { loginPhrase, stickyBackend: stickyBackend(headers.get('fluxnode')), createdAt: new Date().toISOString() });
    console.log(JSON.stringify({ loginFile: file, loginPhrase }, null, 2));
    return;
  }
  if (command === 'authenticate') {
    const login = await read(requireOption(o, 'login'));
    const session = { ...login, zelid: requireOption(o, 'zelid'), signature: await secretText(requireOption(o, 'signature-file')) };
    await request(session.stickyBackend, '/id/verifylogin', { body: { zelid: session.zelid, signature: session.signature, loginPhrase: session.loginPhrase } });
    const file = path.join(PRIVATE, `session-${Date.now()}.json`);
    await save(file, session);
    console.log(JSON.stringify({ sessionFile: file }));
    return;
  }
  const session = await sessionFor(o);
  if (command === 'snapshot') {
    const { dir } = await snapshot(requireOption(o, 'app'), session);
    console.log(JSON.stringify({ snapshotDirectory: dir })); return;
  }
  if (command === 'prepare') {
    const action = requireOption(o, 'action');
    if (!['register', 'update'].includes(action)) throw new Error('--action must be register or update');
    let original, dir, plain;
    if (action === 'update') {
      const name = appName(requireOption(o, 'app'));
      ({ spec: original, dir } = await snapshot(name, session));
      await assertNoPending(name, session, original);
      plain = o.spec ? await read(o.spec) : structuredClone(original);
      if (plain.name !== original.name || plain.owner !== original.owner || plain.version !== original.version) throw new Error('Update must preserve app name, owner and spec version');
      if (Boolean(original._wasEnterprise) !== Boolean(plain._wasEnterprise || plain.enterprise)) throw new Error('Update must preserve Enterprise protection');
      const info = (await request(session.stickyBackend, '/daemon/getinfo', { session })).data;
      plain.expire = remainingBlocks(original, Number(info.blocks));
    } else {
      plain = await read(requireOption(o, 'spec'));
      appName(plain.name);
      if (plain.owner !== session.zelid) throw new Error('Registration owner must match signing wallet');
      dir = path.join(PRIVATE, plain.name, `${Date.now()}-${crypto.randomBytes(4).toString('hex')}`);
    }
    if (o['container-data']) {
      if (o['container-data'] !== CONTAINER_DATA) throw new Error(`Supported Container Data: ${CONTAINER_DATA}`);
      plain = changeContainerData(plain, o.component);
    }
    assertComplete(plain);
    let wire = wireSpec(plain);
    const enterprise = Boolean(original?._wasEnterprise || plain._wasEnterprise || plain.enterprise || o.enterprise === 'true');
    if (enterprise) {
      wire.enterprise = encryptPayload(await publicKey(wire, session), { contacts: wire.contacts || [], compose: wire.compose });
      wire.contacts = []; wire.compose = [];
    } else {
      if ((plain.compose || []).some(c => (c.environmentParameters || []).some(e => /^(DROP_CLUSTER_PASSPHRASE|DROP_STORAGE_API_KEYS_JSON|DROP_STORAGE_APPS_JSON|DROP_STORAGE_ADMIN_KEY)=/.test(e)))) throw new Error('Drop secrets require an Enterprise specification');
      wire.enterprise = '';
    }
    const verified = (await request(session.stickyBackend, `/apps/verifyapp${action === 'update' ? 'update' : 'registration'}specifications`, { session, body: wire, timeout: 120000 })).data;
    if (verified.name !== wire.name || verified.owner !== wire.owner || verified.expire !== wire.expire || (enterprise && (!verified.enterprise || verified.compose?.length))) throw new Error('Flux verification changed identity, expiry or Enterprise protection');
    const price = (await request(session.stickyBackend, '/apps/calculatefiatandfluxprice', { session, body: verified, timeout: 120000 })).data;
    if (![price.usd, price.flux].every(n => Number.isFinite(Number(n)) && Number(n) >= 0)) throw new Error('Invalid Flux price quote');
    const timestamp = Date.now(), message = signingMessage(action, verified, timestamp);
    const prepared = { action, appName: verified.name, spec: verified, timestamp, message, messageSHA256: crypto.createHash('sha256').update(message).digest('hex'), baseHash: original?.hash ?? null, baseHeight: original?.height ?? null, subscriptionExpiryHeight: original ? original.height + original.expire : null, price };
    await save(path.join(dir, 'proposed.decrypted.json'), plain);
    await save(path.join(dir, 'proposed.redacted.json'), redactSpec(plain));
    await save(path.join(dir, 'prepared.json'), prepared);
    await save(path.join(dir, 'message-to-sign.txt'), message);
    console.log(JSON.stringify({ preparedFile: path.join(dir, 'prepared.json'), messageFile: path.join(dir, 'message-to-sign.txt'), redactedReviewFile: path.join(dir, 'proposed.redacted.json'), price }, null, 2)); return;
  }
  if (command === 'submit') {
    const file = requireOption(o, 'prepared'), p = await read(file);
    if (p.spec.owner !== session.zelid || p.spec.name !== p.appName || p.message !== signingMessage(p.action, p.spec, p.timestamp) || crypto.createHash('sha256').update(p.message).digest('hex') !== p.messageSHA256) throw new Error('Prepared signing payload has changed');
    if (Date.now() - p.timestamp > 10 * 60 * 1000 || p.timestamp > Date.now()) throw new Error('Signing payload is stale; prepare and sign again');
    const receiptFile = path.join(path.dirname(file), 'submission.json');
    try { await fs.access(receiptFile); throw new Error('Submission already recorded; do not submit again'); } catch (err) { if (err.code !== 'ENOENT') throw err; }
    let current;
    if (p.action === 'update') {
      current = await fetchSpec(p.appName, session);
      if (current.hash !== p.baseHash || current.height !== p.baseHeight) throw new Error('App specification changed since preparation; prepare again');
    }
    await assertNoPending(p.appName, session, current);
    const signature = await secretText(requireOption(o, 'signature-file'));
    const txid = (await request(session.stickyBackend, `/apps/app${p.action}`, { session, body: { type: `fluxapp${p.action}`, version: 1, appSpecification: p.spec, timestamp: p.timestamp, signature }, timeout: 120000 })).data;
    // Save the accepted transaction before any follow-up which might fail.
    await save(receiptFile, { txid, appName: p.appName, action: p.action, submittedAt: new Date().toISOString(), price: p.price });
    console.log(JSON.stringify({ txid, receiptFile, price: p.price }, null, 2));
    if (Number(p.price.flux) > 0) {
      const payment = (await request(session.stickyBackend, '/apps/deploymentinformation', { session })).data;
      if (!payment.address) throw new Error('Submission accepted and recorded, but payment address lookup failed');
      console.log(JSON.stringify({ paymentAddress: payment.address, amountFlux: p.price.flux, memo: txid }, null, 2));
    }
    return;
  }
  throw new Error('Commands: login, authenticate, snapshot, prepare, submit. See docs/FLUX_APP_TOOL.md.');
}
if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(err => { console.error(err.name === 'Error' ? err.message : 'Operation failed; private response details withheld'); process.exitCode = 1; });
}

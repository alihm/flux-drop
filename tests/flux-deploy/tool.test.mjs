import test from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { CONTAINER_DATA, authHeader, stickyBackend, requestOptions, wireSpec, changeContainerData, remainingBlocks, signingMessage, redactSpec, encryptPayload, decryptPayload } from '../../scripts/flux-deploy.mjs';

const component = { name: 'storage', containerData: 'r:/data', cpu: 0.5, ram: 1000, hdd: 100, ports: [35447], containerPorts: [35447], commands: [], environmentParameters: ['DROP_ROLE=secondary', 'DROP_PRIMARY_APP_NAME=drop', 'DROP_STORAGE_API_KEYS_JSON={"v1":"private+secret=value"}'], repoauth: 'private-password' };
const spec = { version: 8, name: 'dropstoragea', owner: 'wallet', instances: 3, expire: 88000, height: 3000000, hash: 'original', datacenter: false, staticip: false, enterprise: '', contacts: ['private@example.com'], compose: [component], _wasEnterprise: true };

test('Flux request bodies retain nested objects, numeric types, booleans and literal JSON env', () => {
  const payload = { type: 'fluxappupdate', version: 1, appSpecification: wireSpec(spec), timestamp: 1791000000000, signature: 's+/=' };
  const req = requestOptions({ body: payload });
  assert.equal(req.headers['Content-Type'], 'application/x-www-form-urlencoded');
  assert.equal(req.method, 'POST');
  assert.deepEqual(JSON.parse(req.body), payload);
  assert.equal(typeof JSON.parse(req.body).appSpecification.compose[0].ports[0], 'number');
  assert.equal(typeof JSON.parse(req.body).appSpecification.staticip, 'boolean');
  assert.equal(requestOptions({}).body, undefined);
  assert.equal(requestOptions({}).method, 'GET');
});
test('auth and public key forms correctly escape base64 and use loginPhrase casing', () => {
  const session = { zelid: 'wallet', signature: 'a+/=&b', loginPhrase: 'phrase+&' };
  const parsed = Object.fromEntries(new URLSearchParams(authHeader(session)));
  assert.deepEqual(parsed, session);
  const body = new URLSearchParams({ name: 'dropstoragea', owner: 'wallet' }).toString();
  assert.equal(requestOptions({ body }).body, body);
  assert.equal(stickyBackend('node_1.2.3.4'), 'https://1-2-3-4-16127.node.api.runonflux.io');
  assert.throws(() => stickyBackend('1.2.3.999'));
});
test('Container Data change preserves every other original field and does not mutate input', () => {
  const changed = changeContainerData(spec);
  assert.equal(changed.compose[0].containerData, CONTAINER_DATA);
  const expected = structuredClone(spec); expected.compose[0].containerData = CONTAINER_DATA;
  assert.deepEqual(changed, expected);
  assert.equal(spec.compose[0].containerData, 'r:/data');
  assert.throws(() => changeContainerData({ ...spec, compose: [component, { ...component, name: 'other' }] }));
});
test('wire format strips chain-only metadata while retaining datacenter and numeric types', () => {
  const wire = wireSpec(spec);
  assert.equal(wire.height, undefined); assert.equal(wire.hash, undefined); assert.equal(wire._wasEnterprise, undefined);
  assert.equal(wire.datacenter, false); assert.equal(wire.compose[0].cpu, 0.5);
  assert.throws(() => wireSpec({ ...spec, version: 7 }));
  assert.equal(remainingBlocks(spec, spec.height + 300), 87700);
  assert.throws(() => remainingBlocks(spec, spec.height + spec.expire));
  assert.throws(() => remainingBlocks({ ...spec, height: '3000000' }, 3000001));
});
test('wallet message matches Flux concatenation byte for byte, without whitespace changes', () => {
  const wire = wireSpec(spec), timestamp = 1791000000000;
  assert.equal(signingMessage('update', wire, timestamp), 'fluxappupdate1' + JSON.stringify(wire) + timestamp);
  assert.equal(signingMessage('register', wire, timestamp), 'fluxappregister1' + JSON.stringify(wire) + timestamp);
  assert.throws(() => signingMessage('other', wire, timestamp));
});
test('public template removes unknown envs, contacts, repo credentials, and commands', () => {
  const redacted = redactSpec({ ...spec, compose: [{ ...component, commands: ['--password=private'] }] });
  const text = JSON.stringify(redacted);
  assert.ok(!text.includes('private+secret')); assert.ok(!text.includes('private-password')); assert.ok(!text.includes('private@example.com')); assert.ok(!text.includes('--password=private'));
  assert.ok(text.includes('DROP_ROLE=secondary')); assert.ok(text.includes('DROP_PRIMARY_APP_NAME=drop'));
});
test('Enterprise request uses RSA-OAEP SHA256 of base64 AES key and nonce/ciphertext/tag framing', () => {
  const { publicKey, privateKey } = crypto.generateKeyPairSync('rsa', { modulusLength: 2048 });
  const spki = publicKey.export({ type: 'spki', format: 'der' }).toString('base64');
  const payload = { compose: spec.compose, contacts: spec.contacts };
  const bytes = Buffer.from(encryptPayload(spki, payload), 'base64');
  const keyB64 = crypto.privateDecrypt({ key: privateKey, padding: crypto.constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256' }, bytes.subarray(0, 256));
  const key = Buffer.from(keyB64.toString(), 'base64');
  assert.equal(key.length, 32);
  assert.deepEqual(decryptPayload(bytes.subarray(256).toString('base64'), key), payload);
  bytes[bytes.length - 1] ^= 1;
  assert.throws(() => decryptPayload(bytes.subarray(256).toString('base64'), key));
});

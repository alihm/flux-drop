import {test, expect} from '@playwright/test';
import {randomBytes, createHash} from 'node:crypto';

async function authorize(request, name = 'My coding agent') {
  const registration = await request.post('/oauth/register', {data: {redirect_uris: ['http://127.0.0.1:9123/callback'], client_name: name, token_endpoint_auth_method: 'none'}});
  expect(registration.status()).toBe(201);
  const client = await registration.json();
  const verifier = randomBytes(32).toString('base64url');
  return '/oauth/authorize?' + new URLSearchParams({client_id: client.client_id, redirect_uri: 'http://127.0.0.1:12345/callback', response_type: 'code', state: 'browser-state', code_challenge: createHash('sha256').update(verifier).digest('base64url'), code_challenge_method: 'S256'});
}

test('consent shows permissions and both login options, with a bound secure cookie', async ({page, request, context}) => {
  const path = await authorize(request);
  const violations = [];
  await page.addInitScript(() => document.addEventListener('securitypolicyviolation', event => { window.consentViolations = [...(window.consentViolations || []), event.violatedDirective]; }));
  await page.goto(path);
  await expect(page.getByRole('heading', {name: 'Allow My coding agent to use Flux?'})).toBeVisible();
  await expect(page.getByText('This app named itself; the name is not verified.')).toBeVisible();
  await expect(page.getByText('An app on this computer;', {exact: false})).toBeVisible();
  await expect(page.getByText('It cannot pay for anything or see your password.')).toBeVisible();
  await expect(page.getByRole('button', {name: 'Continue with Google'})).toBeVisible();
  await expect(page.getByLabel('Email', {exact: true})).toBeVisible();
  await expect(page.getByLabel('Password', {exact: true})).toHaveAttribute('type', 'password');
  await expect(page.getByRole('button', {name: 'Allow', exact: true})).toBeDisabled();
  // This proves that the hashed script ran under the page's CSP.
  await page.getByRole('button', {name: 'Reset password'}).click();
  await expect(page.getByRole('status')).toContainText('Enter your email address first.');
  violations.push(...await page.evaluate(() => window.consentViolations || []));
  expect(violations).toEqual([]);
  const cookies = await context.cookies();
  const cookie = cookies.find(item => item.name === '__Host-drop-agent');
  expect(cookie).toMatchObject({secure: true, httpOnly: true, path: '/', sameSite: 'Lax'});
});

test('denial uses CSRF and cookie binding and returns issuer and state to the agent', async ({page, request}) => {
  const path = await authorize(request);
  await page.goto(path);
  let callback;
  await page.route('http://127.0.0.1:12345/callback?**', route => { callback = new URL(route.request().url()); return route.fulfill({contentType: 'text/html', body: '<title>Agent callback</title>'}); });
  const post = page.waitForResponse(response => response.url().endsWith('/oauth/authorize') && response.request().method() === 'POST');
  await page.getByRole('button', {name: 'Deny', exact: true}).click();
  expect((await post).status()).toBe(200);
  await expect(page).toHaveTitle('Agent callback');
  expect(callback.searchParams.get('error')).toBe('access_denied');
  expect(callback.searchParams.get('state')).toBe('browser-state');
  expect(callback.searchParams.get('iss')).toBe('https://localhost:18443');
});

test('client-controlled names render as text and never execute markup', async ({page, request}) => {
  const name = '<img src=x onerror="window.pwned=true">';
  await page.goto(await authorize(request, name));
  await expect(page.getByRole('heading', {name: `Allow ${name} to use Flux?`})).toBeVisible();
  expect(await page.locator('#consent img').count()).toBe(0);
  expect(await page.evaluate(() => window.pwned)).toBeUndefined();
});

import { test, expect } from '@playwright/test';

test('unlock UI bootstraps CSRF, submits privately and navigates to the fixed project path', async ({page}) => {
  await page.route('**/api/session', route => route.fulfill({json:{csrfToken:'test-csrf'}}));
  let submitted;
  await page.route('**/api/unlock', async route => {
    submitted = {headers:await route.request().allHeaders(),body:route.request().postDataJSON()};
    await route.fulfill({json:{unlocked:true}});
  });
  await page.route('**/example-abcdef/',route => route.fulfill({contentType:'text/html',body:'<h1>Unlocked fixture</h1>'}));
  const response = await page.goto('/unlock/example-abcdef?next=https://attacker.invalid');
  expect(response.headers()['content-security-policy']).not.toContain('unsafe-inline');
  await page.getByLabel('Project password').fill('a long secret password');
  await page.getByRole('button',{name:'Unlock project'}).click();
  await expect(page).toHaveURL('https://localhost:18443/example-abcdef/');
  expect(submitted.headers['x-csrf-token']).toBe('test-csrf');
  expect(submitted.headers.origin).toBe('https://localhost:18443');
  expect(submitted.body).toEqual({slug:'example-abcdef',password:'a long secret password'});
});

test('unlock UI clears passwords on denial and rate limiting without redirecting',async ({page}) => {
  await page.route('**/api/session',route => route.fulfill({json:{csrfToken:'test-csrf'}}));
  let code=403;
  await page.route('**/api/unlock',route => route.fulfill({status:code,json:{error:'denied'}}));
  await page.goto('/unlock/example-abcdef');
  for(const status of [403,429]) {
    code=status;
    await page.getByLabel('Project password').fill('a long secret password');
    await page.getByRole('button',{name:'Unlock project'}).click();
    await expect(page.getByRole('status')).toContainText(status===403?'Unable to unlock':'Too many attempts');
    await expect(page.getByLabel('Project password')).toHaveValue('');
    await expect(page.getByRole('button',{name:'Unlock project'})).toBeEnabled();
    await expect(page).toHaveURL('https://localhost:18443/unlock/example-abcdef');
  }
});

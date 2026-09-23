import {test,expect} from '@playwright/test';

test('cancelled Google popup preserves the anonymous session',async({page})=>{
  let exchanges=0;
  await page.addInitScript(()=>{window.DropAuth={init(){},async token(){throw Object.assign(new Error('cancelled'),{code:'auth/popup-closed-by-user'});}};});
  await page.route('**/api/config',r=>r.fulfill({json:{authenticationEnabled:true,publishingEnabled:false,firebase:{projectId:'demo-drop'},limits:{uploadBytes:52428800,files:5000}}}));
  await page.route('**/api/session',r=>r.fulfill({json:{csrfToken:'anonymous',authenticated:false}}));
  await page.route('**/api/auth/google',r=>{exchanges++;return r.fulfill({status:500});});
  await page.goto('/');await page.getByRole('button',{name:'Sign in with Google',exact:true}).click();
  await expect(page.locator('#auth-status')).toContainText('Sign-in cancelled');
  await expect(page.getByRole('button',{name:'Sign in with Google',exact:true})).toBeEnabled();
  await expect(page.getByRole('button',{name:'Sign out',exact:true})).toBeHidden();expect(exchanges).toBe(0);
});

test('Google exchange rotates CSRF before claiming and logout requires confirmation',async({page})=>{
  const id='a'.repeat(32);let claimed=false,logout=0;
  await page.addInitScript(()=>{window.DropAuth={init(){},async token(){return 'test-google-token';},async clear(){window.authCleared=true;}};});
  await page.route('**/api/config',r=>r.fulfill({json:{authenticationEnabled:true,publishingEnabled:true,firebase:{projectId:'demo-drop'},limits:{uploadBytes:52428800,files:5000}}}));
  await page.route('**/api/session',r=>r.fulfill({json:{csrfToken:'anonymous',authenticated:false}}));
  await page.route('**/api/projects',r=>r.fulfill({json:{projects:[{id,slug:'site-abcdef',revision:claimed?2:1,expiresAt:claimed?null:'2026-10-22T00:00:00Z'}]}}));
  await page.route('**/api/auth/google',async r=>{
    expect(r.request().postDataJSON()).toEqual({idToken:'test-google-token'});
    expect((await r.request().allHeaders())['x-csrf-token']).toBe('anonymous');
    await r.fulfill({json:{csrfToken:'rotated',authenticated:true}});
  });
  await page.route(`**/api/projects/${id}/claim`,async r=>{
    const h=await r.request().allHeaders();expect(h['x-csrf-token']).toBe('rotated');expect(h['if-match']).toBe('"1"');claimed=true;
    await r.fulfill({json:{project:{id,slug:'site-abcdef',revision:2}}});
  });
  await page.route('**/api/auth/logout',async r=>{logout++;expect((await r.request().allHeaders())['x-csrf-token']).toBe('rotated');await r.fulfill({json:{csrfToken:'new-anonymous',authenticated:false}});});
  await page.goto(`/?claim=${id}#projects`);
  await expect(page.getByRole('button',{name:'Keep this project'})).toBeVisible();expect(claimed).toBe(false);
  await page.getByRole('button',{name:'Keep this project'}).click();
  await expect(page.getByRole('button',{name:'Sign out',exact:true})).toBeVisible();
  await expect(page.getByRole('button',{name:'Keep this project'})).toHaveCount(0);expect(claimed).toBe(true);
  page.once('dialog',d=>d.dismiss());await page.getByRole('button',{name:'Sign out',exact:true}).click();expect(logout).toBe(0);
  page.once('dialog',d=>d.accept());await page.getByRole('button',{name:'Sign out',exact:true}).click();
  await expect(page.locator('#auth-status')).toContainText('new anonymous session');expect(logout).toBe(1);
  expect(await page.evaluate(()=>window.authCleared)).toBe(true);
});

test('server sign-out remains complete if Firebase client cleanup fails',async({page})=>{
  await page.addInitScript(()=>{window.DropAuth={init(){},async token(){return 'test-google-token';},async clear(){throw new Error('client cleanup failed');}};});
  await page.route('**/api/config',r=>r.fulfill({json:{authenticationEnabled:true,publishingEnabled:true,firebase:{projectId:'demo-drop'},limits:{uploadBytes:52428800,files:5000}}}));
  await page.route('**/api/session',r=>r.fulfill({json:{csrfToken:'anonymous',authenticated:false}}));
  await page.route('**/api/projects',r=>r.fulfill({json:{projects:[]}}));
  await page.route('**/api/auth/google',r=>r.fulfill({json:{csrfToken:'signed-in',authenticated:true}}));
  await page.route('**/api/auth/logout',r=>r.fulfill({json:{csrfToken:'signed-out',authenticated:false}}));
  await page.goto('/');
  await page.getByRole('button',{name:'Sign in with Google'}).click();
  await expect(page.getByRole('button',{name:'Sign out'})).toBeVisible();
  page.once('dialog',d=>d.accept());
  await page.getByRole('button',{name:'Sign out'}).click();
  await expect(page.getByRole('button',{name:'Sign in with Google'})).toBeVisible();
  await expect(page.locator('#auth-status')).toContainText('Signed out. A new anonymous session is active.');
});

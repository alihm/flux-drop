import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {test,expect} from '@playwright/test';

const css=readFileSync(new URL('../../../internal/httpserver/ui/home.css',import.meta.url),'utf8');
const js=readFileSync(new URL('../../../internal/httpserver/ui/home.js',import.meta.url),'utf8');
const html=readFileSync(new URL('../../../internal/httpserver/ui/home.html',import.meta.url),'utf8').replace('{{.CSS}}',()=>css).replace('{{.JS}}',()=>js);
const hash=value=>createHash('sha256').update(value).digest('base64');
test.beforeEach(async({page})=>{
  await page.route(url=>url.pathname==='/',route=>route.request().isNavigationRequest()?route.fulfill({contentType:'text/html',headers:{'content-security-policy':`default-src 'none'; script-src 'sha256-${hash(js)}'; style-src 'sha256-${hash(css)}'; connect-src 'self'; img-src data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'`},body:html}):route.fallback());
  await page.route('**/api/agent-keys',route=>route.fulfill({json:{keys:[]}}));
});


test('cancelled Google popup preserves the anonymous session',async({page})=>{
  let exchanges=0;
  await page.addInitScript(()=>{window.DropAuth={init(){},async token(){throw Object.assign(new Error('cancelled'),{code:'auth/popup-closed-by-user'});}};});
  await page.route('**/api/config',r=>r.fulfill({json:{authenticationEnabled:true,publishingEnabled:false,firebase:{projectId:'demo-drop'},limits:{uploadBytes:52428800,files:5000}}}));
  await page.route('**/api/session',r=>r.fulfill({json:{csrfToken:'anonymous',authenticated:false}}));
  await page.route('**/api/auth/google',r=>{exchanges++;return r.fulfill({status:500});});
  await page.goto('/');await page.getByRole('button',{name:'Sign in',exact:true}).click();await expect(page.getByRole('dialog',{name:'Sign in',exact:true})).toBeVisible();expect(exchanges).toBe(0);await page.getByRole('button',{name:'Continue with Google',exact:true}).click();
  await expect(page.locator('#auth-status')).toContainText('Sign-in cancelled');
  await expect(page.getByRole('button',{name:'Continue with Google',exact:true})).toBeEnabled();await page.getByRole('button',{name:'Close sign-in',exact:true}).click();await expect(page.getByRole('button',{name:'Sign in',exact:true})).toBeEnabled();
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
  await expect(page.getByRole('button',{name:'Keep this site'})).toBeVisible();expect(claimed).toBe(false);
  await page.getByRole('button',{name:'Keep this site'}).click();await expect(page.getByRole('dialog',{name:'Sign in',exact:true})).toBeVisible();expect(claimed).toBe(false);await page.getByRole('button',{name:'Continue with Google',exact:true}).click();
  await expect(page.getByRole('button',{name:'Sign out',exact:true})).toBeVisible();
  await expect(page.getByRole('button',{name:'Keep this site'})).toHaveCount(0);expect(claimed).toBe(true);
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
  await page.getByRole('button',{name:'Sign in',exact:true}).click();await page.getByRole('button',{name:'Continue with Google',exact:true}).click();
  await expect(page.getByRole('button',{name:'Sign out'})).toBeVisible();
  page.once('dialog',d=>d.accept());
  await page.getByRole('button',{name:'Sign out'}).click();
  await expect(page.getByRole('button',{name:'Sign in'})).toBeVisible();
  await expect(page.locator('#auth-status')).toContainText('Signed out. A new anonymous session is active.');
});

test('Claim it opens sign-in without invoking Google and resumes after sign-in',async({page})=>{
  await page.setViewportSize({width:390,height:844});
  const id='c'.repeat(32);let claimed=false,exchanges=0;
  await page.addInitScript(()=>{window.providerCalls=0;window.DropAuth={init(){},async token(){window.providerCalls++;return 'claim-google-token';}}});
  await page.route('**/api/config',r=>r.fulfill({json:{authenticationEnabled:true,publishingEnabled:true,firebase:{projectId:'demo-drop'},limits:{uploadBytes:52428800,files:5000}}}));
  await page.route('**/api/session',r=>r.fulfill({json:{csrfToken:'anonymous',authenticated:false}}));
  await page.route('**/api/projects',r=>r.fulfill({json:{projects:[]}}));
  await page.route('**/api/projects?name=*',r=>r.fulfill({json:{path:'/claim-demo-abcdef/',project:{id,slug:'claim-demo-abcdef',revision:1,expiresAt:new Date(Date.now()+7*24*3600000).toISOString()}}}));
  await page.route('**/api/auth/google',r=>{exchanges++;return r.fulfill({json:{csrfToken:'claimed-session',authenticated:true}})});
  await page.route(`**/api/projects/${id}/claim`,async r=>{expect((await r.request().allHeaders())['x-csrf-token']).toBe('claimed-session');claimed=true;await r.fulfill({json:{project:{id,slug:'claim-demo-abcdef',revision:2}}})});
  await page.goto('/');await page.locator('#files').setInputFiles({name:'index.html',mimeType:'text/html',buffer:Buffer.from('<h1>Claim me</h1>')});await page.getByRole('button',{name:'Publish site'}).click();
  await expect(page.locator('#claim-result')).toHaveText('Claim it');await expect(page.locator('#expiry')).toContainText('Sign in to claim it');await expect(page.locator('#expiry')).not.toContainText('with Google');
  await page.locator('#claim-result').click();await expect(page.getByRole('dialog',{name:'Sign in',exact:true})).toBeVisible();expect(await page.evaluate(()=>window.providerCalls)).toBe(0);expect(exchanges).toBe(0);expect(claimed).toBe(false);
  await page.keyboard.press('Escape');await expect(page.getByRole('dialog',{name:'Sign in',exact:true})).toBeHidden();expect(claimed).toBe(false);
  await page.locator('#claim-result').click();await page.getByRole('button',{name:'Continue with Google',exact:true}).click();await expect(page.locator('#claim-callout')).toBeHidden();expect(claimed).toBe(true);expect(exchanges).toBe(1);expect(await page.evaluate(()=>window.providerCalls)).toBe(1);
});

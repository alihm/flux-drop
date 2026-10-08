import {test,expect} from '@playwright/test';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';

const html=readFileSync(new URL('../../../internal/admin/page.html',import.meta.url),'utf8');
const hash=tag=>createHash('sha256').update(html.split(`<${tag}>`)[1].split(`</${tag}>`)[0]).digest('base64');
const csp=`default-src 'none'; script-src 'sha256-${hash('script')}'; style-src 'sha256-${hash('style')}'; connect-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'`;
const wallet='15c3aH6y9Koq1Dg1rGXE9Ypn5nL2AbSJCu';
const session={csrfToken:'test-csrf'};
const GiB=1024**3;
function fixture(){return {apps:[{appName:'storagea',discoveryFresh:true,nodes:[1,2,3].map(i=>({address:`192.0.2.${i}:36447`,healthy:true,capacity:{capacityBytes:100*GiB,availableBytes:90*GiB,reservedBytes:0,writable:true}}))},{appName:'storageb',discoveryFresh:false,nodes:[]}],allocations:{storagea:{allocatedBytes:10*GiB}},controls:{},headroomBytes:GiB}}
async function pageRoute(page){await page.route('**/admin/',r=>r.fulfill({contentType:'text/html',headers:{'content-security-policy':csp},body:html}))}

test('admin shows logical capacity once and searches apps and replicas',async({page})=>{
 const errors=[];page.on('pageerror',e=>errors.push(e.message));await pageRoute(page);
 await page.route('**/admin/api/session',r=>r.fulfill({json:session}));await page.route('**/admin/api/apps',r=>r.fulfill({json:fixture()}));
 await page.goto('/admin/');await expect(page.locator('#metric-capacity')).toHaveText('100 GiB');await expect(page.locator('#metric-free')).toHaveText('89 GiB');await expect(page.locator('#metric-apps')).toHaveText('2');await expect(page.locator('#metric-allocated')).toHaveText('10 GiB');await expect(page.locator('#apps .app')).toHaveCount(2);
 await expect(page.locator('#apps .app').first().locator('.action-help')).toContainText('Stop new uploads');await expect(page.locator('#apps .app').first().locator('.action-help')).toContainText('Flux deployment and files stay intact');
 await page.getByRole('searchbox').fill('192.0.2.2');await expect(page.locator('#apps .app')).toHaveCount(1);await expect(page.locator('#apps')).toContainText('storagea');
 await page.getByRole('searchbox').fill('missing');await expect(page.locator('#apps')).toContainText('No apps match');
 await page.getByRole('searchbox').fill('storageb');await expect(page.locator('#apps')).toContainText('Unknown');await expect(page.locator('#apps')).toContainText('Discovery stale');expect(errors).toEqual([]);
});

test('admin projects are searchable with storage placement and owner details',async({page})=>{
 await pageRoute(page);await page.route('**/admin/api/session',r=>r.fulfill({json:session}));await page.route('**/admin/api/apps',r=>r.fulfill({json:fixture()}));
 await page.route('**/admin/api/projects?*',r=>{const q=new URL(r.request().url()).searchParams.get('q');return r.fulfill({json:{projects:q==='missing'?[]:[{id:'a'.repeat(32),slug:'demo-abcdef',ownerId:'account-demo',ownerKind:'firebase',storageApp:'storagea',bytes:1024,status:'active',private:true,revision:3,createdAt:'2026-10-08T00:00:00Z'}],nextCursor:''}})});
 await page.goto('/admin/');await page.getByRole('button',{name:'Projects',exact:true}).click();await expect(page.getByRole('heading',{name:'User projects',exact:true}).first()).toBeVisible();await expect(page.locator('#project-list')).toContainText('demo-abcdef');await expect(page.locator('#project-list')).toContainText('storagea');await expect(page.locator('#project-list')).toContainText('Private');
 await page.getByText('Project details',{exact:true}).click();await expect(page.locator('#project-list')).toContainText('account-demo');await expect(page.getByRole('link',{name:'Open site'})).toHaveAttribute('href','/demo-abcdef/');
 await page.getByRole('searchbox',{name:'Search user projects'}).fill('missing');await expect(page.locator('#project-list')).toContainText('No projects found');
});
test('admin confirms removal, preserves errors and sends CSRF',async({page})=>{
 await page.setViewportSize({width:390,height:844});await pageRoute(page);
 await page.route('**/admin/api/session',r=>r.fulfill({json:session}));await page.route('**/admin/api/apps',r=>r.fulfill({json:fixture()}));let actions=[];
 await page.route('**/admin/api/apps/storagea',async r=>{actions.push(r.request().postDataJSON());expect((await r.request().allHeaders())['x-csrf-token']).toBe('test-csrf');await r.fulfill({status:409,json:{error:'app_in_use',message:'This app has retained data. Drain it instead.'}})});
 await page.goto('/admin/');await expect(page.locator('#apps .app')).toHaveCount(2);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 await page.locator('#apps .app').first().getByRole('button',{name:'Remove',exact:true}).click();await expect(page.getByRole('dialog')).toBeVisible();await page.getByRole('button',{name:'Cancel',exact:true}).click();expect(actions).toEqual([]);
 await page.locator('#apps .app').first().getByRole('button',{name:'Remove',exact:true}).click();await page.getByRole('dialog').getByRole('button',{name:'Remove',exact:true}).click();await expect(page.getByRole('alert')).toContainText('retained data');expect(actions).toEqual([{action:'remove'}]);
});
test('admin login hides dashboard and wallet and accepts manual signatures',async({page})=>{
 await pageRoute(page);await page.route('**/admin/api/session',r=>r.fulfill({status:401,json:{error:'unauthorized'}}));
 await page.route('**/admin/api/challenge',r=>r.fulfill({json:{id:'0'.repeat(64),pollToken:'1'.repeat(64),message:'Flux Drop Admin login test',expiresAt:new Date(Date.now()+300000).toISOString()}}));
 await page.route('**/admin/api/login',r=>{expect(r.request().postDataJSON()).toEqual({id:'0'.repeat(64),signature:'manual-test-signature',pollToken:'1'.repeat(64)});return r.fulfill({json:{status:'authenticated',session}})});
 await page.route('**/admin/api/logout',r=>r.fulfill({json:{ok:true}}));
 await page.route('**/admin/api/apps',r=>r.fulfill({json:fixture()}));await page.goto('/admin/');await expect(page.getByRole('heading',{name:'Admin login'})).toBeVisible();await expect(page.locator('.sidebar')).toBeHidden();await expect(page.getByRole('heading',{name:'Storage overview'})).toBeHidden();expect(await page.locator('body').innerText()).not.toContain(wallet);await page.getByText('Sign the message manually',{exact:true}).click();await expect(page.getByLabel('Login message')).toHaveValue('Flux Drop Admin login test');await page.getByLabel('Signature from Zelcore').fill('manual-test-signature');await page.getByRole('button',{name:'Sign in',exact:true}).click();await expect(page.locator('#dashboard')).toBeVisible();await expect(page.locator('.sidebar')).toBeVisible();
 await page.getByRole('button',{name:'Sign out',exact:true}).click();await expect(page.getByRole('heading',{name:'Admin login'})).toBeVisible();await expect(page.locator('.sidebar')).toBeHidden();await expect(page.getByRole('heading',{name:'Storage overview'})).toBeHidden();
});

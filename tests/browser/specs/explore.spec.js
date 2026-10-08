import {test,expect} from '@playwright/test';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
const css=readFileSync(new URL('../../../internal/httpserver/ui/home.css',import.meta.url),'utf8');
const js=readFileSync(new URL('../../../internal/httpserver/ui/home.js',import.meta.url),'utf8');
const html=readFileSync(new URL('../../../internal/httpserver/ui/home.html',import.meta.url),'utf8').replace('{{.CSS}}',()=>css).replace('{{.JS}}',()=>js);
const hash=value=>createHash('sha256').update(value).digest('base64');
const csp=`default-src 'none'; script-src 'sha256-${hash(js)}'; style-src 'sha256-${hash(css)}'; connect-src 'self'; img-src 'self' data: blob:; frame-ancestors 'none'; base-uri 'none'; form-action 'none'`;
const jpeg=readFileSync(new URL('../fixtures/preview.jpg',import.meta.url));
function projects(){return ['quiet-studio','weekend-notes','tiny-garden','color-atlas','open-journal','weather-window'].map((name,i)=>({id:String(i+1).repeat(32),slug:name+'-abcdef',claimed:true,digest:'a'.repeat(64),updatedAt:new Date(Date.now()-i*3600000).toISOString(),thumbnail:'/api/projects/'+String(i+1).repeat(32)+'/thumbnail?v='+'a'.repeat(64)}))}
async function setup(page,rows=projects()){
 await page.route(url=>url.pathname==='/',r=>r.fulfill({contentType:'text/html',headers:{'content-security-policy':csp},body:html}));
 await page.route('**/api/config',r=>r.fulfill({json:{authenticationEnabled:false,publishingEnabled:false,exploreEnabled:true,limits:{uploadBytes:52428800,files:5000}}}));
 await page.route('**/api/explore',r=>r.fulfill({json:{projects:rows}}));
 await page.route('**/thumbnail?*',r=>r.fulfill({contentType:'image/svg+xml',headers:{'X-Drop-Preview':'pending'},body:'<svg xmlns="http://www.w3.org/2000/svg"/>'}));
}
test('Explore is available without signing in, filters private cards and refreshes',async({page})=>{
 const errors=[];page.on('pageerror',e=>errors.push(e.message));
 const rows=projects();rows.push({...rows[0],id:'b'.repeat(32),slug:'private-example-abcdef',private:true});rows.push({...rows[0],id:'c'.repeat(32),slug:'unclaimed-example-abcdef',claimed:false});rows.push({...rows[0],slug:'<script>alert(1)</script>'});
 await setup(page,rows);await page.goto('/');
 await expect(page.getByRole('heading',{name:'See what others deployed'})).toBeVisible();await expect(page.locator('.explore-card')).toHaveCount(6);
 await expect(page.locator('#explore')).not.toContainText('private example');await expect(page.locator('#explore')).not.toContainText('unclaimed example');await expect(page.locator('.explore-card').first()).toHaveAttribute('href','/quiet-studio-abcdef/');await expect(page.locator('.explore-card').first()).toHaveAttribute('rel','noopener noreferrer');
 await page.route('**/api/explore',r=>r.fulfill({json:{projects:[]}}));await page.locator('#refresh-explore').click();await expect(page.locator('.explore-card')).toHaveCount(0);await expect(page.locator('#explore-status')).toContainText('Publish a public site');expect(errors).toEqual([]);
});
test('Explore grid fits desktop and mobile with graceful pending previews',async({page})=>{
 await setup(page);await page.setViewportSize({width:1440,height:1000});await page.goto('/');await page.locator('#explore').scrollIntoViewIfNeeded();await expect(page.locator('.explore-card')).toHaveCount(6);
 const columns=()=>page.locator('#explore-list').evaluate(node=>getComputedStyle(node).gridTemplateColumns.split(' ').length);
 expect(await columns()).toBe(3);await expect(page.locator('.thumbnail-label').first()).toHaveText('Preview preparing');
 if(process.env.DROP_EXPLORE_SCREENSHOTS)await page.screenshot({path:'/tmp/drop-explore-desktop-'+test.info().project.name+'.png'});
 await page.setViewportSize({width:390,height:844});await page.locator('#explore').scrollIntoViewIfNeeded();expect(await columns()).toBe(1);expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 if(process.env.DROP_EXPLORE_SCREENSHOTS)await page.screenshot({path:'/tmp/drop-explore-mobile-'+test.info().project.name+'.png'});
});
test('Explore preserves cards and offers retry if refreshing fails',async({page})=>{
 await setup(page);await page.goto('/');await expect(page.locator('.explore-card')).toHaveCount(6);await page.route('**/api/explore',r=>r.fulfill({status:503}));await page.locator('#refresh-explore').click();await expect(page.locator('#explore-status')).toContainText('Could not load');await expect(page.locator('.explore-card')).toHaveCount(6);await expect(page.locator('#refresh-explore')).toBeEnabled();
});
test('Explore loads generated JPEGs through the real page image policy',async({page})=>{
 const errors=[];page.on('pageerror',e=>errors.push(e.message));await setup(page);
 await page.route('**/thumbnail?*',r=>r.fulfill({contentType:'image/jpeg',headers:{'X-Drop-Preview':'ready'},body:jpeg}));
 await page.goto('/');await page.locator('#explore').scrollIntoViewIfNeeded();await expect(page.locator('.explore-card').first().locator('img')).toBeVisible();
 await expect.poll(()=>page.locator('.explore-card').first().locator('img').evaluate(img=>img.complete&&img.naturalWidth===320&&img.naturalHeight===180)).toBe(true);
 await expect(page.locator('.explore-card').first().locator('.thumbnail-label')).toHaveCount(0);expect(errors).toEqual([]);
 if(process.env.DROP_EXPLORE_SCREENSHOTS)await page.screenshot({path:'/tmp/drop-explore-ready-'+test.info().project.name+'.png'});
});

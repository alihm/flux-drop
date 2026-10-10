import {test,expect} from '@playwright/test';
import {readFileSync} from 'node:fs';
const workspace='https://runonflux.com/apps/drop';
const jpeg=readFileSync(new URL('../fixtures/preview.jpg',import.meta.url));
async function setup(page,{explore=false,fail=false}={}){
 await page.route('**/api/config',route=>fail?route.fulfill({status:503}):route.fulfill({json:{authenticationEnabled:true,publishingEnabled:true,firebase:{projectId:'fluxcore-prod'},exploreEnabled:explore,limits:{uploadBytes:52428800,files:5000}}}));
}
test('public landing sends visitors to Flux Apps without auth, session or upload controls',async({page})=>{
 const errors=[],requests=[];page.on('pageerror',e=>errors.push(e.message));page.on('request',r=>requests.push(r));
 await setup(page);await page.goto('/landing');
 await expect(page.locator('#start-publishing')).toBeVisible();
 await expect(page.locator('#start-publishing')).toHaveAttribute('href',workspace);
 await expect(page.locator('header').getByRole('link',{name:'Open Drop'})).toHaveAttribute('href',workspace);
 await expect(page.locator('footer').getByRole('link',{name:'Publish a site'})).toHaveAttribute('href',workspace);
 await expect(page.locator('#sign-in,#publish-panel,#projects,#manage-dialog,input[type=file]')).toHaveCount(0);
 await expect.poll(()=>requests.filter(r=>new URL(r.url()).pathname==='/api/config').length).toBe(1);
 expect(requests.filter(r=>r.method()!=='GET'||/\/api\/(session|auth|projects)/.test(new URL(r.url()).pathname))).toHaveLength(0);
 expect(await page.evaluate(()=>typeof window.DropAuth)).toBe('undefined');
 await page.getByRole('button',{name:/Switch to .* theme/}).click();
 await expect(page.locator('html')).toHaveAttribute('data-theme',/light|dark/);
 for(const width of [1440,390,320]){
  await page.setViewportSize({width,height:900});
  await expect(page.locator('#start-publishing')).toBeVisible();
  expect(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth)).toBe(true);
 }
 if(process.env.DROP_LANDING_SCREENSHOTS)await page.screenshot({path:'/tmp/drop-landing-'+test.info().project.name+'.png',fullPage:true});
 expect(errors).toEqual([]);
});
test('landing public gallery keeps previews and filters private and unclaimed sites',async({page})=>{
 const errors=[];page.on('pageerror',e=>errors.push(e.message));await setup(page,{explore:true});
 const site={id:'a'.repeat(32),slug:'quiet-studio',claimed:true,digest:'b'.repeat(64),updatedAt:new Date().toISOString()};
 await page.route('**/api/explore',r=>r.fulfill({json:{projects:[site,{...site,private:true},{...site,claimed:false},{...site,slug:'//evil.example'}]}}));
 await page.route('**/thumbnail?*',r=>r.fulfill({contentType:'image/jpeg',headers:{'X-Drop-Preview':'ready'},body:jpeg}));
 await page.goto('/landing');await page.locator('#explore').scrollIntoViewIfNeeded();
 await expect(page.locator('.explore-card')).toHaveCount(1);await expect(page.locator('.explore-card')).toHaveAttribute('href','/quiet-studio/');
 await page.locator('.explore-card').scrollIntoViewIfNeeded();await expect(page.locator('.explore-card img')).toBeVisible();
 await expect.poll(()=>page.locator('.explore-card img').evaluate(img=>img.complete&&img.naturalWidth===320)).toBe(true);
 await page.route('**/api/explore',r=>r.fulfill({status:503}));await page.locator('#refresh-explore').click();
 await expect(page.locator('#explore-status')).toContainText('Could not load');await expect(page.locator('.explore-card')).toHaveCount(1);
 expect(errors).toEqual([]);
});
test('workspace CTA works without the API and carries existing claim links',async({page})=>{
 await setup(page,{fail:true});
 const token='a'.repeat(32)+'.'+'b'.repeat(43);
 await page.goto('/landing#claim-token='+token);
 await expect(page.locator('#start-publishing')).toHaveAttribute('href',workspace+'#claim-token='+token);
 await expect(page.locator('#start-publishing')).toContainText('claim this site');expect(new URL(page.url()).hash).toBe('');
 await expect(page.getByRole('heading',{name:'Common questions'})).toBeAttached();
});

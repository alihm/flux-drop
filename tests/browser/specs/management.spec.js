import {test,expect} from '@playwright/test';

const id='a'.repeat(32);
async function setup(page) {
  await page.route('**/api/config',r=>r.fulfill({json:{publishingEnabled:true,authenticationEnabled:true,limits:{uploadBytes:52428800,files:5000}}}));
  await page.route('**/api/session',r=>r.fulfill({json:{csrfToken:'manage-csrf'}}));
  const state={project:{id,slug:'site-abcdef',initialSuffix:'abcdef',revision:1,private:false,expiresAt:'2026-10-22T00:00:00Z'}};
  await page.route('**/api/projects',r=>r.fulfill({json:{projects:state.project?[state.project]:[],nextCursor:''}}));
  return state;
}
async function open(page) {await page.goto('/');await page.getByRole('button',{name:'Manage',exact:true}).click();await expect(page.locator('#manage-dialog')).toBeVisible();}

test('rename uses the current revision and stops stale mutations',async({page})=>{
  const state=await setup(page);let calls=0;
  await page.route(`**/api/projects/${id}`,async route=>{
    const req=route.request();expect(req.method()).toBe('PATCH');expect(req.postDataJSON()).toEqual({name:'renamed'});
    const headers=await req.allHeaders();expect(headers['if-match']).toBe('"1"');expect(headers['x-csrf-token']).toBe('manage-csrf');calls++;
    state.project.revision=2;await route.fulfill({status:409,json:{error:'revision_conflict'}});
  });
  await open(page);await page.getByRole('button',{name:'Settings',exact:true}).click();await page.getByLabel('New site name').fill('renamed');await page.getByRole('button',{name:'Rename',exact:true}).click();
  await expect(page.locator('.management-status')).toContainText('refresh your sites');
  await expect(page.getByRole('button',{name:'Rename',exact:true})).toBeDisabled();expect(calls).toBe(1);
  await page.getByRole('button',{name:'Close site settings'}).click();await page.getByRole('button',{name:'Refresh',exact:true}).click();await page.getByRole('button',{name:'Manage',exact:true}).click();await page.getByRole('button',{name:'Settings',exact:true}).click();
  await expect(page.getByRole('button',{name:'Rename',exact:true})).toBeEnabled();
});

test('privacy controls submit the password without rendering it and delete requires exact confirmation',async({page})=>{
  // This flow opens several animated panels. WebKit on shared CI runners can
  // need more than the default 20 seconds; retain the normal assertion limits.
  test.setTimeout(45_000);
  const state=await setup(page);let deletions=0;
  await page.route(`**/api/projects/${id}/privacy`,async route=>{
    expect(route.request().postDataJSON()).toEqual({private:true,password:'a long private password'});
    state.project={...state.project,private:true,revision:2};await route.fulfill({json:{project:state.project}});
  });
  await page.route(`**/api/projects/${id}`,async route=>{
    expect(route.request().method()).toBe('DELETE');expect((await route.request().allHeaders())['if-match']).toBe('"2"');deletions++;state.project=null;await route.fulfill({status:204});
  });
  await open(page);await page.getByRole('button',{name:'Access',exact:true}).click();await page.getByLabel('Password for private access').fill('a long private password');await page.getByRole('button',{name:'Make private',exact:true}).click();
  await expect(page.locator('.project-card')).toContainText('Private');await page.getByRole('button',{name:'Manage',exact:true}).click();await page.getByRole('button',{name:'Access',exact:true}).click();
  await expect(page.getByLabel('New site password')).toHaveValue('');
  await page.getByRole('button',{name:'Settings',exact:true}).click();
  await page.getByRole('button',{name:'Delete site',exact:true}).click();await expect(page.locator('.management-status')).toContainText('exact site URL name');expect(deletions).toBe(0);
  await page.getByLabel('Type the full site URL name to confirm').fill('site-abcdef');await page.getByRole('button',{name:'Delete site',exact:true}).click();
  await expect(page.locator('#projects')).toBeHidden();expect(deletions).toBe(1);
});

test('content replacement keeps its idempotency key across an unchanged retry',async({page})=>{
  const state=await setup(page);let key,calls=0;
  page.on('dialog',dialog=>dialog.accept());
  await page.route(`**/api/projects/${id}/versions`,async route=>{
    const headers=await route.request().allHeaders();expect(headers['if-match']).toBe('"1"');expect(headers['x-csrf-token']).toBe('manage-csrf');
    if(!key)key=headers['idempotency-key'];else expect(headers['idempotency-key']).toBe(key);
    expect(route.request().postData()).toContain('index.html');calls++;
    if(calls===1)await route.fulfill({status:503,json:{error:'unavailable'}});
    else{state.project.revision=2;await route.fulfill({json:{project:state.project}});}
  });
  await open(page);await page.getByLabel('Replacement HTML, ZIP, or files').setInputFiles({name:'index.html',mimeType:'text/html',buffer:Buffer.from('<h1>Updated</h1>')});
  await page.getByRole('button',{name:'Replace content',exact:true}).click();await expect(page.locator('.management-status')).toContainText('result could not be confirmed');
  await page.getByRole('button',{name:'Replace content',exact:true}).click();await expect(page.locator('#manage-dialog')).toBeHidden();expect(calls).toBe(2);
});

test('content replacement displays file validation diagnostics', async ({page}) => {
  await setup(page);
  page.on('dialog', dialog => dialog.accept());
  const files = ['assets/app.js.map', '.env', 'CNAME'];
  const message = 'invalid static upload: ' + files.map(name => `unsupported file: "${name}"`).join('; ');
  await page.route(`**/api/projects/${id}/versions`, route => route.fulfill({status: 400, json: {error: 'invalid_project', message, files}}));
  await open(page);
  await page.getByLabel('Replacement HTML, ZIP, or files').setInputFiles({name: 'site.zip', mimeType: 'application/zip', buffer: Buffer.from('ZIP fixture')});
  await page.getByRole('button', {name: 'Replace content', exact: true}).click();
  await expect(page.locator('.management-status')).toHaveText(message);
  await expect(page.getByRole('button', {name: 'Replace content', exact: true})).toBeEnabled();
});

 test('clean names and watermark preference work in Manage',async({page})=>{
  const state=await setup(page);state.project.slug='site-abcdef';state.project.initialSuffix='';
  let calls=0;
  await page.route(`**/api/projects/${id}/watermark`,async route=>{
    expect(route.request().method()).toBe('PUT');expect(route.request().postDataJSON()).toEqual({enabled:false});
    const headers=await route.request().allHeaders();expect(headers['if-match']).toBe('"1"');expect(headers['x-csrf-token']).toBe('manage-csrf');
    calls++;state.project.watermarkDisabled=true;state.project.revision++;
    await route.fulfill({json:{project:state.project}});
  });
  await open(page);await page.getByRole('button',{name:'Settings',exact:true}).click();
  await expect(page.getByLabel('New site name')).toHaveValue('site-abcdef');
  await expect(page.getByLabel('Show Powered by RunOnFlux')).toBeChecked();
  await page.getByLabel('Show Powered by RunOnFlux').uncheck();
  await page.getByRole('button',{name:'Save watermark setting',exact:true}).click();
  await expect(page.locator('#manage-dialog')).toBeHidden();expect(calls).toBe(1);
  await page.getByRole('button',{name:'Manage',exact:true}).click();await page.getByRole('button',{name:'Settings',exact:true}).click();
  await expect(page.getByLabel('Show Powered by RunOnFlux')).not.toBeChecked();
 });

test('page views load only when opened, show a chart and switch to hourly counts', async ({page}) => {
  await setup(page);
  await page.route('**/api/config', route => route.fulfill({json: {publishingEnabled: true, authenticationEnabled: true, analyticsEnabled: true, limits: {uploadBytes: 52428800, files: 5000}}}));
  let requests = 0;
  await page.route(`**/api/projects/${id}/analytics?*`, async route => {
    requests++;
    const query = new URL(route.request().url()).searchParams;
    expect(query.get('interval')).toBe(requests === 1 ? 'day' : 'hour');
    await route.fulfill({json: {projectId: id, timezone: 'UTC', approximate: true, pageViews: 42, updatedAt: '2026-10-10T12:00:00Z', buckets: [{start: '2026-10-10T12:00:00Z', pageViews: 42}]}});
  });
  await open(page); expect(requests).toBe(0);
  await page.getByRole('button', {name: 'Page views', exact: true}).click();
  await expect(page.locator('.analytics-total')).toHaveText('42 page views');
  await expect(page.getByRole('img', {name: 'Daily page views'})).toBeVisible();
  await page.getByLabel('Page-view period').selectOption('24h');
  await expect(page.getByRole('img', {name: 'Hourly page views'})).toBeVisible();
  await page.getByText('View counts by hour', {exact: true}).click();
  await expect(page.locator('.analytics-table')).toContainText('2026-10-10 12:00');
  expect(requests).toBe(2);
});

test('page-view failures can be retried without blocking management', async ({page}) => {
  await setup(page);
  await page.route('**/api/config', route => route.fulfill({json: {publishingEnabled: true, authenticationEnabled: true, analyticsEnabled: true, limits: {uploadBytes: 52428800, files: 5000}}}));
  let requests = 0;
  await page.route(`**/api/projects/${id}/analytics?*`, route => {
    requests++;
    return requests === 1 ? route.fulfill({status: 503, json: {error: 'analytics_unavailable'}}) : route.fulfill({json: {pageViews: 0, updatedAt: null, buckets: []}});
  });
  await open(page); await page.getByRole('button', {name: 'Page views', exact: true}).click();
  await expect(page.locator('.analytics-status')).toContainText('temporarily unavailable');
  await page.getByRole('button', {name: 'Refresh page views', exact: true}).click();
  await expect(page.locator('.analytics-total')).toHaveText('0 page views');
  await expect(page.locator('.analytics-result')).toContainText('No page views recorded');
  await page.getByRole('button', {name: 'Settings', exact: true}).click();
  await expect(page.getByLabel('New site name')).toBeVisible();
});

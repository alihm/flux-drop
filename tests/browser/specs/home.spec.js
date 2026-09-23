import {test,expect} from '@playwright/test';

async function setup(page,enabled=true) {
  await page.route('**/api/config',route=>route.fulfill({json:{publishingEnabled:enabled,authenticationEnabled:enabled,limits:{uploadBytes:52428800,files:5000}}}));
  await page.route('**/api/session',route=>route.fulfill({json:{csrfToken:'home-csrf'}}));
  await page.route('**/api/projects',route=>route.fulfill({json:{projects:[],nextCursor:''}}));
}
test('landing disables publishing honestly when unavailable',async({page})=>{
  await setup(page,false);await page.goto('/');
  await expect(page.getByRole('heading',{name:'From files to a live URL.'})).toBeVisible();
  await expect(page.locator('#status')).toContainText('unavailable');
  await page.locator('#files').setInputFiles({name:'index.html',mimeType:'text/html',buffer:Buffer.from('<h1>Hello</h1>')});
  await expect(page.locator('#selection')).toContainText('1 file');
  await expect(page.getByRole('button',{name:'Publish project'})).toBeDisabled();
});
test('publishing uses CSRF and a stable retry key and renders a safe success link',async({page})=>{
  await setup(page);let firstKey,attempts=0;
  await page.route('**/api/projects?name=*',async route=>{
    const headers=await route.request().allHeaders();expect(headers['x-csrf-token']).toBe('home-csrf');
    const key=headers['idempotency-key'];expect(key).toBeTruthy();
    if(!firstKey)firstKey=key;else expect(key).toBe(firstKey);
    expect(route.request().postData()).toContain('index.html');
    attempts++;
    await route.fulfill(attempts===1?{status:503,json:{error:'unavailable'}}:{json:{path:'/my-site-abcdef/',project:{expiresAt:'2026-10-22T00:00:00Z'}}});
  });
  await page.goto('/');await expect(page.locator('#status')).toContainText('Choose a site');
  await page.locator('#files').setInputFiles({name:'index.html',mimeType:'text/html',buffer:Buffer.from('<h1>Hello</h1>')});
  await page.getByLabel('Project name').fill('my-site');
  await page.getByRole('button',{name:'Publish project'}).click();
  await expect(page.locator('#status')).toContainText('could not be confirmed');
  await page.getByRole('button',{name:'Publish project'}).click();
  await expect(page.locator('#project-link')).toHaveAttribute('href','/my-site-abcdef/');
  await expect(page.locator('#status')).toContainText('Published successfully');
  await expect(page.getByRole('button',{name:'Publish project'})).toBeDisabled();
  await page.getByRole('button',{name:'Publish another'}).click();
  await expect(page.locator('#selection')).toContainText('No files selected');
});

test('empty state and session failure explain why publishing is disabled',async({page})=>{
  await setup(page);
  await page.route('**/api/session',route=>route.fulfill({status:503,json:{error:'unavailable'}}));
  await page.goto('/');
  await expect(page.locator('#projects')).toBeHidden();
  await expect(page.locator('#projects-nav')).toBeHidden();
  await page.locator('#files').setInputFiles({name:'index.html',mimeType:'text/html',buffer:Buffer.from('<h1>Test</h1>')});
  await expect(page.locator('#status')).toContainText('session is unavailable');
  await expect(page.getByRole('button',{name:'Publish project'})).toBeDisabled();
});

test('selection validates the site entry point before uploading',async({page})=>{
  await setup(page);await page.goto('/');
  await page.locator('#files').setInputFiles({name:'style.css',mimeType:'text/css',buffer:Buffer.from('body{}')});
  await expect(page.locator('#status')).toContainText('index.html');
  await expect(page.getByRole('button',{name:'Publish project'})).toBeDisabled();
  await page.getByRole('button',{name:'Clear'}).click();
  await expect(page.locator('#selection')).toContainText('No files selected');
});

test('folder paths survive upload and duplicate content shows the existing link',async({page})=>{
  await setup(page);
  await page.route('**/api/projects?name=*',async route=>{
    expect(route.request().postData()).toContain('site/assets/style.css');
    expect(route.request().postData()).toContain('site/index.html');
    await route.fulfill({status:409,json:{error:'duplicate_content',path:'/existing-abcdef/'}});
  });
  await page.goto('/');await expect(page.locator('#status')).toContainText('Choose a site');
  await page.locator('#folder').evaluate(input=>{
    const transfer=new DataTransfer();
    for(const [name,path,body] of [['index.html','site/index.html','<h1>Hello</h1>'],['style.css','site/assets/style.css','body{}']]){
      const file=new File([body],name);Object.defineProperty(file,'webkitRelativePath',{value:path});transfer.items.add(file);
    }
    input.files=transfer.files;input.dispatchEvent(new Event('change',{bubbles:true}));
  });
  await expect(page.locator('#selection')).toContainText('2 files');
  await page.getByRole('button',{name:'Publish project'}).click();
  await expect(page.locator('#project-link')).toHaveAttribute('href','/existing-abcdef/');
  await expect(page.locator('#status')).toContainText('No duplicate project was created');
});

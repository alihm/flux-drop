import {test, expect} from '@playwright/test';

test('published HTML keeps its standards mode, functionality and a small fixed badge', async ({page}) => {
  await page.goto('/watermark-demo/');
  await expect(page.getByRole('heading', {name:'My deployed site'})).toBeVisible();
  const badge = page.locator('[data-drop-watermark="runonflux"]');
  await expect(badge).toBeVisible();
  await expect(badge).toHaveText('Powered by RunOnFlux');
  expect(await page.evaluate(() => document.compatMode)).toBe('CSS1Compat');
  expect(await badge.evaluate(element => {
    const style=getComputedStyle(element);
    return [style.position,style.fontSize,style.pointerEvents];
  })).toEqual(['fixed','11px','none']);
  await page.getByRole('button',{name:'Try it'}).click();
  await expect(page.getByRole('button',{name:'Works'})).toBeVisible();
  await page.setViewportSize({width:390,height:844});
  const box=await badge.boundingBox();
  expect(box.width).toBeLessThan(200);
  expect(box.x+box.width).toBeLessThanOrEqual(390);
  expect(box.y+box.height).toBeLessThanOrEqual(844);
});

test('disabled watermark returns the original document',async({page,request})=>{
  await page.goto('/watermark-disabled/');
  await expect(page.getByRole('heading',{name:'My deployed site'})).toBeVisible();
  await expect(page.locator('[data-drop-watermark]')).toHaveCount(0);
  const get=await request.get('/watermark-demo/');
  const head=await request.head('/watermark-demo/');
  expect(get.status()).toBe(200);expect(head.status()).toBe(200);
  expect(head.headers()['content-length']).toBe(String((await get.body()).length));
  expect((await head.body()).length).toBe(0);
});

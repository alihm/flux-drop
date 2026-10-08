import {chromium} from 'playwright-core';
let text='';for await(const chunk of process.stdin){text+=chunk;if(text.length>4096)throw new Error('Invalid preview job');}
const {Source:source,Slug:slug}=JSON.parse(text);
const origin='http://preview.invalid';
const base=new URL('/'+slug+'/',origin);
const sourceURL=new URL(source);
if(sourceURL.hostname!=='127.0.0.1'||sourceURL.protocol!=='http:'||!/^[a-z0-9-]+$/.test(slug))throw new Error('Invalid preview source');
const browser=await chromium.launch({executablePath:'/usr/local/bin/drop-browser',headless:true,timeout:10000,args:['--disable-dev-shm-usage','--disable-background-networking','--disable-crash-reporter','--disable-breakpad','--disable-gpu','--disable-software-rasterizer','--single-process','--in-process-gpu','--use-gl=disabled','--no-zygote','--disable-features=MediaRouter,Vulkan','--force-webrtc-ip-handling-policy=disable_non_proxied_udp']});
try{
 const context=await browser.newContext({viewport:{width:1280,height:720},deviceScaleFactor:0.25,serviceWorkers:'block',acceptDownloads:false});
 await context.routeWebSocket('**/*',socket=>socket.close());
 let requests=0,total=0;
 await context.route('**/*',async route=>{
  const request=route.request();const url=new URL(request.url());
  if(request.method()!=='GET'||url.origin!==origin||!url.pathname.startsWith(base.pathname)||++requests>80){await route.abort();return;}
  if(request.isNavigationRequest()&&request.frame()!==context.pages()[0]?.mainFrame()){await route.abort();return;}
  let path=url.pathname.slice(base.pathname.length);if(!path||path.endsWith('/'))path+='index.html';
  if(path.includes('%')||path.includes('\\')||path.split('/').some(part=>!part||part==='.'||part==='..')){await route.abort();return;}
  try{
   const response=await fetch(new URL(path,sourceURL),{redirect:'error',signal:AbortSignal.timeout(10000)});
   if(!response.ok){await route.abort();return;}
   const chunks=[];for await(const chunk of response.body){total+=chunk.length;if(total>32*1024*1024)throw new Error('Preview asset limit');chunks.push(chunk);}
   const headers={'content-type':response.headers.get('content-type')||'application/octet-stream'};
   if(request.isNavigationRequest())headers['content-security-policy']="sandbox allow-scripts; default-src 'self' data: blob:; script-src 'self' 'unsafe-inline' 'unsafe-eval'; style-src 'self' 'unsafe-inline'; connect-src 'self'; object-src 'none'; frame-src 'none'; base-uri 'none'; form-action 'none'";
   await route.fulfill({status:200,headers,body:Buffer.concat(chunks)});
  }catch{await route.abort();}
 });
 const page=await context.newPage();page.on('dialog',dialog=>dialog.dismiss());
 page.on('popup',popup=>popup.close());
 const response=await page.goto(base.href,{waitUntil:'load',timeout:15000});
 if(!response||!response.ok())throw new Error('Preview document unavailable');
 await page.waitForTimeout(700);
 const raw=await page.screenshot({type:'jpeg',quality:68,fullPage:false,animations:'disabled',timeout:5000});
 if(raw.length>64*1024)throw new Error('Preview image limit');
 process.stdout.write(raw);
}finally{await browser.close();}

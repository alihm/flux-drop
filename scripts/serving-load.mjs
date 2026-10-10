// Read-only, closed-loop load probe. Explicit paths avoid warming the measured data.
// node scripts/serving-load.mjs ORIGIN [SITE=/] [REQUESTS=200] [CONCURRENCY=50]
// DROP_LOAD_PATHS=/tmp/paths.json DROP_LOAD_TIMEOUT_MS=150000 DROP_LOAD_SLOW_MS=0
import http from 'node:http';
import https from 'node:https';
import {readFile} from 'node:fs/promises';
import {performance} from 'node:perf_hooks';
const [origin, site='/', count='200', concurrency='50']=process.argv.slice(2);
const requests=Number(count), workers=Number(concurrency);
const timeout=Number(process.env.DROP_LOAD_TIMEOUT_MS||150000);
const slow=Number(process.env.DROP_LOAD_SLOW_MS||0);
if(!origin||![requests,workers,timeout].every(n=>Number.isSafeInteger(n)&&n>0)||!Number.isSafeInteger(slow)||slow<0)throw Error('Invalid load configuration');
const base=new URL(origin), client=base.protocol==='https:'?https:http;
const agent=new client.Agent({keepAlive:true,maxSockets:workers});
function get(path,discovery=false){return new Promise(resolve=>{
 const start=performance.now();let bytes=0,body='',status='error',timing,settled=false;
 const finish=error=>{if(settled)return;settled=true;resolve({ms:performance.now()-start,status:error?'error':status,bytes,body,timing,error:error?.message});};
 const target=new URL(path,base);if(target.origin!==base.origin){finish(Error('Cross-origin path refused'));return;}
 const req=client.get(target,{agent},res=>{status=res.statusCode;timing=res.headers['server-timing'];res.on('data',chunk=>{bytes+=chunk.length;if(discovery&&bytes<=2**20)body+=chunk;if(slow){res.pause();setTimeout(()=>res.resume(),slow);}});res.on('end',()=>finish());res.on('error',finish);res.on('aborted',()=>finish(Error('aborted')));});
 const timer=setTimeout(()=>req.destroy(Error('request deadline')),timeout);req.on('close',()=>clearTimeout(timer));req.on('error',finish);
});}
try{
 let paths;
 if(process.env.DROP_LOAD_PATHS){paths=JSON.parse(await readFile(process.env.DROP_LOAD_PATHS,'utf8'));}
 else{
  const explore=await get('/api/explore',true);const cards=JSON.parse(explore.body).projects||[];
  paths=['/api/config','/api/explore',...cards.slice(0,20).map(p=>p.thumbnail),site];
  const html=await get(site,true);for(const m of html.body.matchAll(/(?:src|href)=["']([^"'#]+)["']/g)){const u=new URL(m[1],new URL(site,base));if(u.origin===base.origin&&u.pathname.startsWith(site)&&!paths.includes(u.pathname))paths.push(u.pathname);}
 }
 if(!Array.isArray(paths)||!paths.length||!paths.every(p=>typeof p==='string'&&p.startsWith('/')&&!p.startsWith('//')))throw Error('Expected nonempty array of same-origin paths');
 const results=[];let next=0;const start=performance.now();
 await Promise.all(Array.from({length:workers},async()=>{while(next<requests){const path=paths[next++%paths.length];results.push({path,...await get(path)});}}));
 const seconds=(performance.now()-start)/1000;
 const quantile=(values,p)=>values.slice().sort((a,b)=>a-b)[Math.min(values.length-1,Math.floor(values.length*p))];
 for(const [name,rows] of [['all',results],...['config','explore','thumbnail','files'].map(name=>[name,results.filter(r=>name==='files'?!r.path.startsWith('/api/'):r.path.includes(name))])]){const times=rows.map(r=>r.ms);console.log(JSON.stringify({name,n:rows.length,elapsedSeconds:seconds,totalRps:rows.length/seconds,successfulRps:rows.filter(r=>r.status>=200&&r.status<400).length/seconds,bytes:rows.reduce((n,r)=>n+r.bytes,0),p50:quantile(times,.5),p95:quantile(times,.95),p99:quantile(times,.99),statuses:rows.reduce((m,r)=>(m[r.status]=(m[r.status]||0)+1,m),{}),transportErrors:rows.filter(r=>r.error).length,serverTiming:rows.find(r=>r.timing)?.timing}));}
}finally{agent.destroy();}

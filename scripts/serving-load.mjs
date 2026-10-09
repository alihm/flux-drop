// Read-only load probe. Usage: node scripts/serving-load.mjs ORIGIN [SITE_PATH] [REQUESTS=200] [CONCURRENCY=50]
import http from 'node:http';
import https from 'node:https';
import {performance} from 'node:perf_hooks';
const [origin, site='/', count='200', concurrency='50']=process.argv.slice(2);
if(!origin)throw Error('An explicit origin is required');
const base=new URL(origin), client=base.protocol==='https:'?https:http;
const agent=new client.Agent({keepAlive:true,maxSockets:Number(concurrency)});
function get(path){return new Promise((resolve,reject)=>{const start=performance.now();const req=client.get(new URL(path,base),{agent,timeout:15000},res=>{let body='';res.on('data',chunk=>body+=chunk);res.on('end',()=>resolve({ms:performance.now()-start,status:res.statusCode,timing:res.headers['server-timing'],body}));});req.on('error',reject);req.on('timeout',()=>req.destroy(Error('timeout')));});}
try{
 const explore=await get('/api/explore');const cards=JSON.parse(explore.body).projects||[];
 const paths=['/api/config','/api/explore',...cards.slice(0,20).map(p=>p.thumbnail),site];
 const html=await get(site);for(const m of html.body.matchAll(/(?:src|href)=["']([^"'#]+)["']/g)){const u=new URL(m[1],new URL(site,base));if(u.origin===base.origin&&u.pathname.startsWith(site)&&!paths.includes(u.pathname))paths.push(u.pathname);}
 // Warm connections before the measured run; elapsed still includes server queue time.
 await Promise.all(Array.from({length:Number(concurrency)},()=>get('/api/config')));
 const results=[];let next=0;
 await Promise.all(Array.from({length:Number(concurrency)},async()=>{while(next<Number(count)){const i=next++,path=paths[i%paths.length];try{results.push({path,...await get(path)});}catch(e){results.push({path,ms:15000,status:'error',error:e.message});}}}));
 const quantile=(values,p)=>values.slice().sort((a,b)=>a-b)[Math.min(values.length-1,Math.floor(values.length*p))]?.toFixed(2);
 for(const [name,rows] of [['all',results],...['config','explore','thumbnail','files'].map(name=>[name,results.filter(r=>name==='files'?!r.path.startsWith('/api/'):r.path.includes(name))])]){const times=rows.map(r=>r.ms);console.log(JSON.stringify({name,n:rows.length,p50:quantile(times,.5),p95:quantile(times,.95),p99:quantile(times,.99),statuses:rows.reduce((m,r)=>(m[r.status]=(m[r.status]||0)+1,m),{}),serverTiming:rows.find(r=>r.timing)?.timing}));}
}finally{agent.destroy();}

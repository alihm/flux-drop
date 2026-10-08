import {createServer} from 'node:http';
import {mkdtemp,rm,writeFile} from 'node:fs/promises';
import {spawn,spawnSync} from 'node:child_process';
import assert from 'node:assert/strict';
const job=await mkdtemp('/tmp/drop-browser-');
let forbidden=0,css=0;
const server=createServer((req,res)=>{
 if(req.url==='/cap/index.html'){
  res.setHeader('Content-Type','text/html');res.end(`<link rel="stylesheet" href="style.css"><main><h1 id="title">Before scripts</h1><p>Thumbnail rendering</p></main><script>document.getElementById('title').textContent='Scripts rendered';fetch('http://127.0.0.1:${server.address().port}/secrets').catch(()=>{});fetch('file:///var/lib/drop-cluster/secret').catch(()=>{});new WebSocket('ws://127.0.0.1:${server.address().port}/socket')</script>`);
 }else if(req.url==='/cap/style.css'){css++;res.setHeader('Content-Type','text/css');res.end('body{background:#0b121c;color:#6fe3c8;font:80px sans-serif;margin:0}main{padding:80px}h1{font-size:90px}p{color:white;font-size:30px}')}
 else{forbidden++;res.writeHead(403);res.end('secret')}
});
await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));
try{
 const verified=spawnSync('/usr/local/bin/drop-browser',['--verify-sandbox'],{env:{PATH:'/usr/local/bin:/usr/bin:/bin',HOME:job,TMPDIR:job,DROP_BROWSER_JOB:job}});
 assert.equal(verified.status,0,verified.stderr.toString());console.log(verified.stdout.toString().trim());
 const child=spawn('node',['/opt/drop-preview/render.mjs'],{env:{PATH:'/usr/local/bin:/usr/bin:/bin',HOME:job,TMPDIR:job,DROP_BROWSER_JOB:job},detached:true});
 child.stdin.end(JSON.stringify({Source:`http://127.0.0.1:${server.address().port}/cap/`,Slug:'sample-abcdef'}));
 const out=[],err=[];child.stdout.on('data',chunk=>out.push(chunk));child.stderr.on('data',chunk=>err.push(chunk));
 const timer=setTimeout(()=>{try{process.kill(-child.pid,'SIGKILL')}catch{}},35000);
 const code=await new Promise(resolve=>child.on('exit',resolve));clearTimeout(timer);try{process.kill(-child.pid,'SIGKILL')}catch{}
 assert.equal(code,0,Buffer.concat(err).toString());
 const raw=Buffer.concat(out);assert.equal(raw[0],255);assert.equal(raw[1],216);assert.ok(raw.length<64*1024);assert.ok(css>0,'Uploaded CSS did not render');assert.equal(forbidden,0,'Page reached a protected or external route');
 await writeFile(process.env.PREVIEW_SMOKE_OUTPUT||'/tmp/preview-smoke.jpg',raw);
 console.log(`PASS: JPEG ${raw.length} bytes, uploaded CSS, blocked external/credential routes`);
}finally{server.closeAllConnections();server.close();await rm(job,{recursive:true,force:true});}

import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import http from 'node:http';

// Isolated demo-project emulators only. Never accepts a remote endpoint.
const base='http://127.0.0.1:18088';
const auth='http://127.0.0.1:19099';
const authorization='Basic '+Buffer.from('tester:isolated-test-password-not-for-deployment').toString('base64');
const cookies=new Map();let csrf;
async function call(path,{method='GET',body,headers={},status=200,anonymous=false}={}){
  const all={Origin:'https://drop.app.runonflux.io',...(!anonymous?{Authorization:authorization,Cookie:[...cookies].map(([k,v])=>`${k}=${v}`).join('; ')}:{}),...headers};
  if(csrf && !anonymous)all['X-CSRF-Token']=csrf;
  if(body && typeof body==='object'){all['Content-Type']='application/json';body=JSON.stringify(body);}
  // Node fetch overwrites Sec-Fetch-Mode; raw HTTP preserves our simulated
  // navigation metadata. Browser isolation is covered by the Playwright suite.
  const {r,text}=await new Promise((resolve,reject)=>{
    const request=http.request(base+path,{method,headers:all,timeout:20000},response=>{
      let text='';response.setEncoding('utf8');response.on('data',chunk=>text+=chunk);
      response.on('end',()=>{const headers=new Headers();for(let i=0;i<response.rawHeaders.length;i+=2)headers.append(response.rawHeaders[i],response.rawHeaders[i+1]);resolve({r:{status:response.statusCode,headers},text})});
    });request.on('error',reject);request.on('timeout',()=>request.destroy(Error('request timeout')));request.end(body);
  });
  assert.equal(r.status,status,`${method} ${path}: ${text}`);
  for(const cookie of r.headers.getSetCookie()){
    assert.match(cookie,/HttpOnly/i);assert.match(cookie,/Secure/i);
    const [name,value]=cookie.split(';')[0].split('=');cookies.set(name,value);
  }
  let data;try{data=JSON.parse(text)}catch{data=text};return {r,data};
}
async function ready(){
  for(let i=0;i<90;i++){try{if((await fetch(base+'/readyz',{signal:AbortSignal.timeout(1000)})).status===200)return}catch{};await new Promise(r=>setTimeout(r,1000))}
  throw Error('staging container did not become ready');
}
await ready();
await call('/api/config',{anonymous:true,status:401});
assert.equal((await call('/api/config')).data.publishingEnabled,true);
csrf=(await call('/api/session',{method:'POST'})).data.csrfToken;
const oldCookie=cookies.get('__Host-drop-session');
const key=randomUUID();
const html='<h1>Final image integration '+key+'</h1>';
let p=(await call('/api/projects?name=staging',{method:'POST',body:html,headers:{'Content-Type':'text/html','Idempotency-Key':key}})).data.project;
const path='/'+p.slug+'/';
let served=await call(path,{anonymous:true});assert.equal(served.data,html);assert.match(served.r.headers.get('content-security-policy'),/sandbox allow-scripts/);assert.doesNotMatch(served.r.headers.get('content-security-policy'),/allow-same-origin/);
await call('/api/projects/'+p.id,{method:'DELETE',headers:{Origin:'null','If-Match':`"${p.revision}"`},status:403});
const duplicate=await call('/api/projects?name=duplicate',{method:'POST',body:html,headers:{'Content-Type':'text/html','Idempotency-Key':randomUUID()},status:409});assert.equal(duplicate.data.path,path);
const revised=html+'updated';
p=(await call('/api/projects/'+p.id+'/versions',{method:'POST',body:revised,headers:{'Content-Type':'text/html','Idempotency-Key':randomUUID(),'If-Match':`"${p.revision}"`}})).data.project;
assert.equal('/'+p.slug+'/',path);assert.equal((await call(path,{anonymous:true})).data,revised);
p=(await call('/api/projects/'+p.id,{method:'PATCH',body:{name:'renamed'},headers:{'If-Match':`"${p.revision}"`}})).data.project;
await call(path,{anonymous:true,status:307});
const current='/'+p.slug+'/';
// Obtain a Google-provider ID token from the actual Firebase Auth emulator.
const b64=v=>Buffer.from(JSON.stringify(v)).toString('base64url');
const googleToken=b64({alg:'none',typ:'JWT'})+'.'+b64({sub:key,email:`${key}@example.test`,email_verified:true,name:'Staging test',iss:'https://accounts.google.com',aud:'test',iat:Math.floor(Date.now()/1000),exp:Math.floor(Date.now()/1000)+3600})+'.';
const tokenResponse=await fetch(auth+'/identitytoolkit.googleapis.com/v1/accounts:signInWithIdp?key=test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({requestUri:'http://localhost',postBody:new URLSearchParams({providerId:'google.com',id_token:googleToken}).toString(),returnSecureToken:true})});
const identity=await tokenResponse.json();assert.ok(identity.idToken,JSON.stringify(identity));
const login=(await call('/api/auth/google',{method:'POST',body:{idToken:identity.idToken}})).data;assert.equal(login.authenticated,true);csrf=login.csrfToken;assert.notEqual(cookies.get('__Host-drop-session'),oldCookie);
p=(await call('/api/projects/'+p.id+'/claim',{method:'POST',body:{},headers:{'If-Match':`"${p.revision}"`}})).data.project;assert.equal(p.expiresAt,null);
p=(await call('/api/projects/'+p.id+'/privacy',{method:'PUT',body:{private:true,password:'a very long staging password'},headers:{'If-Match':`"${p.revision}"`}})).data.project;
await call(current,{anonymous:true,status:404});
await call(current,{headers:{'Sec-Fetch-Mode':'navigate','Sec-Fetch-Dest':'document'},status:303});
await call('/api/unlock',{method:'POST',body:{slug:p.slug,password:'a very long staging password'}});
assert.equal((await call(current,{headers:{'Sec-Fetch-Mode':'navigate','Sec-Fetch-Dest':'document'}})).data,revised);
await call(current,{status:404});
p=(await call('/api/projects/'+p.id+'/privacy',{method:'PUT',body:{private:false},headers:{'If-Match':`"${p.revision}"`}})).data.project;
execFileSync('docker',['compose','-f','tests/staging/compose.yaml','restart','drop'],{stdio:'inherit'});
await ready();assert.equal((await call(current,{anonymous:true})).data,revised);
assert.equal((await call('/api/projects/'+p.id)).data.project.id,p.id);
await call('/api/projects/'+p.id,{method:'DELETE',headers:{'If-Match':`"${p.revision}"`},status:204});
await call(current,{anonymous:true,status:404});
const logout=(await call('/api/auth/logout',{method:'POST',body:{}})).data;assert.equal(logout.authenticated,false);
execFileSync('docker',['compose','-f','tests/staging/compose.yaml','stop','firestore'],{stdio:'inherit'});
try {
  let unhealthy=false;
  for(let i=0;i<20;i++){if((await fetch(base+'/readyz',{signal:AbortSignal.timeout(8000)})).status===503){unhealthy=true;break};await new Promise(r=>setTimeout(r,1000))}
  assert.ok(unhealthy,'readiness stayed healthy during Firestore outage');
  assert.equal((await fetch(base+'/healthz')).status,200);
} finally {execFileSync('docker',['compose','-f','tests/staging/compose.yaml','start','firestore'],{stdio:'inherit'});}
await ready();
// Nginx failure must stop the whole application rather than leave a half-live container.
execFileSync('docker',['compose','-f','tests/staging/compose.yaml','exec','-T','drop','nginx','-s','quit'],{stdio:'inherit'});
let stopped=false;
for(let i=0;i<25;i++){
  const running=execFileSync('docker',['compose','-f','tests/staging/compose.yaml','ps','--status','running','--services'],{encoding:'utf8'}).trim().split('\n');
  if(!running.includes('drop')){stopped=true;break};await new Promise(r=>setTimeout(r,1000));
}
assert.ok(stopped,'supervisor did not stop after Nginx exited');
execFileSync('docker',['compose','-f','tests/staging/compose.yaml','start','drop'],{stdio:'inherit'});await ready();
console.log('PASS: final-image upload, dedup, update, rename, Auth-emulator Google exchange, claim, privacy/unlock, restart persistence, delete, logout');
console.log('PASS: readiness fails during metadata outage; supervisor stops on Nginx exit and restarts cleanly');

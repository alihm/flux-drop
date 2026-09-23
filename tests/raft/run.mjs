import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {execFileSync} from 'node:child_process';
import http from 'node:http';

// Isolated three-node Raft containers only; no Firebase admin credentials/emulators.
const bases={one:'http://172.29.188.11:8080',two:'http://172.29.188.12:8080',three:'http://172.29.188.13:8080'};
let base=bases.one;
const compose=(...args)=>execFileSync('docker',['compose','-f','tests/raft/compose.yaml',...args],{encoding:'utf8'});
const authorization='Basic '+Buffer.from('tester:isolated-raft-test-password-not-for-deployment').toString('base64');
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

for(const value of Object.values(bases)){base=value;await ready()}
base=bases.one;
csrf=(await call('/api/session',{method:'POST'})).data.csrfToken;
const key=randomUUID(),html='<h1>Raft final image '+key+'</h1>';
let p=(await call('/api/projects?name=raft',{method:'POST',body:html,headers:{'Content-Type':'text/html','Idempotency-Key':key}})).data.project;
let path='/'+p.slug+'/';
for(const value of Object.values(bases)){base=value;assert.equal((await call('/api/projects/'+p.id)).data.project.id,p.id);assert.equal((await call(path,{anonymous:true})).data,html)}
const duplicate=await call('/api/projects?name=duplicate',{method:'POST',body:html,headers:{'Content-Type':'text/html','Idempotency-Key':randomUUID()},status:409});
assert.equal(duplicate.data.path,path);
const revised=html+' updated';
p=(await call('/api/projects/'+p.id+'/versions',{method:'POST',body:revised,headers:{'Content-Type':'text/html','Idempotency-Key':randomUUID(),'If-Match':'"'+p.revision+'"'}})).data.project;
assert.equal('/'+p.slug+'/',path);
p=(await call('/api/projects/'+p.id,{method:'PATCH',body:{name:'renamed'},headers:{'If-Match':'"'+p.revision+'"'}})).data.project;
await call(path,{anonymous:true,status:307});path='/'+p.slug+'/';
p=(await call('/api/projects/'+p.id+'/privacy',{method:'PUT',body:{private:true,password:'a very long raft password'},headers:{'If-Match':'"'+p.revision+'"'}})).data.project;
await call(path,{anonymous:true,status:404});
await call('/api/unlock',{method:'POST',body:{slug:p.slug,password:'a very long raft password'}});
const navigation={'Sec-Fetch-Mode':'navigate','Sec-Fetch-Dest':'document'};
assert.equal((await call(path,{headers:navigation})).data,revised);
const local=JSON.parse(compose('exec','-T','one','/usr/local/bin/drop-cluster','-status'));
assert.ok(Object.hasOwn(bases,local.leaderID),'unknown leader');
compose('stop',local.leaderID);
const survivors=Object.keys(bases).filter(id=>id!==local.leaderID);
base=bases[survivors[0]];await ready();
assert.equal((await call(path,{headers:navigation})).data,revised);
p=(await call('/api/projects/'+p.id+'/privacy',{method:'PUT',body:{private:true,password:'a replacement raft password'},headers:{'If-Match':'"'+p.revision+'"'}})).data.project;
await call(path,{headers:navigation,status:303});
await call('/api/unlock',{method:'POST',body:{slug:p.slug,password:'a replacement raft password'}});
assert.equal((await call(path,{headers:navigation})).data,revised);
compose('stop',survivors[1]);
for(let i=0;i<30;i++){
 if((await fetch(base+'/readyz',{signal:AbortSignal.timeout(10000)})).status===503)break;
 if(i===29)throw Error('minority remained ready');await new Promise(r=>setTimeout(r,500));
}
const denied=await fetch(base+path,{headers:{Authorization:authorization,Cookie:[...cookies].map(([k,v])=>k+'='+v).join('; ')},redirect:'manual',signal:AbortSignal.timeout(10000)});
assert.notEqual(denied.status,200,'minority served private content');
assert.equal((await fetch(base+'/healthz')).status,200);
compose('up','-d','--no-deps','--no-recreate',local.leaderID,survivors[1]);
for(const value of Object.values(bases)){base=value;await ready()}
assert.equal((await call('/api/projects/'+p.id)).data.project.id,p.id);
assert.equal((await call(path,{headers:navigation})).data,revised);
compose('restart','--no-deps','one','two','three');
for(const value of Object.values(bases)){base=value;await ready()}
assert.equal((await call(path,{headers:navigation})).data,revised);
await call('/api/projects/'+p.id,{method:'DELETE',headers:{'If-Match':'"'+p.revision+'"'},status:204});
await call(path,{anonymous:true,status:404});
const oldCookie=cookies.get('__Host-drop-session');
csrf=(await call('/api/auth/logout',{method:'POST',body:{}})).data.csrfToken;
assert.notEqual(cookies.get('__Host-drop-session'),oldCookie);
for(const value of Object.values(bases)){base=value;await call('/api/projects',{headers:{Cookie:'__Host-drop-session='+oldCookie},status:401})}
console.log('PASS: credential-free final-image publishing, cross-node sessions, dedup/update/rename, privacy/unlock/revocation, leader loss, quorum-loss denial, full restart, deletion and logout');

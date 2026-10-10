import assert from 'node:assert/strict';
import {createHash, randomBytes, randomUUID} from 'node:crypto';
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
  if(status!==null)assert.equal(r.status,status,`${method} ${path}: ${text}`);
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
function contentWithoutWatermark(body){
  // Public serving adds the default watermark; content bytes remain unchanged.
  assert.match(body,/<a data-drop-watermark="runonflux"[^>]+href="https:\/\/runonflux.com\/apps\/drop"/);
  return body.replace(/<a data-drop-watermark="runonflux"[^>]*>[\s\S]*?<\/a>/,'');
}

for(const value of Object.values(bases)){base=value;await ready()}
// Exercise the production image across HTTP instances, including initialization
// of the replicated private key in this deployment without a passphrase.
for(const value of Object.values(bases)){
  base=value;
  const discovery=(await call('/.well-known/oauth-authorization-server')).data;
  assert.equal(discovery.issuer,'https://drop.app.runonflux.io');
  const challenge=await call('/agent/mcp',{anonymous:true,status:401});
  assert.equal(challenge.r.headers.get('www-authenticate'),'Bearer resource_metadata="https://runonflux.com/apps/.well-known/oauth-protected-resource", scope="orbit drop"');
}
base=bases.one;
const client=(await call('/oauth/register',{method:'POST',status:201,body:{client_name:'Raft OAuth integration',redirect_uris:['http://127.0.0.1:32111/callback'],token_endpoint_auth_method:'none'}})).data;
const verifier=randomBytes(32).toString('base64url');
const params=new URLSearchParams({response_type:'code',client_id:client.client_id,redirect_uri:'http://127.0.0.1:32112/callback',code_challenge:createHash('sha256').update(verifier).digest('base64url'),code_challenge_method:'S256',state:'raft-oauth-test'});
base=bases.two;
const consent=await call('/oauth/authorize?'+params);
assert.match(consent.data,/Raft OAuth integration/);
assert.match(consent.r.headers.get('content-security-policy'),/frame-ancestors 'none'/);
const handle=consent.data.match(/data-handle="([^"]+)"/)[1],consentCSRF=consent.data.match(/data-csrf="([^"]+)"/)[1];
base=bases.three;
const callback=new URL((await call('/oauth/authorize',{method:'POST',body:{handle,csrf:consentCSRF,action:'deny'}})).data.redirect);
assert.equal(callback.searchParams.get('error'),'access_denied');
assert.equal(callback.searchParams.get('state'),'raft-oauth-test');
assert.equal(callback.searchParams.get('iss'),'https://drop.app.runonflux.io');
assert.equal(callback.port,'32112');
console.log('PASS: image OAuth discovery/MCP challenge; registration on one, consent on two, denial on three; loopback port exception');
// Serving deliberately uses local applied state. A healthy fixture converges,
// but production has no maximum delay. Poll only site responses, never management
// mutations; this also waits for reserved/pending local records to become active.
async function serving(path,options={},body){
 const expected=options.status??200;let response;
 for(const deadline=Date.now()+20000;;){
  response=await call(path,{...options,status:null});
  if(response.r.status===expected&&(body===undefined||contentWithoutWatermark(response.data)===body))return response;
  if(Date.now()>=deadline)break;await new Promise(r=>setTimeout(r,100));
 }
 assert.equal(response.r.status,expected,`serving did not converge: ${path}`);
 assert.equal(contentWithoutWatermark(response.data),body,`serving bytes did not converge: ${path}`);return response;
}
base=bases.one;
csrf=(await call('/api/session',{method:'POST'})).data.csrfToken;
const key=randomUUID(),html='<h1>Raft final image '+key+'</h1>';
let p=(await call('/api/projects?name=raft',{method:'POST',body:html,headers:{'Content-Type':'text/html','Idempotency-Key':key}})).data.project;
let path='/'+p.slug+'/';
for(const value of Object.values(bases)){base=value;assert.equal((await call('/api/projects/'+p.id)).data.project.id,p.id);assert.equal(contentWithoutWatermark((await serving(path,{anonymous:true},html)).data),html)}
const duplicate=await call('/api/projects?name=duplicate',{method:'POST',body:html,headers:{'Content-Type':'text/html','Idempotency-Key':randomUUID()},status:409});
assert.equal(duplicate.data.path,path);
const revised=html+' updated';
p=(await call('/api/projects/'+p.id+'/versions',{method:'POST',body:revised,headers:{'Content-Type':'text/html','Idempotency-Key':randomUUID(),'If-Match':'"'+p.revision+'"'}})).data.project;
assert.equal('/'+p.slug+'/',path);
p=(await call('/api/projects/'+p.id,{method:'PATCH',body:{name:'renamed'},headers:{'If-Match':'"'+p.revision+'"'}})).data.project;
await serving(path,{anonymous:true,status:307});path='/'+p.slug+'/';
p=(await call('/api/projects/'+p.id+'/privacy',{method:'PUT',body:{private:true,password:'a very long raft password'},headers:{'If-Match':'"'+p.revision+'"'}})).data.project;
await serving(path,{anonymous:true,status:404});
await call('/api/unlock',{method:'POST',body:{slug:p.slug,password:'a very long raft password'}});
const navigation={'Sec-Fetch-Mode':'navigate','Sec-Fetch-Dest':'document'};
assert.equal(contentWithoutWatermark((await serving(path,{headers:navigation},revised)).data),revised);
const local=JSON.parse(compose('exec','-T','one','/usr/local/bin/drop-cluster','-status'));
assert.ok(Object.hasOwn(bases,local.leaderID),'unknown leader');
compose('stop',local.leaderID);
const survivors=Object.keys(bases).filter(id=>id!==local.leaderID);
base=bases[survivors[0]];await ready();
assert.equal(contentWithoutWatermark((await serving(path,{headers:navigation},revised)).data),revised);
p=(await call('/api/projects/'+p.id+'/privacy',{method:'PUT',body:{private:true,password:'a replacement raft password'},headers:{'If-Match':'"'+p.revision+'"'}})).data.project;
await serving(path,{headers:navigation,status:303});
await call('/api/unlock',{method:'POST',body:{slug:p.slug,password:'a replacement raft password'}});
assert.equal(contentWithoutWatermark((await serving(path,{headers:navigation},revised)).data),revised);
compose('stop',survivors[1]);
for(let i=0;i<30;i++){
 if((await fetch(base+'/readyz',{signal:AbortSignal.timeout(10000)})).status===503)break;
 if(i===29)throw Error('minority remained ready');await new Promise(r=>setTimeout(r,500));
}
// Applied follower serving remains available without quorum under the accepted
// eventual model. Owner management still requires the original leader path.
assert.equal(contentWithoutWatermark((await serving(path,{headers:navigation},revised)).data),revised);
await call('/api/projects/'+p.id,{status:503});
assert.equal((await fetch(base+'/healthz')).status,200);
compose('up','-d','--no-deps','--no-recreate',local.leaderID,survivors[1]);
for(const value of Object.values(bases)){base=value;await ready()}
assert.equal((await call('/api/projects/'+p.id)).data.project.id,p.id);
assert.equal(contentWithoutWatermark((await serving(path,{headers:navigation},revised)).data),revised);
compose('restart','--no-deps','one','two','three');
for(const value of Object.values(bases)){base=value;await ready()}
assert.equal(contentWithoutWatermark((await serving(path,{headers:navigation},revised)).data),revised);
await call('/api/projects/'+p.id,{method:'DELETE',headers:{'If-Match':'"'+p.revision+'"'},status:204});
await serving(path,{anonymous:true,status:404});
const oldCookie=cookies.get('__Host-drop-session');
csrf=(await call('/api/auth/logout',{method:'POST',body:{}})).data.csrfToken;
assert.notEqual(cookies.get('__Host-drop-session'),oldCookie);
for(const value of Object.values(bases)){base=value;await call('/api/projects',{headers:{Cookie:'__Host-drop-session='+oldCookie},status:401})}
console.log('PASS: credential-free final-image publishing, cross-node sessions, dedup/update/rename, privacy/unlock/revocation, leader loss, local serving during quorum loss with management denial, full restart, deletion and logout');

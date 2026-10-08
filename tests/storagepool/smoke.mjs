import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {randomUUID} from 'node:crypto';

const imageIndex=process.argv.indexOf('--image');
if(imageIndex>=0){
  const image=process.argv[imageIndex+1];
  assert.ok(image&&/^[A-Za-z0-9][A-Za-z0-9./_:@-]{0,511}$/.test(image),'valid --image required');
  process.env.DROP_SMOKE_IMAGE=image;
}

// Each run owns an independent project and disposable volumes. No network,
// production API, Firebase login, existing containers, or secrets are used.
const project='drop-storage-smoke-'+randomUUID().slice(0,8);
const prefix=['compose','-p',project,'-f','tests/storagepool/compose.yaml'];
const compose=(args)=>execFileSync('docker',[...prefix,...args],{encoding:'utf8',timeout:45000});
const inside=(service,args)=>compose(['exec','-T',service,...args]);
const wait=ms=>new Promise(resolve=>setTimeout(resolve,ms));
async function alive(service){
  for(let i=0;i<45;i++){
    try{assert.equal(JSON.parse(inside(service,['wget','-qO-','http://127.0.0.1:8080/healthz'])).status,'ok');return;}catch{}
    await wait(1000);
  }
  throw Error(service+' failed to start');
}
function response(service,path){
  return inside(service,['sh','-c',`wget -S -O /dev/null http://127.0.0.1:8080${path} 2>&1 || true`]);
}
try{
  compose(['up','-d']);
  await Promise.all([alive('primary'),alive('secondary')]);
  for(const service of ['primary','secondary'])inside(service,['/usr/local/bin/drop-init','healthcheck']);
  assert.match(response('primary','/'),/HTTP\/1\.1 200/);
  assert.match(response('primary','/admin/'),/HTTP\/1\.1 200/);
  assert.match(response('primary','/admin/api/apps'),/HTTP\/1\.1 401/);
  assert.match(response('primary','/admin/api/projects'),/HTTP\/1\.1 401/);
  for(const path of ['/','/admin/','/admin/api/apps','/admin/api/projects','/api/config','/api/projects','/internal/storage/v1/capacity','/hello-abcdef/']){
    assert.match(response('secondary',path),/HTTP\/1\.1 404/,path);
  }
  for(const service of ['primary','secondary'])assert.match(response(service,'/readyz'),/HTTP\/1\.1 503/);
  inside('secondary',['sh','-c','test ! -e /var/lib/drop-cluster/private/cluster-node.json']);
  inside('primary',['sh','-c','test ! -e /data/cluster-genesis.json']);
  const id=inside('secondary',['cat','/var/lib/drop-cluster/private/storage-instance-id']).trim();
  assert.match(id,/^[a-f0-9]{32}$/);
  const ownerMode=inside('secondary',['stat','-c','%u:%a','/var/lib/drop-cluster/private']).trim();
  assert.equal(ownerMode,'65534:700');
  compose(['restart','secondary']);await alive('secondary');
  assert.equal(inside('secondary',['cat','/var/lib/drop-cluster/private/storage-instance-id']).trim(),id);
  console.log('PASS: final-image role startup, supervision, public boundaries, offline fail-closed readiness, no rebootstrap, private state ownership, and restart identity');
}catch(error){
  try{console.error(compose(['logs','--tail','30']));}catch{}
  throw error;
}finally{
  compose(['down','--volumes','--remove-orphans']);
}

import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';

const compose=(...args)=>execFileSync('docker',['compose','-f','tests/automatic/compose.yaml',...args],{encoding:'utf8',stdio:['ignore','pipe','pipe']});
async function healthy(){
  for(let i=0;i<60;i++){
    try{compose('exec','-T','app','/usr/local/bin/drop-init','healthcheck');return}catch{}
    await new Promise(resolve=>setTimeout(resolve,500));
  }
  throw Error('automatic startup did not become healthy');
}
await healthy();
const first=JSON.parse(compose('exec','-T','app','/usr/local/bin/drop-cluster','-status'));
assert.match(first.id,/^[a-f0-9]{32}$/);
assert.match(first.clusterID,/^[a-f0-9]{32}$/);
assert.equal(first.lastIndex,0,'network-isolated node must not infer singleton bootstrap');
const permissions=compose('exec','-T','app','stat','-c','%a %u %g',
  '/var/lib/drop-cluster/cluster-node.json',
  '/var/lib/drop-cluster/cluster-ca.json',
  '/var/lib/drop-cluster/node.key',
  '/var/lib/drop-cluster/content-journal.db');
assert.deepEqual(permissions.trim().split('\n'),Array(4).fill('600 65534 65534'));
compose('exec','-T','app','wget','-q','-O','/dev/null','http://127.0.0.1:8080/');
compose('restart','--no-deps','app');
await healthy();
const second=JSON.parse(compose('exec','-T','app','/usr/local/bin/drop-cluster','-status'));
assert.equal(second.id,first.id);
assert.equal(second.clusterID,first.clusterID);
assert.equal(second.lastIndex,0);
console.log('PASS: passphrase-only startup, two volumes, private generated files, public UI, stable restart identity, no bootstrap on discovery outage');

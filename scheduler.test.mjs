import test from 'node:test';import assert from 'node:assert/strict';
import {run,choose} from './scheduler.mjs';import {encryptState,decryptState} from './crypto.mjs';
const now=new Date('2026-10-07T03:02:00Z');
function fixture(){return {version:1,enabled:true,account:'plusoneapp_appcreater',rules:[{app_id:'',platform:'Threads',enabled:true,days:[2],times:['12:00'],updated_at:'2026-10-06T00:00:00Z'}],items:[{id:'one',app_id:'',platform:'Threads',position:1,state:'waiting',body:'買い物リストを作ったのに、お店で見るのを忘れる。',hashtags:'#アプリ',enqueued_at:'2026-10-06T00:00:00Z',dispatch_at:''},{id:'two',app_id:'',platform:'Threads',position:2,state:'waiting',body:'次の投稿',hashtags:'#アプリ',enqueued_at:'2026-10-06T00:00:00Z',dispatch_at:''}],ticks:{}};}
function memory(value=fixture()){let version=1,state=structuredClone(value),writes=0;return {get state(){return structuredClone(state)},get writes(){return writes},async read(){return {state:structuredClone(state),sha:String(version)}},async write(value,sha){assert.equal(sha,String(version));writes++;state=structuredClone(value);return String(++version)}};}
function publisher(){return {calls:0,async identity(expected){assert.equal(expected,'plusoneapp_appcreater')},async publish(){this.calls++;return {id:'123',url:'https://www.threads.net/@user/post/123'}}};}
test('state encryption authenticates and has no public plaintext',()=>{const key=Buffer.alloc(32,1).toString('base64'),value=fixture(),encrypted=encryptState(value,key);assert(!JSON.stringify(encrypted).includes(value.items[0].body));assert.deepEqual(decryptState(encrypted,key),value);assert.throws(()=>decryptState(encrypted,Buffer.alloc(32,2).toString('base64')));});
test('dry run verifies account but never writes or publishes',async()=>{const store=memory(),p=publisher();let checks=0;p.identity=async()=>{checks++};const result=await run(store,p,{now});assert.equal(checks,1);assert(result.account_verified);assert.equal(p.calls,0);assert.equal(store.writes,0)});
test('one recent JST tick one post and duplicate run sends none',async()=>{const store=memory(),p=publisher();assert.equal((await run(store,p,{now,dryRun:false})).sent,1);await run(store,p,{now,dryRun:false});assert.equal(p.calls,1);assert.equal(store.state.items[0].state,'sent');assert.equal(store.state.items[1].state,'waiting');});
test('timeout blocks queue and never re-sends',async()=>{const store=memory(),p=publisher();p.publish=async()=>{p.calls++;throw Error('timeout')};await run(store,p,{now,dryRun:false});await run(store,p,{now:new Date('2026-10-14T03:02:00Z'),dryRun:false});assert.equal(p.calls,1);assert.equal(store.state.items[0].state,'error')});
test('failed result commit preserves pre-POST durable marker',async()=>{const store=memory(),p=publisher(),write=store.write.bind(store);store.write=async(v,sha)=>{if(v.items[0].state==='sent')throw Error('commit timeout');return write(v,sha)};await assert.rejects(run(store,p,{now,dryRun:false}));assert.equal(store.state.items[0].state,'sending');store.write=write;await run(store,p,{now,dryRun:false});assert.equal(p.calls,1);assert.equal(store.state.items[0].state,'error')});
test('CAS race prevents publication',async()=>{const store=memory(),p=publisher();store.write=async()=>{throw Error('conflict')};await assert.rejects(run(store,p,{now,dryRun:false}));assert.equal(p.calls,0)});
test('parallel runs lose CAS before sending',async()=>{const store=memory(),p=publisher();const results=await Promise.allSettled([run(store,p,{now,dryRun:false}),run(store,p,{now,dryRun:false})]);assert.equal(results.filter(v=>v.status==='fulfilled').length,1);assert.equal(p.calls,1)});
test('stopped, cancelled, errors, other media excluded',()=>{for(const change of [v=>v.enabled=false,v=>v.rules[0].enabled=false,v=>v.items[0].state='error',v=>v.items.forEach(i=>i.state='cancelled'),v=>v.rules[0].platform='X',v=>v.rules[0].platform='Instagram']){const state=fixture();change(state);assert.equal(choose(state,now),null)}});
test('late jobs retain slots while approval and changed rules never reach backwards',()=>{const state=fixture();assert(choose(state,new Date('2026-10-07T08:00:00Z')));state.items[0].enqueued_at='2026-10-07T03:01:00Z';assert.equal(choose(state,now),null);state.items[0].enqueued_at='2026-10-06T00:00:00Z';state.rules[0].updated_at='2026-10-07T03:01:00Z';assert.equal(choose(state,now),null);const midnight=fixture();midnight.rules[0].days=[3];midnight.rules[0].times=['00:00'];assert(choose(midnight,new Date('2026-10-07T15:02:00Z')))});
test('malformed overlong body blocks instead of publishing',async()=>{const value=fixture();value.items[0].body='a'.repeat(501);const store=memory(value),p=publisher();await run(store,p,{now,dryRun:false});assert.equal(store.state.items[0].state,'error');assert.equal(p.calls,0)});
test('expired account records actionable health without consuming the queue',async()=>{const store=memory(),p=publisher();p.identity=async()=>{throw Error('expired')};const result=await run(store,p,{now,dryRun:false});assert(result.connection_error);assert.equal(p.calls,0);assert.equal(store.state.items[0].state,'waiting');assert(store.state.message.includes('接続'))});
test('backlog uses the oldest slot and queue order, with only one post per run',async()=>{
 const value=fixture();value.rules[0].times=['12:00','08:00'];value.items.reverse();const store=memory(value),p=publisher();
 await run(store,p,{now:new Date('2026-10-07T06:00:00Z'),dryRun:false});
 assert.equal(p.calls,1);assert.equal(store.state.items.find(i=>i.id==='one').dispatch_at,'2026-10-07T08:00');
 assert.equal(store.state.items.find(i=>i.id==='two').state,'waiting');assert.equal(Object.keys(store.state.ticks).length,1);
});
test('cooldown prevents a burst, then consumes a different slot without deleting receipts',async()=>{
 const value=fixture();value.rules[0].times=['08:00','12:00'];const store=memory(value),p=publisher(),at=new Date('2026-10-07T06:00:00Z');
 await run(store,p,{now:at,dryRun:false});
 const waiting=await run(store,p,{now:new Date(at.getTime()+29*60000),dryRun:false});
 assert.equal(waiting.result,'waiting');assert.match(waiting.reason,/30分/);assert.equal(p.calls,1);
 await run(store,p,{now:new Date(at.getTime()+30*60000),dryRun:false});
 assert.equal(p.calls,2);assert.equal(Object.keys(store.state.ticks).length,2);assert(store.state.items.every(i=>i.state==='sent'&&!i.body&&!i.hashtags));
 assert.equal(store.state.items[1].dispatch_at,'2026-10-07T12:00');
 assert.equal(store.state.last_published_at,'2026-10-07T06:30:00.000Z');
});
test('the oldest unused slot wins across scopes and cooldown is scoped',async()=>{
 const value=fixture();value.rules[0].times=['12:00'];value.rules.push({...value.rules[0],app_id:'other',times:['08:00']});
 value.items.push({...value.items[0],id:'other',app_id:'other',position:9});const store=memory(value),p=publisher(),at=new Date('2026-10-07T06:00:00Z');
 await run(store,p,{now:at,dryRun:false});assert.equal(store.state.items.find(i=>i.id==='other').state,'sent');
 await run(store,p,{now:at,dryRun:false});assert.equal(p.calls,2);assert.equal(store.state.items[0].state,'sent');
});
test('24 hour window includes its exact boundary and excludes older slots',()=>{
 const value=fixture();const boundary=new Date('2026-10-08T03:00:00Z');assert.equal(choose(value,boundary).at,'2026-10-07T12:00');
 assert.equal(choose(value,new Date(boundary.getTime()+1)),null);
});
test('JST midnight preserves the previous weekday and orders yesterday before today',()=>{
 const value=fixture();value.rules[0].days=[2,3];value.rules[0].times=['00:00','23:50'];value.items[0].enqueued_at='2026-10-07T14:00:00Z';
 assert.equal(choose(value,new Date('2026-10-07T15:05:00Z')).at,'2026-10-07T23:50');
 value.ticks['|Threads|2026-10-07T23:50']='receipt';assert.equal(choose(value,new Date('2026-10-07T15:05:00Z')).at,'2026-10-08T00:00');
});
test('health distinguishes stopped, empty, blocked head and scope mismatch without publishing',async()=>{
 for(const [change,result,reason] of [
  [v=>v.enabled=false,'stopped','停止中'],[v=>v.rules[0].enabled=false,'stopped','開始'],
  [v=>v.items=[],'waiting','ありません'],[v=>v.items[0].state='error','blocked','先頭'],
  [v=>v.items.forEach(i=>i.app_id='no-rule'),'blocked','発信対象']
 ]){const value=fixture();change(value);const store=memory(value),p=publisher();const actual=await run(store,p,{now,dryRun:false});
  assert.equal(actual.result,result);assert.match(actual.reason,new RegExp(reason));assert.equal(store.state.last_result,result);
  assert.equal(store.state.last_run_at,now.toISOString());assert.equal(store.state.last_reason,actual.reason);assert.equal(p.calls,0);assert.equal(store.writes,1);
 }
});
test('uncertain sends record blocked health and dry-run never recovers or changes that state',async()=>{
 const value=fixture();value.items[0].state='sending';value.last_published_at='preserved';const store=memory(value),p=publisher();
 await run(store,p,{now,dryRun:true});assert.deepEqual(store.state,value);assert.equal(store.writes,0);
 await run(store,p,{now,dryRun:false});assert.equal(store.state.items[0].state,'error');assert.equal(store.state.last_result,'blocked');
 assert.equal(store.state.last_published_at,'preserved');assert.equal(p.calls,0);
});
test('successful and connection-failed runs persist their health without leaking text in summaries',async()=>{
 const store=memory(),p=publisher();const result=await run(store,p,{now,dryRun:false});assert.equal(result.result,'sent');
 assert.equal(store.state.last_result,'sent');assert.equal(store.state.last_published_at,now.toISOString());assert(!JSON.stringify(result).includes(fixture().items[0].body));
 const failed=memory(),bad=publisher();bad.identity=async()=>{throw Error('no permission')};await run(failed,bad,{now,dryRun:false});
 assert.equal(failed.state.last_result,'connection_error');assert.equal(failed.state.last_run_at,now.toISOString());assert.deepEqual(failed.state.ticks,{});
});
test('an unconfigured scope does not report all delivery blocked when another scope is waiting for its next time',async()=>{
 const value=fixture();value.items.push({...value.items[0],id:'unconfigured',app_id:'no-rule'});
 const store=memory(value),p=publisher();const result=await run(store,p,{now:new Date('2026-10-07T02:00:00Z'),dryRun:false});
 assert.equal(result.result,'waiting');assert.equal(result.reason,'一部の投稿待ちに配信設定がありません。設定済みの対象は次の時刻を待っています。');assert.equal(p.calls,0);
 assert.equal(store.state.last_result,'waiting');
});
test('dry-run does not report invalid copy as eligible and does not mutate production state',async()=>{
 const value=fixture();value.items[0].body='a'.repeat(501);const store=memory(value),p=publisher();
 const result=await run(store,p,{now,dryRun:true});assert.equal(result.eligible,false);assert.equal(result.account_verified,true);
 assert.deepEqual(store.state,value);assert.equal(store.writes,0);assert.equal(p.calls,0);
});
function commented(){const value=fixture();value.items=value.items.slice(0,1);value.items[0].first_comment='詳しい使い方の補足です。';return value;}
function replyPublisher(store){const p=publisher();p.replies=[];p.reply=async(text,parent)=>{
 const receipt=store.state.items[0];assert.equal(receipt.state,'sent');assert.equal(receipt.media_id,'123');assert.equal(receipt.comment_state,'sending');assert.equal(receipt.comment_parent_id,parent);
 p.replies.push({text,parent});return {id:'456'};
};return p;}
test('account schedule picks one FIFO across app subjects and applies cooldown across them',async()=>{
 const value=fixture();value.rules[0].scope='account';value.rules[0].times=['08:00','12:00'];value.items[0].app_id='okaimo';value.items[1].app_id='manaloop';value.items.reverse();
 const store=memory(value),p=publisher(),at=new Date('2026-10-07T06:00:00Z');await run(store,p,{now:at,dryRun:false});
 assert.equal(store.state.items.find(i=>i.id==='one').state,'sent');assert.equal(store.state.items.find(i=>i.id==='two').state,'waiting');assert.equal(store.state.ticks['|Threads|2026-10-07T08:00'],'one');
 await run(store,p,{now:new Date(at.getTime()+5*60000),dryRun:false});assert.equal(p.calls,1);
 await run(store,p,{now:new Date(at.getTime()+30*60000),dryRun:false});assert.equal(p.calls,2);
});
test('a paused account rule cannot fall back to an old enabled per-app rule',async()=>{
 const value=fixture();value.items[0].app_id='okaimo';value.rules.push({...value.rules[0],app_id:'okaimo'});value.rules[0]={...value.rules[0],scope:'account',enabled:false};
 const store=memory(value),p=publisher();await run(store,p,{now,dryRun:false});assert.equal(p.calls,0);assert.equal(store.state.last_result,'stopped');
});
test('body receipt is durable before comment claim, reply uses that exact parent and logs contain no copy',async()=>{
 const store=memory(commented()),p=replyPublisher(store),result=await run(store,p,{now,dryRun:false});
 assert.equal(result.sent,1);assert.equal(result.comment_sent,1);assert.deepEqual(p.replies,[{text:'詳しい使い方の補足です。',parent:'123'}]);
 assert.equal(store.state.items[0].comment_state,'sent');assert.equal(store.state.items[0].comment_media_id,'456');assert.equal(store.state.items[0].first_comment,'');
 assert(!JSON.stringify(result).includes('補足'));assert(!JSON.stringify(result).includes('https://'));await run(store,p,{now,dryRun:false});assert.equal(p.calls,1);assert.equal(p.replies.length,1);
});
test('comment timeout leaves body sent and never automatically re-sends either publication',async()=>{
 const store=memory(commented()),p=replyPublisher(store);let calls=0;p.reply=async()=>{calls++;throw Error('timeout')};
 const result=await run(store,p,{now,dryRun:false});assert.equal(result.sent,1);assert.equal(result.result,'comment_error');assert.equal(store.state.items[0].state,'sent');assert.equal(store.state.items[0].comment_state,'uncertain');
 await run(store,p,{now:new Date('2026-10-14T03:02:00Z'),dryRun:false});assert.equal(p.calls,1);assert.equal(calls,1);assert.equal(store.state.last_result,'comment_error');
});
test('body result persistence failure never starts a comment and never re-sends the body',async()=>{
 const store=memory(commented()),p=replyPublisher(store),write=store.write.bind(store);store.write=async(v,sha)=>{if(v.items[0].state==='sent')throw Error('body result lost');return write(v,sha)};
 await assert.rejects(run(store,p,{now,dryRun:false}));assert.equal(store.state.items[0].state,'sending');assert.equal(p.replies.length,0);
 store.write=write;await run(store,p,{now,dryRun:false});assert.equal(p.calls,1);assert.equal(p.replies.length,0);assert.equal(store.state.items[0].state,'error');
});
test('a comment whose pre-POST claim was not saved can safely resume without re-sending body',async()=>{
 const store=memory(commented()),p=replyPublisher(store),write=store.write.bind(store);store.write=async(v,sha)=>{if(v.items[0].comment_state==='sending')throw Error('comment claim CAS conflict');return write(v,sha)};
 await assert.rejects(run(store,p,{now,dryRun:false}));assert.equal(store.state.items[0].state,'sent');assert.equal(store.state.items[0].comment_state,'waiting');assert.equal(p.replies.length,0);
 store.write=write;const resumed=await run(store,p,{now:new Date(now.getTime()+60000),dryRun:false});assert.equal(resumed.sent,0);assert.equal(resumed.comment_sent,1);assert.equal(p.calls,1);assert.equal(p.replies.length,1);
});
test('a lost comment success record becomes uncertain and cannot trigger a duplicate reply',async()=>{
 const store=memory(commented()),p=replyPublisher(store),write=store.write.bind(store);store.write=async(v,sha)=>{if(v.items[0].comment_state==='sent')throw Error('reply result lost');return write(v,sha)};
 await assert.rejects(run(store,p,{now,dryRun:false}));assert.equal(store.state.items[0].state,'sent');assert.equal(store.state.items[0].comment_state,'sending');
 store.write=write;await run(store,p,{now,dryRun:false});assert.equal(store.state.items[0].comment_state,'uncertain');assert.equal(store.state.last_result,'comment_error');assert.equal(p.calls,1);assert.equal(p.replies.length,1);
});
test('waiting comments obey stop, validate their fixed parent and preserve read-only dry-run',async()=>{
 const value=commented();Object.assign(value.items[0],{state:'sent',media_id:'123',comment_parent_id:'123',comment_state:'waiting',published_at:now.toISOString()});
 const store=memory(value),p=replyPublisher(store);await run(store,p,{now,dryRun:true});assert.deepEqual(store.state,value);assert.equal(store.writes,0);assert.equal(p.replies.length,0);
 const paused=structuredClone(value);paused.enabled=false;const stop=memory(paused);await run(stop,p,{now,dryRun:false});assert.equal(stop.state.items[0].comment_state,'waiting');assert.equal(p.replies.length,0);
 value.items[0].comment_parent_id='999';const bad=memory(value);await run(bad,p,{now,dryRun:false});assert.equal(bad.state.items[0].state,'sent');assert.equal(bad.state.items[0].comment_state,'error');assert.equal(p.calls,0);assert.equal(p.replies.length,0);
});
test('an invalid comment prevents body publication rather than knowingly creating a partial post',async()=>{
 const value=commented();value.items[0].first_comment='a'.repeat(501);const store=memory(value),p=replyPublisher(store);
 await run(store,p,{now,dryRun:false});assert.equal(store.state.items[0].state,'error');assert.equal(p.calls,0);assert.equal(p.replies.length,0);
});
test('concurrent recovery runs CAS-claim a waiting comment once',async()=>{
 const value=commented();Object.assign(value.items[0],{state:'sent',media_id:'123',comment_parent_id:'123',comment_state:'waiting',published_at:now.toISOString()});const store=memory(value),p=replyPublisher(store);
 const results=await Promise.allSettled([run(store,p,{now,dryRun:false}),run(store,p,{now,dryRun:false})]);assert.equal(results.filter(r=>r.status==='fulfilled').length,1);assert.equal(p.calls,0);assert.equal(p.replies.length,1);
});

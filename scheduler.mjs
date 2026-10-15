import {encryptState,decryptState} from './crypto.mjs';
const account='plusoneapp_appcreater';
const ruleMatches=(rule,item)=>item.platform==='Threads'&&(rule.scope==='account'||item.app_id===rule.app_id);
function activeRules(state){const all=(state.rules??[]).filter(r=>r.platform==='Threads'),global=all.filter(r=>r.scope==='account');return (global.length?global:all).filter(r=>r.enabled);}
const copyIssue=text=>typeof text!=='string'||[...text.trim()].length>500||text.includes('［')||text.includes('］')||text.includes('[要確認]');
function inspect(state,now){
 if(state.version!==1||state.account!==account)return {result:'blocked',reason:'配信先と保存形式を確認してください。'};
 if(state.enabled!==true)return {result:'stopped',reason:'クラウドの自動投稿は停止中です。'};
 const rules=activeRules(state);
 if(!rules.length)return {result:'stopped',reason:'Threadsの配信が開始されていません。'};
 const pending=(state.items??[]).filter(p=>p.platform==='Threads'&&['waiting','sending','error'].includes(p.state));
 const candidates=[];let blocked='',waiting='',invalid;
 const minute=now.getTime()-now.getTime()%60000;
 for(const rule of rules){
  const head=pending.filter(p=>ruleMatches(rule,p)).sort((a,b)=>a.position-b.position)[0];
  if(!head)continue;
  if(head.state!=='waiting'){blocked='先頭の投稿の送信結果を確認してください。後ろの投稿も停止しています。';continue;}
  if(!Number.isFinite(Date.parse(head.enqueued_at))||!Number.isFinite(Date.parse(rule.updated_at))||!Array.isArray(rule.days)||!Array.isArray(rule.times)){
   invalid??={invalid:true,item:head,reason:'投稿待ちと配信設定の日時を確認してください。'};continue;
  }
  const previous=(state.items??[]).filter(p=>ruleMatches(rule,p)&&p.state==='sent').reduce((latest,p)=>Math.max(latest,Date.parse(p.published_at)||0),0);
  if(previous&&now.getTime()-previous<30*60000){waiting='前の投稿から30分間は、続けて投稿せず待ちます。';continue;}
  // GitHub schedules can arrive hours late. Consume the oldest unused slot
  // within 24 hours, only after both approval and the current rule were saved.
  for(let offset=1440;offset>=0;offset--){
   const tick=minute-offset*60000,jst=new Date(tick+9*3600000),at=jst.toISOString().slice(0,16),weekday=(jst.getUTCDay()+6)%7;
   if(tick<now.getTime()-24*3600000)continue;
   if(!rule.days.includes(weekday)||!rule.times.includes(at.slice(11)))continue;
   if(tick<Date.parse(head.enqueued_at)||tick<Date.parse(rule.updated_at))continue;
   const key=`${rule.app_id}|Threads|${at}`;
   if(Object.hasOwn(state.ticks??{},key))continue;
   const text=[head.body,head.hashtags].map(s=>typeof s==='string'?s.trim():'').filter(Boolean).join('\n\n');
   if(typeof head.body!=='string'||!head.body.trim()||copyIssue(text)||(head.first_comment!==undefined&&copyIssue(head.first_comment))){invalid??={invalid:true,item:head,reason:'本文と最初のコメントを確認してください。'};break;}
   candidates.push({item:head,at,key,text,tick});break;
  }
 }
 if(candidates.length)return {selected:candidates.sort((a,b)=>a.tick-b.tick||a.item.position-b.item.position)[0]};
 if(invalid)return {selected:invalid};
 if(blocked)return {result:'blocked',reason:blocked};
 if(waiting)return {result:'waiting',reason:waiting};
 if(pending.some(p=>!rules.some(r=>ruleMatches(r,p)))){
  const configuredWaiting=pending.some(p=>p.state==='waiting'&&rules.some(r=>ruleMatches(r,p)));
  return configuredWaiting?{result:'waiting',reason:'一部の投稿待ちに配信設定がありません。設定済みの対象は次の時刻を待っています。'}:{result:'blocked',reason:'投稿待ちの発信対象に、配信中の設定がありません。'};
 }
 return {result:'waiting',reason:pending.length?'未使用の投稿時刻を待っています。24時間より前の時刻には遡りません。':'配信対象の投稿待ちがありません。'};
}
export function choose(state,now=new Date()){return inspect(state,now).selected??null;}
function health(state,now,result,reason){state.last_run_at=now.toISOString();state.last_result=result;state.last_reason=reason;}
async function sendComment(store,publisher,state,sha,item,now,bodySent){
 const text=item.first_comment,parent=item.comment_parent_id;
 if(typeof text!=='string'||!text.trim()||copyIssue(text)||!/^\d+$/.test(parent??'')||parent!==item.media_id){
  item.comment_state='error';item.comment_message='本文は公開済みです。最初のコメントと返信先を確認してください。';health(state,now,'comment_error',item.comment_message);
  await store.write(state,sha);return {sent:bodySent,comment_sent:0,result:'comment_error',reason:item.comment_message};
 }
 // This journal is independent from the already durable body receipt.
 // A failed CAS here leaves only a waiting comment and cannot re-send the body.
 item.comment_state='sending';item.comment_message='';
 health(state,now,'comment_error','本文は公開済みです。最初のコメントの送信結果を確認中です。');
 const claimed=await store.write(state,sha);
 let result;
 try{result=await publisher.reply(text.trim(),parent);if(!/^\d+$/.test(result?.id??''))throw Error('Missing reply receipt');}catch{
  item.comment_state='uncertain';item.comment_message='本文は公開済みですが、最初のコメントの送信結果が未確定です。自動では再送しません。';health(state,now,'comment_error',item.comment_message);
  await store.write(state,claimed);return {sent:bodySent,comment_sent:0,result:'comment_error',reason:item.comment_message,uncertain:true};
 }
 item.comment_state='sent';item.comment_media_id=result.id;item.comment_message='';item.first_comment='';
 health(state,now,'sent','本文と最初のコメントを公開しました。');
 await store.write(state,claimed); // Failure retains sending; no automatic retry.
 return {sent:bodySent,comment_sent:1,result:'sent',reason:state.last_reason};
}
export async function run(store,publisher,{now=new Date(),dryRun=true}={}){
 const saved=await store.read(),state=saved.state;
 if(dryRun){await publisher.identity(account);const selected=choose(state,now);return {dry_run:true,eligible:!!selected&&!selected.invalid,account_verified:true};}
 // A previously durable sending marker can be an interrupted successful POST.
 // Never automatically re-send it, and keep its queue blocked.
 for(const item of state.items??[])if(item.state==='sending'){item.state='error';item.message='前回の送信結果が未確定です。自動では再送しません。';}
 for(const item of state.items??[])if(item.comment_state==='sending'){item.comment_state='uncertain';item.comment_message='本文は公開済みですが、最初のコメントの送信結果が未確定です。自動では再送しません。';}
 const enabled=state.version===1&&state.account===account&&state.enabled===true,rules=enabled?activeRules(state):[];
 const comment=(state.items??[]).filter(p=>p.platform==='Threads'&&p.state==='sent'&&p.comment_state==='waiting'&&rules.some(r=>ruleMatches(r,p))).sort((a,b)=>a.position-b.position)[0];
 if(comment){
  try{await publisher.identity(account);}catch{comment.comment_message='本文は公開済みです。最初のコメントを送るにはThreadsの接続を確認してください。';health(state,now,'comment_error',comment.comment_message);await store.write(state,saved.sha);return {sent:0,comment_sent:0,result:'comment_error',reason:comment.comment_message};}
  return sendComment(store,publisher,state,saved.sha,comment,now,0);
 }
 const decision=inspect(state,now),selected=decision.selected;
 if(!selected){const partial=rules.length&&(state.items??[]).some(p=>p.platform==='Threads'&&p.state==='sent'&&['error','uncertain'].includes(p.comment_state));const result=partial?'comment_error':decision.result,reason=partial?'本文は公開済みですが、最初のコメントの確認が必要です。本文と未確定のコメントは再送しません。':decision.reason;health(state,now,result,reason);await store.write(state,saved.sha);return {sent:0,result,reason};}
 if(selected.invalid){selected.item.state='error';selected.item.message=selected.reason;health(state,now,'blocked',selected.reason);await store.write(state,saved.sha);return {sent:0,result:'blocked',reason:selected.reason};}
 try{await publisher.identity(account);}catch{state.message='Threadsの接続を確認してください。投稿待ちは送信していません。';health(state,now,'connection_error',state.message);await store.write(state,saved.sha);return {sent:0,result:'connection_error',reason:state.message,connection_error:true};}
 state.message='';
 state.ticks??={};state.ticks[selected.key]=selected.item.id;
 selected.item.state='sending';selected.item.dispatch_at=selected.at;state.updated_at=now.toISOString();
 health(state,now,'blocked','投稿を送信中です。結果が確定するまで再送しません。');
 const sha=await store.write(state,saved.sha); // MUST be durable before any SNS POST.
 let result;
 try{result=await publisher.publish(selected.text);if(!/^\d+$/.test(result?.id??''))throw Error('Missing body receipt');}catch{
  selected.item.state='error';selected.item.message='送信結果を確認できません。SNSで確認するまで再送しません。';
  health(state,now,'blocked',selected.item.message);await store.write(state,sha);return {sent:0,result:'blocked',reason:selected.item.message,uncertain:true};
 }
 selected.item.state='sent';selected.item.media_id=result.id;selected.item.url=result.url??'';selected.item.published_at=now.toISOString();selected.item.message='';
 const withComment=!!selected.item.first_comment?.trim();
 if(withComment){selected.item.comment_state='waiting';selected.item.comment_parent_id=result.id;selected.item.comment_media_id='';selected.item.comment_message='';}
 // Keep idempotency receipts permanently; the original post remains in the PC database.
 selected.item.body='';selected.item.hashtags='';
 state.last_published_at=now.toISOString();health(state,now,withComment?'waiting':'sent',withComment?'本文は公開済みです。最初のコメントの送信を待っています。':'確認済みの投稿を1件公開しました。');
 const bodySaved=await store.write(state,sha); // Failure leaves the remote pre-POST sending marker intact.
 if(withComment)return sendComment(store,publisher,state,bodySaved,selected.item,now,1);
 return {sent:1,result:'sent',reason:state.last_reason};
}
export function githubStore({repo,token,key,fetcher=fetch}){
 const endpoint=`https://api.github.com/repos/${repo}/contents/cloud-state.json`;
 const headers={Authorization:`Bearer ${token}`,'User-Agent':'PlusOneApp-Scheduler','X-GitHub-Api-Version':'2022-11-28','Content-Type':'application/json'};
 return {
  async read(){const response=await fetcher(endpoint,{headers});if(!response.ok)throw Error('State read failed');const raw=await response.json();return {sha:raw.sha,state:decryptState(JSON.parse(Buffer.from(raw.content.replace(/\s/g,''),'base64').toString()),key)};},
  async write(state,sha){const content=Buffer.from(JSON.stringify(encryptState(state,key))).toString('base64');const response=await fetcher(endpoint,{method:'PUT',headers,body:JSON.stringify({message:'Update encrypted scheduler state',content,sha})});if(!response.ok)throw Error('State changed or could not be saved');return (await response.json()).content.sha;}
 };
}

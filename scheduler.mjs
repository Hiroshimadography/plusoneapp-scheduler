import {encryptState,decryptState} from './crypto.mjs';
const account='plusoneapp_appcreater';
export function choose(state,now=new Date()){
 if(state.version!==1||state.account!==account||state.enabled!==true)return null;
 const local=new Date(now.getTime()+9*3600000),day=(local.getUTCDay()+6)%7;
 const minute=now.getTime()-now.getTime()%60000;
 for(const rule of state.rules??[]){
  if(!rule.enabled||rule.platform!=='Threads')continue;
  const head=(state.items??[]).filter(p=>p.app_id===rule.app_id&&p.platform==='Threads'&&['waiting','sending','error'].includes(p.state)).sort((a,b)=>a.position-b.position)[0];
  if(!head||head.state!=='waiting')continue;
  if(!Number.isFinite(Date.parse(head.enqueued_at))||!Number.isFinite(Date.parse(rule.updated_at))){head.state='error';head.message='投稿待ちと配信設定の日時を確認してください。';return {invalid:true,item:head};}
  for(let offset=0;offset<30;offset++){
   const tick=minute-offset*60000,jst=new Date(tick+9*3600000),at=jst.toISOString().slice(0,16),weekday=(jst.getUTCDay()+6)%7;
   if(!rule.days.includes(weekday)||!rule.times.includes(at.slice(11)))continue;
   if(tick<Date.parse(head.enqueued_at)||tick<Date.parse(rule.updated_at))break;
   const key=`${rule.app_id}|Threads|${at}`;
   if(state.ticks?.[key])break;
   const text=[head.body.trim(),head.hashtags.trim()].filter(Boolean).join('\n\n');
   if(!text||[...text].length>500||text.includes('［')||text.includes('］')||text.includes('[要確認]')){head.state='error';head.message='投稿文を確認してください。';return {invalid:true,item:head};}
   return {item:head,at,key,text};
  }
 }
 return null;
}
export async function run(store,publisher,{now=new Date(),dryRun=true}={}){
 const saved=await store.read(),state=saved.state;
 // A previously durable sending marker can be an interrupted successful POST.
 // Never automatically re-send it, and keep its queue blocked.
 let recovered=false;
 for(const item of state.items??[])if(item.state==='sending'){item.state='error';item.message='前回の送信結果が未確定です。自動では再送しません。';recovered=true;}
 const selected=choose(state,now);
 if(dryRun){await publisher.identity(account);return {dry_run:true,eligible:!!selected,account_verified:true};}
 if(!selected){if(recovered)await store.write(state,saved.sha);return {sent:0};}
 if(selected.invalid){await store.write(state,saved.sha);return {sent:0};}
 try{await publisher.identity(account);}catch{state.message='Threadsの接続を確認してください。投稿待ちは送信していません。';await store.write(state,saved.sha);return {sent:0,connection_error:true};}
 state.message='';
 state.ticks??={};state.ticks[selected.key]=selected.item.id;
 selected.item.state='sending';selected.item.dispatch_at=selected.at;state.updated_at=now.toISOString();
 const sha=await store.write(state,saved.sha); // MUST be durable before any SNS POST.
 let result;
 try{result=await publisher.publish(selected.text);}catch{
  selected.item.state='error';selected.item.message='送信結果を確認できません。SNSで確認するまで再送しません。';
  await store.write(state,sha);return {sent:0,uncertain:true};
 }
 selected.item.state='sent';selected.item.media_id=result.id;selected.item.url=result.url??'';selected.item.published_at=now.toISOString();selected.item.message='';
 // Keep idempotency receipts permanently; the original post remains in the PC database.
 selected.item.body='';selected.item.hashtags='';
 await store.write(state,sha); // Failure leaves the remote pre-POST sending marker intact.
 return {sent:1};
}
export function githubStore({repo,token,key,fetcher=fetch}){
 const endpoint=`https://api.github.com/repos/${repo}/contents/cloud-state.json`;
 const headers={Authorization:`Bearer ${token}`,'User-Agent':'PlusOneApp-Scheduler','X-GitHub-Api-Version':'2022-11-28','Content-Type':'application/json'};
 return {
  async read(){const response=await fetcher(endpoint,{headers});if(!response.ok)throw Error('State read failed');const raw=await response.json();return {sha:raw.sha,state:decryptState(JSON.parse(Buffer.from(raw.content.replace(/\s/g,''),'base64').toString()),key)};},
  async write(state,sha){const content=Buffer.from(JSON.stringify(encryptState(state,key))).toString('base64');const response=await fetcher(endpoint,{method:'PUT',headers,body:JSON.stringify({message:'Update encrypted scheduler state',content,sha})});if(!response.ok)throw Error('State changed or could not be saved');return (await response.json()).content.sha;}
 };
}

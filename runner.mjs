import {run,githubStore} from './scheduler.mjs';
const {GITHUB_REPOSITORY:repo,GITHUB_TOKEN:token,SCHEDULER_KEY:key,THREADS_TOKEN:threads}=process.env;
if(!repo||!token||!key||!threads){console.error('Scheduler credentials are missing.');process.exit(1);}
let liveToken=threads;
const publisher={
 async identity(expected){
  const response=await fetch('https://graph.threads.net/me?fields=id,username',{headers:{Authorization:`Bearer ${liveToken}`},signal:AbortSignal.timeout(15000)});
  if(!response.ok||(await response.json()).username!==expected)throw Error('Account connection needs attention');
 },
 async publish(text){
  const response=await fetch('https://graph.threads.net/me/threads',{method:'POST',headers:{Authorization:`Bearer ${liveToken}`},body:new URLSearchParams({media_type:'TEXT',text,auto_publish_text:'true'}),signal:AbortSignal.timeout(30000)});
  if(!response.ok)throw Error('Publication result is uncertain');const value=await response.json();if(!/^\d+$/.test(value.id??''))throw Error('Missing publication result');
  let url='';try{const result=await fetch(`https://graph.threads.net/${value.id}?fields=permalink`,{headers:{Authorization:`Bearer ${liveToken}`},signal:AbortSignal.timeout(15000)});if(result.ok){const link=(await result.json()).permalink;if(typeof link==='string'&&link.startsWith('https://'))url=link;}}catch{}
  return {id:value.id,url};
 }
};
try{
 const store=githubStore({repo,token,key}),dryRun=process.env.DRY_RUN!=='false';
 const saved=await store.read();liveToken=saved.state.threads_token||threads;
 if(!dryRun&&(!saved.state.token_refreshed_at||Date.now()-Date.parse(saved.state.token_refreshed_at)>7*86400000)){
  // Refreshed credentials are persisted only inside the authenticated encrypted state.
  const endpoint=new URL('https://graph.threads.net/refresh_access_token');endpoint.searchParams.set('grant_type','th_refresh_token');endpoint.searchParams.set('access_token',liveToken);
  try{const response=await fetch(endpoint,{signal:AbortSignal.timeout(15000)});if(response.ok){const value=await response.json();if(typeof value.access_token==='string'&&value.access_token){saved.state.threads_token=value.access_token;saved.state.token_refreshed_at=new Date().toISOString();await store.write(saved.state,saved.sha);liveToken=value.access_token;}}}catch{}
 }
 const result=await run(store,publisher,{dryRun});console.log(JSON.stringify(result));
}catch{console.error('Scheduler stopped. Check the account connection or encrypted queue sync. No automatic retry of an uncertain publication.');process.exit(1);}

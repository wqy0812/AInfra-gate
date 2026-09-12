'use strict';
let key = '', state = null, polling = false, sending = false, pending = null, lastRender = '';
function operationID(){if(crypto.randomUUID)return crypto.randomUUID();return Array.from(crypto.getRandomValues(new Uint8Array(16)),b=>b.toString(16).padStart(2,'0')).join('');}
const $ = id => document.getElementById(id);
const labels = {'backend-create':'新增后端','backend-update':'修改后端',switch:'切换',warmup:'预热',rollback:'自动回切',maintenance:'进入维护',restore:'恢复验证','confirm-drain':'确认排空',accepted:'已受理',warming:'预热验证',draining:'等待排空',completed:'已完成',failed:'失败',cancelled:'条件变化，已取消',interrupted:'已中断',drain_timeout:'已切流，未排空',rollback_requested:'正在启动回切',running:'进行中',json:'普通回答',sse:'流式回答',tool_call:'流式工具调用',tool_result:'工具结果续答',discovery:'模型发现'};
const label = text => labels[text] || text;
function el(tag, text, cls) { const n = document.createElement(tag); if(text !== undefined) n.textContent = text; if(cls) n.className = cls; return n; }
function error(message) { $('error').textContent = message; $('error').hidden = !message; }
function time(value) { return value ? new Date(value).toLocaleString('zh-CN',{hour12:false}) : '—'; }
async function api(path, body, method) {
  const response = await fetch('/admin/v1/' + path,{method:method || (body ? 'POST':'GET'),headers:{'Authorization':'Bearer '+key,'Content-Type':'application/json'},body:body ? JSON.stringify(body):undefined,cache:'no-store'});
  const data = await response.json();
  if(!response.ok) { const e = new Error(data.error?.message || '管理请求失败'); e.status=response.status;e.code=data.error?.code; throw e; }
  return data;
}
function disconnect(){if(typeof closeBackendEditor==='function')closeBackendEditor();key='';state=null;pending=null;lastRender='';$('key').value='';$('workspace').hidden=true;$('login').hidden=false;$('logout').hidden=true;$('connection').textContent='未连接';}
$('logout').onclick = disconnect;
$('login-form').onsubmit = async event => { event.preventDefault();error('');key=$('key').value;$('key').value='';await refresh(); };
async function refresh(){
  if(!key || polling) return; polling=true;
  try { const next=await api('status'); if(!key) return;state=next;$('login').hidden=true;$('workspace').hidden=false;$('logout').hidden=false;$('connection').textContent='已连接';render(); }
  catch(e){$('connection').textContent='连接异常';lastRender='';if(state){state=null;$('backend-new').disabled=true;$('backends').querySelectorAll('button').forEach(b=>b.disabled=true);}error(e.message);if(e.status===401) disconnect();}
  finally{polling=false;}
}
function actionButton(text,kind,name,disabled,primary=false){const b=el('button',text,primary?'':'quiet');b.dataset.action=kind;b.dataset.backend=name;b.disabled=disabled;b.onclick=()=>act(kind,name);return b;}
function render(){
  const s=state;if(!s)return;const busy=Boolean(s.busy)||sending;$('backend-new').disabled=busy;
  const signature=JSON.stringify(s)+sending;if(signature===lastRender)return;lastRender=signature;
  const focus=document.activeElement?.dataset;const focusedAction=focus?.action,focusedBackend=focus?.backend;
  $('active').textContent=s.active_backend;$('revision').textContent=s.route_version;
  $('rollback').textContent=s.rollback_armed ? '可回切至 '+s.rollback_candidate : '未启用 / 已锁定';
  $('alert').textContent=s.alert || '';$('alert').hidden=!s.alert;
  const cards=$('backends');cards.replaceChildren();
  for(const name of s.order){
    const b=s.backends[name],c=s.counts[name],active=name===s.active_backend,locked=b.maintenance||b.drain_unknown;
    const card=el('article',undefined,'backend-card'+(active?' active':''));
    const head=el('div',undefined,'card-head');head.append(el('h3',name),el('span',active?'接收新请求':b.maintenance?(c.probes?'恢复验证中':'维护锁定'):b.drain_unknown?'排空待确认':c.probes?'预热验证中':c.business?'正在排空':'备用','badge'+(active?' live':locked?' locked':'')));card.append(head);
    card.append(el('p',c.model || b.last_warmup?.model || '尚未发现模型','muted model'));
    const counts=el('div',undefined,'counts');for(const [v,l]of [[c.business,'业务占用'],[c.streams,'其中 SSE'],[c.probes,'探测占用']]){const d=el('div');d.append(el('strong',v),el('span',l));counts.append(d);}card.append(counts);
    const warm=b.last_warmup;card.append(el('p',warm ? `${warm.passed?'验证通过':'验证未通过'}${warm.fresh?'':' · 历史结果'} · ${time(warm.finished_at)}`:'配置已保存，尚未验证','muted'));
    if(c.abnormal) card.append(el('p',`异常结束 ${c.abnormal} 次 · ${{backend_failure:'后端故障',client_cancelled:'客户端取消',client_failure:'客户端传输中断',protocol_unknown:'流格式无法判定',upstream_rejected:'下游拒绝请求'}[c.last_error] || c.last_error || ''}`,'failed muted'));
    const actions=el('div',undefined,'actions');
    const edit=el('button','编辑','quiet');edit.disabled=busy||active||b.drain_unknown||c.business+c.probes>0;edit.onclick=()=>openBackendEditor(name);actions.append(edit);
    actions.append(actionButton('切到此端','switch',name,busy||active||locked,true),actionButton('仅预热','warmup',name,busy||locked));
    if(b.maintenance) actions.append(actionButton('恢复验证','restore',name,busy));
    else actions.append(actionButton('进入维护','maintenance',name,busy||active||b.drain_unknown||c.business+c.probes>0));
    if(b.drain_unknown&&!active)actions.append(actionButton('确认旧请求已结束','confirm-drain',name,busy||c.business+c.probes>0));
    card.append(actions);cards.append(card);
  }
  const ops=s.operations || [],op=ops.find(o=>o.id===s.busy)||ops.at(-1),body=$('operation-body');body.replaceChildren();
  $('operation-status').textContent=op?label(op.outcome):'暂无操作';
  if(op){
    const switching=['switch','rollback'].includes(op.kind);
    body.append(el('p',`${label(op.kind)}：${switching?op.source+' → ':''}${op.target}`));
    const stages=el('div',undefined,'stages');const route=switching?['accepted','warming','draining','completed']:['warmup','restore'].includes(op.kind)?['accepted','warming','completed']:['accepted','completed'];for(const stage of route)stages.append(el('span',label(stage),'stage'+(op.phase===stage?' current':'')));body.append(stages);
    body.append(el('p',`${time(op.started_at)}${switching?' · '+(op.route_committed?'路由已提交':'路由未提交'):''}`,'muted'));
    if(op.error)body.append(el('p',op.error,'failed'));
    const steps=el('ul',undefined,'step-list');for(const step of op.report?.steps||[]){const li=el('li');li.append(el('span',`${step.round?'第 '+step.round+' 轮 · ':''}${label(step.name)}`),el('span',step.passed?`通过 · ${Math.round(step.duration_ms)} ms${step.first_token_ms!==undefined?' · 首增量 '+Math.round(step.first_token_ms)+' ms':''}`:step.error,step.passed?'passed':'failed'));steps.append(li);}body.append(steps);
  }else body.append(el('p','从后端卡片发起预热或切流。','muted'));
  const history=$('history');history.replaceChildren();for(const o of ops.slice(-10).reverse()){const tr=el('tr');for(const value of [time(o.started_at),label(o.kind),o.target,label(o.outcome)])tr.append(el('td',value));history.append(tr);}
  if(focusedAction&&focusedBackend){const b=Array.from(cards.querySelectorAll('button')).find(b=>b.dataset.action===focusedAction&&b.dataset.backend===focusedBackend);if(b&&!b.disabled)b.focus({preventScroll:true});}
}
async function confirmAction(kind,name){
  if(!['maintenance','confirm-drain'].includes(kind))return true;
  $('confirm-title').textContent=kind==='maintenance'?'将 '+name+' 锁定为维护状态':'确认 '+name+' 的旧请求已结束';
  $('confirm-text').textContent=kind==='maintenance'?'锁定成功后，该端停止接受业务、探测和自动回切。收到成功结果后再重启对应服务。':'请先在下游确认网关重启前的请求和任务均已结束。本操作只确认排空状态，不会终止下游任务。';
  const d=$('confirm');d.returnValue='';d.showModal();return new Promise(resolve=>d.addEventListener('close',()=>resolve(d.returnValue==='accept'),{once:true}));
}
async function act(kind,name){
  if(!state||sending||state.busy)return;
  if(!await confirmAction(kind,name))return;
  if(!state||sending||state.busy)return;
  sending=true;error('');render();
  const id=pending&&pending.kind===kind&&pending.target===name?pending.id:operationID();
  const request=pending&&pending.id===id?pending.request:{operation_id:id,target:name,expected_revision:state.revision};
  pending={kind,target:name,id,request};
  try{await api(kind==='switch'?'switches':kind==='warmup'?'warmups':`backends/${encodeURIComponent(name)}/${kind}`,request);pending=null;}
  catch(e){error(e.message+(e.status?'':'；操作可能已受理，请先查看进度。'));if(e.status)pending=null;}
  finally{sending=false;await refresh();render();}
}
setInterval(refresh,2000);

// Optional browser integration. Authentication and server-side revision checks
// are exactly the same as those used by the visible controls.
if(document.modelContext?.registerTool){
  const lifetime=new AbortController();window.addEventListener('pagehide',()=>lifetime.abort(),{once:true});
  try{Promise.resolve(document.modelContext.registerTool({name:'get_gateway_status',title:'查看网关状态',description:'读取已连接网关的当前路由、在途请求和维护锁，并刷新控制台。',inputSchema:{type:'object',properties:{},additionalProperties:false},annotations:{readOnlyHint:true,untrustedContentHint:true},async execute(input){if(!input||typeof input!=='object'||Array.isArray(input)||Object.keys(input).length)throw new Error('此工具不接受参数');if(!key)throw new Error('请先在控制台连接管理服务');const s=await api('status');state=s;render();return {active_backend:s.active_backend,route_version:s.route_version,busy:s.busy||null,backends:s.backends,counts:s.counts,alert:s.alert||null};}},{signal:lifetime.signal})).catch(()=>{});}catch{}
}

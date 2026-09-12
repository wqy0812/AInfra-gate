'use strict';
let editingBackend = null, editorRevision = 0, backendPending = null, editorOpening = false;
const editorError = message => { $('backend-error').textContent=message; $('backend-error').hidden=!message; };
function closeBackendEditor(){
  $('backend-editor').close();$('backend-form').reset();$('backend-headers').replaceChildren();backendPending=null;editingBackend=null;$('backend-reload').hidden=true;editorError('');
}
$('backend-cancel').onclick=closeBackendEditor;
$('backend-reload').onclick=()=>{const name=editingBackend;closeBackendEditor();openBackendEditor(name);};
$('backend-editor').addEventListener('cancel',event=>{event.preventDefault();if(!sending)closeBackendEditor();});
$('backend-new').onclick=()=>openBackendEditor(null);
$('header-add').onclick=()=>addHeaderRow('',null);
function addHeaderRow(name, saved){
  const row=el('div',undefined,'header-row');
  function field(title,node){const l=el('label',title);l.append(node);row.append(l);return node;}
  const nameInput=field('请求头名称',el('input'));nameInput.value=name;nameInput.disabled=Boolean(saved);nameInput.dataset.field='name';
  const mode=field('操作',el('select'));mode.dataset.field='mode';
  for(const [value,title] of [['keep',saved?'保留已保存值':'不设置'],['value','替换为直接录入值'],['env','替换为环境变量'],['delete','删除']]){const option=el('option',title);option.value=value;mode.append(option);}
  const value=field('完整请求头值（不回显）',el('input'));value.type='password';value.autocomplete='new-password';value.dataset.field='value';
  const env=field('环境变量名称',el('input'));env.dataset.field='env';env.value=saved?.env||'';
  const prefix=field('环境变量值前缀（如 Bearer 加空格）',el('input'));prefix.dataset.field='prefix';prefix.value=saved?.prefix||'';
  function update(){value.disabled=mode.value!=='value';env.disabled=prefix.disabled=mode.value!=='env';}
  mode.onchange=update;update();$('backend-headers').append(row);
}
async function openBackendEditor(name){
  if(!state||sending||state.busy||editorOpening)return;
  editorOpening=true;error('');
  try{
    const list=await api('backends');if(!key)return;
    const view=name?list.backends[name]:null;if(name&&!view)throw new Error('后端已不存在，请刷新');
    editingBackend=name;editorRevision=list.revision;backendPending=null;$('backend-reload').hidden=true;editorError('');
    $('backend-form').reset();$('backend-headers').replaceChildren();
    $('backend-editor-title').textContent=name?'编辑 '+name:'新增后端';
    $('backend-name').value=name||'';$('backend-name').disabled=Boolean(name);
    const b=view?.backend||{};
    $('backend-url').value=b.url||'';$('backend-models-url').value=b.models_url||'';$('backend-model').value=b.preferred_model||'';
    $('backend-advanced').value=JSON.stringify({models_list_path:b.models_list_path||'data',model_id_path:b.model_id_path||'id',overrides:b.overrides||{},compatibility:b.compatibility||{metadata_values:'preserve',drop_fields:[]}},null,2);
    const headers=view?.credentials||{};for(const [header,saved] of Object.entries(headers))addHeaderRow(header,saved);
    if(!Object.keys(headers).some(h=>h.toLowerCase()==='authorization'))addHeaderRow('Authorization',null);
    $('backend-editor').showModal();
  }catch(e){error(e.message);}finally{editorOpening=false;}
}
$('backend-form').onsubmit=async event=>{
  event.preventDefault();if(!state||sending||state.busy)return;editorError('');
  let payload;
  try{
    const advanced=JSON.parse($('backend-advanced').value);
    const allowed=['models_list_path','model_id_path','overrides','compatibility'];
    if(!advanced||Array.isArray(advanced)||typeof advanced!=='object'||Object.keys(advanced).some(k=>!allowed.includes(k)))throw new Error('高级配置仅支持模型发现路径、overrides 和 compatibility');
    const credentials=Object.create(null), seen=new Set();
    for(const row of $('backend-headers').children){
      const read=key=>row.querySelector(`[data-field="${key}"]`).value;
      const mode=read('mode'),name=read('name').trim();
      if(mode==='keep')continue;
      if(!name||seen.has(name.toLowerCase()))throw new Error('请求头名称不能为空或重复');seen.add(name.toLowerCase());
      credentials[name]=mode==='delete'?{action:'delete'}:mode==='value'?{action:'replace',value:read('value')}:{action:'replace',env:read('env').trim(),prefix:read('prefix')};
    }
    payload={target:editingBackend||$('backend-name').value.trim(),backend:{...advanced,adapter:'openai',url:$('backend-url').value.trim(),models_url:$('backend-models-url').value.trim(),preferred_model:$('backend-model').value.trim()||null},credentials};
  }catch(e){editorError(e.message);return;}
  const signature=JSON.stringify(payload);
  if(backendPending&&backendPending.signature!==signature){editorError('上次保存结果尚未确认，请先恢复原输入并重试，或关闭表单后重新读取配置。');return;}
  const request=backendPending?.request||{...payload,expected_revision:editorRevision,operation_id:operationID()};
  backendPending={signature,request};sending=true;$('backend-save').disabled=$('backend-cancel').disabled=true;render();
  try{
    await api(editingBackend?`backends/${encodeURIComponent(editingBackend)}`:'backends',request,editingBackend?'PATCH':'POST');
    closeBackendEditor();$('backend-notice').textContent='配置已保存，尚未验证。可在后端卡片发起预热或切流。';$('backend-notice').hidden=false;
  }catch(e){
    editorError(e.message+(e.status?'':'；保存可能已完成，请保持输入不变并重试。'));
    if(e.status){backendPending=null;if(e.code==='stale_revision'){$('backend-reload').hidden=false;editorError('配置或管理状态已变化。请重新读取最新配置后编辑，避免覆盖其他修改；当前输入仍保留。');}}
  }finally{sending=false;$('backend-save').disabled=$('backend-cancel').disabled=false;await refresh();render();}
};

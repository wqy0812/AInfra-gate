import json,os,pathlib,subprocess,time,urllib.request,urllib.parse,signal,sys
root=pathlib.Path('/data2/monitoring/profile-v1-stage');os.chdir(str(root));run=root/('fixture-run-'+str(time.time_ns()));run.mkdir();processes=[];log=(root/'e2e.log').open('w');docker_name='aigate-profile-fixture-query'
def spawn(args,env=None):
 p=subprocess.Popen(args,stdout=log,stderr=log,env=env);processes.append(p);return p
def fetch(port,path,body=None,key=False):
 data=json.dumps(body).encode() if body is not None else None
 r=urllib.request.Request('http://127.0.0.1:'+str(port)+path,data=data,headers={'Content-Type':'application/json',**({'Authorization':'Bearer fixture-only'} if key else {})})
 with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(r,timeout=30) as response:return response.read()
def wait(port,path,key=False):
 for i in range(100):
  try:return fetch(port,path,key=key)
  except Exception:time.sleep(.1)
 raise RuntimeError('not ready '+str(port))
try:
 begin=time.time() # staged files already extracted; preserve the revised query implementation
 subprocess.check_call(['chmod','+x','aigate-profile-linux','aigate-profile-tests-linux'])
 subprocess.check_call(['./aigate-profile-tests-linux','-test.run','TestProfile','-test.timeout','30s'],stdout=log,stderr=log)
 config={'timeout_seconds':30,'backends':{'fixture':{'adapter':'openai','url':'http://127.0.0.1:18816/v1/chat/completions','models_url':'http://127.0.0.1:18816/v1/models'}},'profile':{'enabled':True,'listen':'127.0.0.1:18882','directory':str(run/'events'),'max_disk_bytes':16777216}}
 (root/'fixture-config.json').write_text(json.dumps(config));(root/'fixture-key').write_text('fixture-only')
 (root/'fixture-scrape.yml').write_text('global:\n  scrape_interval: 1s\nscrape_configs:\n- job_name: aigate\n  bearer_token_file: '+str(root/'fixture-key')+'\n  static_configs:\n  - targets: ["127.0.0.1:18882"]\n')
 spawn(['python3','fixture.py']);wait(18816,'/v1/models')
 env=dict(os.environ,AIGATE_PROFILE_KEY='fixture-only');gateway_process=spawn(['./aigate-profile-linux','serve','--config','fixture-config.json','--listen','127.0.0.1:18815'],env);wait(18815,'/health')
 bins='/data2/monitoring/release/vendor/bin/'
 spawn([bins+'victoria-metrics-prod','-httpListenAddr=127.0.0.1:18828','-storageDataPath='+str(run/'vm'),'-retentionPeriod=1d','-memory.allowedBytes=128MiB']);wait(18828,'/health')
 spawn([bins+'vmagent-prod','-httpListenAddr=127.0.0.1:18829','-promscrape.config='+str(root/'fixture-scrape.yml'),'-remoteWrite.url=http://127.0.0.1:18828/api/v1/write','-remoteWrite.tmpDataPath='+str(run/'buffer'),'-memory.allowedBytes=64MiB']);wait(18829,'/health')
 # Existing immutable runtime; only new query files are mounted into this isolated instance.
 image=subprocess.check_output(['docker','inspect','monitoring-api','--format','{{.Image}}']).decode().strip()
 subprocess.check_call(['docker','run','-d','--name',docker_name,'--network','host','--memory','512m','--cpus','1','-v',str(root/'monitoring/monitoring')+':/monitoring/monitoring-profile:ro','-v',str(root/'monitoring/monitoring/request_profile.py')+':/monitoring/monitoring/request_profile.py:ro','-v',str(root/'monitoring/monitoring/api.py')+':/monitoring/monitoring/api.py:ro','-e','VM_URL=http://127.0.0.1:18828','-e','STATE_DIR=/tmp/profile-fixture-state','--entrypoint','python',image,'-m','uvicorn','monitoring.api:app','--host','127.0.0.1','--port','18830','--no-access-log'],stdout=log)
 body={'messages':[{'role':'system','content':'synthetic private prefix'},{'role':'user','content':'synthetic private request'}]}
 fetch(18815,'/v1/chat/completions',body);time.sleep(4);start=time.time()
 for i in range(10):
  body['stream']=i%2==0;response=fetch(18815,'/v1/chat/completions',body);assert b'fixture answer' in response
 time.sleep(5);end=time.time()
 query='/api/request-profile/metrics?'+urllib.parse.urlencode({'start':start,'end':end,'backend':'fixture'})
 report=json.loads(fetch(18830,query));values=report['values'];scalar=lambda k:values[k][0]['value'] if values.get(k) else None
 (root/'debug-report.json').write_text(json.dumps(report)); (root/'debug-up.json').write_bytes(fetch(18828,'/api/v1/query?query=up')); (root/'debug-metrics.txt').write_bytes(fetch(18882,'/metrics',key=True))
 (root/'debug-raw.jsonl').write_bytes(fetch(18828,'/api/v1/export?'+urllib.parse.urlencode({'match[]':'aigate_prompt_tokens_total'})))
 (root/'debug-direct.json').write_bytes(fetch(18828,'/api/v1/query?'+urllib.parse.urlencode({'query':'sum(increase(aigate_prompt_tokens_total[60s]))','time':end,'latency_offset':'1ms','nocache':'1'})))
 assert scalar('prompt_tokens_total')==1000,scalar('prompt_tokens_total')
 assert scalar('completion_tokens_total')==100,scalar('completion_tokens_total')
 cold=json.loads(fetch(18830,'/api/request-profile/metrics?'+urllib.parse.urlencode({'start':begin,'end':end,'backend':'fixture'})))
 assert cold['values']['prompt_tokens_total'][0]['value']==1100,cold['values']['prompt_tokens_total']
 assert abs(report['cache_token_ratio']-.8)<1e-9
 assert scalar('prefix_repeated')==10,scalar('prefix_repeated')
 gateway_process.terminate();gateway_process.wait(timeout=10)
 spawn(['./aigate-profile-linux','serve','--config','fixture-config.json','--listen','127.0.0.1:18815'],env);wait(18815,'/health');body['stream']=False;fetch(18815,'/v1/chat/completions',body)
 time.sleep(6);end=time.time()
 restored=json.loads(fetch(18830,'/api/request-profile/metrics?'+urllib.parse.urlencode({'start':begin,'end':end,'backend':'fixture'})))
 assert restored['values']['prompt_tokens_total'][0]['value']==1200,restored['values']['prompt_tokens_total']
 status=json.loads(fetch(18882,'/profile/v1/status',key=True));job=json.loads(fetch(18882,'/profile/v1/analyses',{'start':max(status['retained_start'],begin),'end':end},True))
 for i in range(60):
  job=json.loads(fetch(18882,'/profile/v1/analyses/'+job['id'],key=True))
  if job['state'] in ('completed','failed'):break
  time.sleep(.1)
 assert job['state']=='completed',job
 assert job['result']['requests']==12,job
 assert job['result']['prefix_levels']['1']['families']==1
 for path in (run/'events').glob('events-*.jsonl'):
  assert 'synthetic private' not in path.read_text()
 result={'gateway_vm_query':True,'input_tokens':scalar('prompt_tokens_total'),'output_tokens':scalar('completion_tokens_total'),'cache_ratio':report['cache_token_ratio'],'detail_requests':job['result']['requests'],'private_payload_absent':True,'graceful_restart_input_total':1200,'source':'isolated synthetic HTTP + VM + vmagent'}
 (root/'e2e-result.json').write_text(json.dumps(result,indent=2));print(json.dumps(result))
finally:
 subprocess.run(['docker','logs',docker_name],stdout=log,stderr=log)
 subprocess.run(['docker','rm','-f',docker_name],stdout=log,stderr=log)
 for p in reversed(processes):
  p.terminate()
  try:p.wait(timeout=10)
  except subprocess.TimeoutExpired:p.kill();p.wait()
 log.close()

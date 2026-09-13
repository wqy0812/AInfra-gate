"""Run via SSH MCP on test1/test2; clone only the explicitly owned containers."""
import hashlib,http.client,json,os,pathlib,shutil,socket,sqlite3,subprocess,sys,time,urllib.request
mode=sys.argv[1];root=pathlib.Path('/data2/monitoring/profile-v1-stage' if mode=='monitoring' else '/data2/code-eval/releases/gateway-profile-v1-stage');os.chdir(str(root));root.chmod(0o700)
def cmd(*a):return subprocess.check_output(a,stderr=subprocess.STDOUT).decode()
def inspect(n):return json.loads(cmd('docker','inspect',n))[0]
def get(port,path):
 with urllib.request.build_opener(urllib.request.ProxyHandler({})).open('http://127.0.0.1:'+str(port)+path,timeout=25) as r:return json.load(r)
class Connection(http.client.HTTPConnection):
 def connect(self):self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);self.sock.connect('/var/run/docker.sock')
def replace(name,image,env=None,bind=None):
 v=inspect(name);old=name+'-profile-v1-rollback';assert subprocess.call(['docker','inspect',old],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)!=0
 fields=('User','ExposedPorts','Env','Cmd','Healthcheck','Volumes','WorkingDir','Entrypoint','Labels','StopSignal','StopTimeout','Hostname')
 c={k:v['Config'][k] for k in fields if k in v['Config']};c.update(Image=image,HostConfig=v['HostConfig'])
 env=env or {};c['Env']=[x for x in c.get('Env',[]) if x.split('=',1)[0] not in env and not x.startswith('PROFILE_ENABLED=')]+[k+'='+val for k,val in env.items()]
 if bind:c['HostConfig']['Binds']=list(c['HostConfig'].get('Binds') or [])+[bind]
 cmd('docker','stop',name);cmd('docker','rename',name,old)
 try:
  connection=Connection('localhost');connection.request('POST','/v1.39/containers/create?name='+name,json.dumps(c),{'Content-Type':'application/json'});r=connection.getresponse();body=r.read();connection.close();assert r.status==201,body
  cmd('docker','start',name)
 except BaseException:
  subprocess.call(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);cmd('docker','rename',old,name);cmd('docker','start',name);raise
 return old

def build(name,relative,dest,tag,remove=False):
 source=inspect(name)['Image'];folder=root/(name+'-build');folder.mkdir(exist_ok=True)
 for f in relative:
  target=folder/f;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(root/mode/f,target)
 dockerfile='FROM '+source+'\n'+''.join('COPY '+f+' '+dest+'/'+f+'\n' for f in relative)
 if remove:dockerfile+='RUN rm -f /app/app/request_profiles.py /app/app/profile_reader.py /app/app/profile_stats.py /app/scripts/import_request_profile.py && rm -f /app/app/__pycache__/request_profiles.* /app/app/__pycache__/profile_reader.* /app/app/__pycache__/profile_stats.*\n'
 (folder/'Dockerfile').write_text(dockerfile)
 with (root/(name+'-build.log')).open('w') as out:subprocess.check_call(['docker','build','-t',tag,str(folder)],stdout=out,stderr=out)
 return tag

subprocess.check_call(['tar','xzf','profile-code.tar.gz'])
if mode=='monitoring':
 image=build('monitoring-api',['monitoring/api.py','monitoring/request_profile.py'],'/monitoring','monitoring-api:gateway-profile-v1')
 cmd('docker','run','--rm','--network','none','--entrypoint','python',image,'-c','import monitoring.api; print("imports OK")')
 old=replace('monitoring-api',image)
 try:
  for i in range(40):
   try:
    now=time.time();r=get(18430,'/api/request-profile/metrics?start='+str(now-300)+'&end='+str(now));assert r['source']=='victoriametrics';break
   except Exception:time.sleep(.5)
  else:raise RuntimeError('new monitoring profile endpoint unavailable')
 except BaseException:
  cmd('docker','rm','-f','monitoring-api');cmd('docker','rename',old,'monitoring-api');cmd('docker','start','monitoring-api');raise
 cmd('docker','rm',old)
 for f in ['monitoring/api.py','monitoring/request_profile.py']:
  target=pathlib.Path('/data2/monitoring/release')/f;shutil.copy2(root/'monitoring'/f,target)
 # Existing read-only /config mount exposes the credential only to the collector.
 deploy=pathlib.Path('/data2/monitoring/release/deploy');shutil.copy2(deploy/'scrape.yml',root/'scrape-before.yml');shutil.copy2(root/'aigate-profile-observer.key',deploy/'aigate-profile-key');(deploy/'aigate-profile-key').chmod(0o600)
 shutil.copy2(root/'monitoring/deploy/scrape.yml',deploy/'scrape.yml')
 request=urllib.request.Request('http://127.0.0.1:18429/-/reload',data=b'')
 urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request,timeout=10).read()
 print(json.dumps({'monitoring_api':inspect('monitoring-api')['Id'],'vmagent_reload':True}))
else:
 names=['code-eval-web','code-eval-engine'];files=['app/main.py','app/config.py','app/web.py','app/web_read.py','app/web_export.py','app/request_profile_api.py','app/static/index.html','app/static/style.css','app/static/request-profiles.js'];images={}
 for name in names:
  assert inspect(name)['Config']['Labels'].get('code-eval.owner')=='platform'
  images[name]=build(name,files,'/app','code-eval-platform:gateway-profile-v1-'+name.rsplit('-',1)[1],True)
  cmd('docker','run','--rm','--network','none','-e','DATA_DIR=/tmp/profile-import','--entrypoint','python',images[name],'/app/app/compat.py','python','-c','from app import main,web; print("imports OK")')
 assert not get(18080,'/api/task-capacity')['active'],'Evaluation active; deployment deferred'
 dbpath='/data2/code-eval/data/platform.db'
 with sqlite3.connect(dbpath) as db:
  assert not db.execute("SELECT 1 FROM runs WHERE status IN ('running','stopping')").fetchone(),'Active runs'
  assert not db.execute("SELECT 1 FROM runs WHERE status IN ('queued','scheduled') AND json_extract(config,'$.requires_key')=1").fetchone(),'Queued volatile key'
 secret=pathlib.Path('/data2/code-eval/config/aigate-profile-key');shutil.copy2(root/'aigate-profile-observer.key',secret);secret.chmod(0o600)
 backups=[]
 try:
  for name in names:
   backups.append((name,replace(name,images[name],{'GATEWAY_PROFILE_URL':'http://122.235.251.106:18082','GATEWAY_PROFILE_KEY_FILE':'/run/secrets/aigate-profile-key'},str(secret)+':/run/secrets/aigate-profile-key:ro')))
  for i in range(60):
   try:
    health=get(18080,'/api/health');assert health['components']['engine']['status']=='ok'
    now=time.time();r=get(18080,'/api/request-profile/metrics?start='+str(now-300)+'&end='+str(now));assert r['source']=='victoriametrics';break
   except Exception:time.sleep(.5)
  else:raise RuntimeError('new platform health/profile endpoint unavailable')
 except BaseException:
  for name,old in reversed(backups):cmd('docker','rm','-f',name);cmd('docker','rename',old,name);cmd('docker','start',name)
  raise
 for name,old in backups:cmd('docker','rm',old)
 print(json.dumps({'platform_containers':{name:inspect(name)['Id'] for name in names},'profile_database_writer_removed':True}))

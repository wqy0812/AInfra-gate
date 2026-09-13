import json,os,pathlib,shutil,subprocess,time,urllib.request
root=pathlib.Path('/data4/aigate');release=root/'releases/gateway-profile-v1';envPath=root/'admin.env';configPath=root/'config.json';envText=envPath.read_text();env=dict(line.split('=',1) for line in envText.splitlines() if '=' in line and not line.startswith('#'))
def api(path):
 r=urllib.request.Request('http://127.0.0.1:8081'+path,headers={'Authorization':'Bearer '+env['AIGATE_ADMIN_KEY']});return json.load(urllib.request.urlopen(r,timeout=5))
def cmd(*args):return subprocess.check_output(args,stderr=subprocess.STDOUT).decode()
status=api('/admin/v1/status');assert not status.get('busy'),'Management operation active'
for name,c in status['counts'].items():
 for key,value in c.items():
  if key in ('business','streams','probes','in_flight','active') and isinstance(value,(int,float)):assert value==0,(name,key,value)
print(json.dumps({'active_backend':status['active_backend'],'counts':status['counts']}))
config=json.loads(configPath.read_text());beforeConfig=configPath.read_bytes();beforeEnv=envPath.read_bytes();old=(root/'current').resolve();backup=root/'backups/gateway-profile-v1';backup.mkdir(exist_ok=False)
shutil.copy2(configPath,backup/'config.json');shutil.copy2(envPath,backup/'admin.env');(backup/'previous-release.txt').write_text(str(old))
key=(release/'aigate-profile-observer.key').read_text().strip();env['AIGATE_PROFILE_KEY']=key
config['profile']={'enabled':True,'listen':'122.235.251.106:18082','directory':'/data4/aigate/request-profile','api_key_env':'AIGATE_PROFILE_KEY','retention_hours':72,'max_disk_bytes':10737418240}
(release/'config-check.json').write_text(json.dumps(config));(release/'config-check.json').chmod(0o600)
shutil.copy2(release/'aigate-profile-linux',release/'aigate');(release/'aigate').chmod(0o755)
checkEnv=dict(os.environ,**env);subprocess.check_call([str(release/'aigate'),'check','--config',str(release/'config-check.json')],env=checkEnv)
# SIGTERM drains existing requests under the existing 31-minute service timeout.
cmd('systemctl','stop','aigate')
try:
 state=pathlib.Path(config['management']['state_file']);shutil.copy2(state,backup/'management-state.json')
 configPath.write_text(json.dumps(config,ensure_ascii=False,indent=2));configPath.chmod(0o600)
 envPath.write_text('\n'.join(line for line in envText.splitlines() if not line.startswith('AIGATE_PROFILE_KEY='))+'\nAIGATE_PROFILE_KEY='+key+'\n');envPath.chmod(0o600)
 temp=root/'current-profile-next';temp.symlink_to(release);temp.replace(root/'current');cmd('systemctl','start','aigate')
 for i in range(50):
  try:
   after=api('/admin/v1/status');assert after['active_backend']==status['active_backend'];assert after['route_version']==status['route_version']
   req=urllib.request.Request('http://122.235.251.106:18082/profile/v1/status',headers={'Authorization':'Bearer '+key});profile=json.load(urllib.request.urlopen(req,timeout=3));break
  except Exception:time.sleep(.2)
 else:raise RuntimeError('profile endpoint startup failed')
 print(json.dumps({'gateway':'active','profile_enabled':True,'active_backend':after['active_backend'],'route_version':after['route_version'],'profile_schema':profile['schema']}))
except BaseException:
 subprocess.call(['systemctl','stop','aigate']);configPath.write_bytes(beforeConfig);envPath.write_bytes(beforeEnv)
 temp=root/'current-profile-rollback';temp.symlink_to(old);temp.replace(root/'current');cmd('systemctl','start','aigate');raise

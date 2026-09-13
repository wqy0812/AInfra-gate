import hashlib,json,os,pathlib,shutil,sys
root=pathlib.Path('/data3/remove-request-collection-20260913/retired-sampling')
targets=[p for p in [pathlib.Path('/data2/request-model'),pathlib.Path('/data2/request-model-0828')]+list(root.glob('request-model*')) if p.exists()]
# Inspect all live processes, excluding this collector and its ancestors.
ancestors=set();pid=os.getpid()
while pid>1:
 ancestors.add(pid)
 try:pid=int(pathlib.Path('/proc/'+str(pid)+'/stat').read_text().split(')')[1].split()[1])
 except Exception:break
refs=[]
for proc in pathlib.Path('/proc').iterdir():
 if not proc.name.isdigit() or int(proc.name) in ancestors:continue
 for f in [proc/'cwd']+list((proc/'fd').glob('*')):
  try:dest=os.readlink(str(f))
  except OSError:continue
  if any(dest==str(p) or dest.startswith(str(p)+'/') for p in targets):refs.append({'pid':proc.name,'fd':str(f),'path':dest})
 files=[]
for p in targets:
 for f in ([p] if p.is_file() else p.rglob('*')):
  if f.is_file():files.append({'path':str(f),'bytes':f.stat().st_size})
report={'targets':[str(p) for p in targets],'files':files,'active_references':refs,'applied':False}
out=pathlib.Path('/data3/gateway-profile-v1-retirement.json')
if '--apply' in sys.argv:
 assert not refs,'Live references remain; retain operational files separately'
 for p in targets:
  if p.is_dir():shutil.rmtree(str(p))
  else:p.unlink()
 report['applied']=True
out.write_text(json.dumps(report,indent=2));out.chmod(0o600)
print(json.dumps({'targets':report['targets'],'file_count':len(files),'active_references':refs,'applied':report['applied']}))

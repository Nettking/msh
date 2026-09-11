"""Read-only isolate the guarded resolved-configuration discrepancy."""
import datetime,hashlib,json,os,pathlib,subprocess
B=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910');R=B/'source';S=B/'inputs/c03-activation'
N='0536f03d67eb277e11573c2188d8e820399627e3';M='9b286f931497bf6291e215f6340443c5162826b0'
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_','OLLAMA_','PYTHON'))};env.update(json.loads((S/'supported-start-0536f03d-distinct-images.environment.private.json').read_text()))
def run(args):return subprocess.check_output(args,cwd=R,env=env,text=True,timeout=30).strip()
assert run(['git','rev-parse','HEAD'])==M and not run(['git','status','--porcelain','--untracked-files=all'])
assert run(['git','diff',N,M,'--','docker-compose.yml']).count('\n+')==2
# The proved sole base Compose change is the new Flask default port. Render with
# original N environment then remove that addition to reconstruct old resolution.
old=json.loads(run(['docker','compose','config','--format','json']));old['services']['flask']['environment'].pop('FCP_AUTO_JOIN_PORT')
env['FCP_BUILD_COMMIT']=M
new=json.loads(run(['docker','compose','config','--format','json']))
expected=json.loads(json.dumps(old))
for name in ('flask','relay','recorder'):
 expected['services'][name]['image']='fcp-v1-nitro-'+name+':'+M
 expected['services'][name]['build']['args']['FCP_BUILD_COMMIT']=M
 expected['services'][name]['environment']['FCP_BUILD_COMMIT']=M
expected['services']['flask']['environment']['FCP_AUTO_JOIN_PORT']='5151'
delta=[]
def walk(a,b,path=''):
 if isinstance(a,dict) and isinstance(b,dict):
  for k in sorted(set(a)|set(b)):walk(a.get(k),b.get(k),path+'/'+k)
 elif a!=b:
  def expose(v):
   if v is None or isinstance(v,(bool,int)):return v
   if isinstance(v,str) and (v in (N,M,'5151') or v.startswith('fcp-v1-nitro-')):return v
   return {'type':type(v).__name__,'sha256':hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()}
  delta.append({'path':path,'expected':expose(a),'actual':expose(b)})
walk(expected,new)
print(json.dumps({'observed_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'candidate':M,'host':'nitro','source_clean':True,'unexpected_to_audit_delta':delta,'state_changed':False,'protected_recorder_data_untouched':True,'physical_acceptance':False},indent=2))

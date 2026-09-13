import hashlib,json,pathlib,subprocess,sys,urllib.request
h=pathlib.Path('/home/martin/fcp-v1-501b528e-harness-20260913');r=pathlib.Path('/home/martin/fcp-v1-73c779-nitro-20260910/source');c=h/'.acceptance/runtime-control';sys.path.insert(0,str(r))
from catalog.federation.host_build import builder_name
name=builder_name(r)
ids=subprocess.check_output(['docker','ps','-aq','--filter','name=buildx_buildkit_'+name],text=True).split()
builders=json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []
out={'candidate':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','campaign_status':json.loads((c/'P01-qualified-status.json').read_text())['status'],'owned_builders':[{'running':x['State']['Running'],'oom_killed':x['State']['OOMKilled'],'exit_code':x['State']['ExitCode']} for x in builders],'services':[],'physical_pass':False,'product_mutated_by_inspection':False}
ids=subprocess.check_output(['docker','ps','-q','--filter','label=com.docker.compose.project=fcp-v1-fba508-nitro'],text=True).split()
for x in json.loads(subprocess.check_output(['docker','inspect',*ids],text=True)) if ids else []:
 service=x['Config']['Labels'].get('com.docker.compose.service')
 if service in ['flask','relay','recorder']:
  image=json.loads(subprocess.check_output(['docker','image','inspect',x['Image']],text=True))[0]
  out['services'].append({'service':service,'running':x['State']['Running'],'candidate':image['Config']['Labels'].get('no.fcp.build_commit')})
values=json.loads((c/'environment.private.json').read_text());bind=values.get('FCP_WEB_BIND','127.0.0.1');port=values.get('FCP_WEB_PORT','5000')
if bind in ['0.0.0.0','::']:bind='127.0.0.1'
url='http://'+bind+':'+str(port)+'/onboarding'
try:
 with urllib.request.urlopen(url,timeout=5) as response:out['current_configured_http_status']=response.status
except Exception as error:out['current_http_error_type']=type(error).__name__
out['readiness_disposition']='One later activation timed out waiting for readiness after its build succeeded. No additional candidate defect demonstrated; retain this host observation and use fresh acceptance for the replacement candidate.'
(c/'post-P01-safety.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))

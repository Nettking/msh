"""Retain terminal physical evidence without exporting private onboarding inputs."""
import datetime,hashlib,json,pathlib,sys,zipfile
root=pathlib.Path(__file__).parent
h=pathlib.Path('C:/wsl/fcp-v1-1aac6148-main-20260913');c=h/'.acceptance/onboarding-test';e=h/'evidence/v1-physical'
sha='1aac6148759d7b2fd488ec26b97e1a786bdafa80'
assert not (root/'onboarding-disposition.json').exists(),'Preserve the previous snapshot; review before exporting another'
status=json.loads((c/'bootstrap-status.json').read_text())
assert status['status']=='STOPPED' and status['phase']=='VERIFY' and status['tailnet-start']['exit_code']==0
assert status['product_federation_state']=='connected'
verification=json.loads((c/'tailnet-verify.json').read_text());assert verification['verdict']=='fail'
for name in ['preparation.json','explicit-volume-review.json','bootstrap-original-volume-refusal.json','bootstrap-status.json','tailnet-prepare.json','tailnet-action.json','tailnet-verify.json','host-availability.json']:
 with (root/('onboarding-'+name)).open('xb') as out:out.write((c/name).read_bytes())
with (root/'windows-P05-path-containment-status.json').open('xb') as out:out.write((h/'.acceptance/runtime-control/P05-path-containment-status.json').read_bytes())
sys.path.insert(0,str(h))
from scripts.acceptance.v1_physical_campaign import scenario_status
progress={scenario:scenario_status(e,scenario,expected_commit=sha) for scenario in ['P03','P05']}
assert progress['P03']['failing_assertions']==['start-tailscale-cmd']
archive=root/'P03-P05-onboarding-evidence.zip'
with zipfile.ZipFile(archive,'x',zipfile.ZIP_DEFLATED) as z:
 for p in [e/'campaign.json',*sorted((e/'hosts').glob('*.json')),*sorted((e/'observations/P03').glob('*.json')),*sorted((e/'observations/P05').glob('*.json'))]:z.writestr(p.relative_to(e).as_posix(),p.read_bytes())
report={'candidate':sha,'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'P03_assertions':'7/8 PASS; tailnet verification FAIL retained','P05_assertions_pass':['ollama-absence','malformed-timestamp-path'],'progress':progress,'archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'onboarding':{'scope':'separate new empty owned workbench','initial_refusal':'Product safely refused ambiguous coordinator volumes before activation. Explicit project-scoped selection corrected the fixture; original refusal retained.','owner_initialization_exit_code':0,'product_federation_state':'connected','unmodified_tailnet_launcher_exit_code':0,'supplemental_evidence':'Execution reached and recorded the operator action only after checking three exact candidate images and actual tailnet HTTP200 from Windows and native Nitro.','verifier_failure':'All Compose rows were excluded as unbound before their build labels were compared. Expected working-directory metadata requires comparison with the actual container labels.','classification':'Unresolved acceptance binding discrepancy; no demonstrated candidate defect. Do not promote the launcher assertion to PASS.','recovery':'No repeated launcher, account initialization, build, reset, or verifier recovery.'},'windows_host_observation':{'docker_read_only_queries':'Docker Desktop returned HTTP500 for container list and inspect; bounded metadata read unavailable.','http_read_only_queries':'Both the original owned workbench and isolated workbench timed out with the unchanged10s deadline after the successful launcher proof.','classification':'Current host/runtime unavailability; candidate causation unresolved and not demonstrated.','host_free_bytes':106952519680,'host_free_memory_kib':4773096,'daemon_restarted':False,'next_action':'When Docker responds, compare the exact project/working-directory labels with the binding. Correct only a demonstrated fixture metadata error, then verify the existing action once without relaunching.'},'active_executor':None,'physical_pass':False,'P07':'NOT STARTED; approved real agents and aged non-protected corpus pending','P12':'NOT STARTED; approved real agents and aged non-protected corpus pending','protected_data_accessed':False,'AQG_requested':False}
(root/'onboarding-disposition.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps({'P03':report['P03_assertions'],'P05':report['P05_assertions_pass'],'archive_sha256':report['archive_sha256'],'active_executor':None,'physical_pass':False}))

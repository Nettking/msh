"""Retain the completed checked-in impact result and compare reviewed execution code."""
import datetime,hashlib,json,pathlib,subprocess,sys
here=pathlib.Path(__file__).resolve().parent
root=pathlib.Path('C:/wsl/fcp-v1-9b286f93-merged-main-20260911')
N='0536f03d67eb277e11573c2188d8e820399627e3';M='9b286f931497bf6291e215f6340443c5162826b0'
def git(*args):return subprocess.check_output(['git',*args],cwd=root,text=True).strip()
assert git('rev-parse','HEAD')==M and not git('status','--porcelain','--untracked-files=all')
planpath=here/'main-9b286f93-revalidation-plan.json';plan=json.loads(planpath.read_text(encoding='utf-8-sig'))
assert plan['safe'] is False and plan['carry_forward_scenarios']==[] and len(plan['impacted_scenarios'])==12
assert plan['unknown_paths']==['catalog/federation/tailscale_host_discovery.py']
reviewed=['scripts/acceptance/v1_physical_automation.py','scripts/acceptance/v1_physical_runner.py',
 'scripts/acceptance/v1_physical_probes.py','scripts/acceptance/v1_physical_campaign.py',
 'scripts/acceptance/v1_prepare_windows.ps1','scripts/acceptance/v1_prepare_linux.sh',
 'docs/implementation/v1_physical_campaign_automation.md','docs/implementation/v1_physical_campaign.md',
 'docs/implementation/v1_b01_b09_physical_acceptance.md']
assert not git('diff','--name-only',N,M,'--',*reviewed)
receipt={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'baseline':N,'candidate':M,
 'command':[sys.executable,'-B','-m','catalog.federation.tests.cf7_acceptance.physical_revalidation','--baseline',N,'--candidate',M,'--repo-root','.'],
 'cwd':str(root),'python_version':sys.version.split()[0],'clean_exact_checkout':True,'exit_code':2,
 'plan_sha256':hashlib.sha256(planpath.read_bytes()).hexdigest(),
 'outcome':'ALL_12_SCENARIOS_REQUIRE_FRESH_OBSERVATION','carry_forward_authorized':False,
 'prior_physical_passes':0,'unknown_paths':plan['unknown_paths'],
 'policy_interpretation':'Exit2 denies carry-forward for unknown discovery path. Full fresh observations satisfy fail-closed policy. No impact-map weakening, product repair or software rerun is warranted.',
 'unchanged_execution_contracts':{p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in reviewed},
 'review_delta':'Only startup readiness and Compose responder-port propagation affect activation configuration; inspect resolved mounts/project/port and actual M source on each host before launch.',
 'automate_safety':'No blanket automate. All ten full untimed scenarios contain stateful/disruptive work; use runtime-bound individual probes/actions after preconditions and isolation.',
 'physical_acceptance':False,'state_changed':'local audit receipt only','protected_recorder_data_untouched':True}
(here/'main-9b286f93-revalidation-review.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({k:receipt[k] for k in ('candidate','outcome','exit_code','clean_exact_checkout','automate_safety')}))

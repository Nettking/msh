import datetime,json,pathlib,sys
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();root=pathlib.Path('handoff/diagnostics');snap=json.loads(sorted(root.glob('pr468-ba44100e-auto-*.json'))[-1].read_text());billing=next(r for r in snap['runs'] if r['path'].endswith('docs-portal.yml'))
for j in billing['jobs']:
 assert not j['steps'] and not j['runner_id'];j['annotations']=api(j['check_run_url'].split('/repos/Nettking/msh')[1]+'/annotations');assert any('spending limit' in a['message'] for a in j['annotations'])
(root/'pr468-ba44100e-hosted-admission.json').write_text(json.dumps({'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'head_sha':snap['head_sha'],'classification':'EXISTING_HOSTED_BILLING_ADMISSION_FAILURE_NO_EXECUTION','run':billing},indent=2)+'\n',encoding='utf-8')
old=json.loads((root/'ci-migration-auto-runs-20260912T125703.json').read_text());oldjobs=[j for r in old['runs'] for j in r['jobs'] if j['id'] in [103543444403,103543444443,103544075720]]
current=next(j for r in snap['runs'] for j in r['jobs'] if j['id']==103557723358)
runner=api('/actions/runners?per_page=100')['runners'];selected=[r for r in runner if r['id'] in [28,29,30,31]]
print(json.dumps({'old_D10_runner_ids':sorted(set(j['runner_id'] for j in oldjobs)),'current_Beast_runner_id':current['runner_id'],'current_checkout_step':[s for s in current['steps'] if s['name']=='Run actions/checkout@v4']}))
o={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'head_sha':snap['head_sha'],'status':'LATER_CHECKOUT_SUCCEEDED_WITHOUT_AGENT_HOST_MUTATION','old_D10_runner_ids':sorted(set(j['runner_id'] for j in oldjobs)),'current_job':current,'runner_metadata':selected,'interpretation':'Same named Beast CI path later completed checkout/setup; original access failure remains valid. No manual recovery was applied, and the underlying ACL/open-handle cause is unconfirmed. Avoid speculative cleanup while current jobs run.','AQG_note':'Windows runner30 and Linux31 are distinct and now executing jobs. Setup progress is observed; label/online state alone is not release-pool qualification. Agent made no admission/configuration changes.','protected_recorder_data':'UNTOUCHED'}
(root/'D10-later-checkout-observation.json').write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'runners':[{'id':r['id'],'name':r['name'],'os':r['os'],'status':r['status'],'labels':[l['name'] for l in r['labels']]} for r in selected]}))

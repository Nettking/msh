import sys,json,pathlib
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();p=pathlib.Path('handoff/diagnostics/ci-f7-js-only-trigger-proof.json');o=json.loads(p.read_text())
for r in o['runs']:
    if r['conclusion']=='failure':
        r['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs']
        for j in r['jobs']:
            j['annotations']=api(j['check_run_url'].split('/repos/Nettking/msh')[1]+'/annotations')
            assert not j['steps'] and j['runner_id']==0
            assert any('spending limit' in a['message'] for a in j['annotations'])
        r['failure_classification']='CI_INFRASTRUCTURE_HOSTED_BILLING_ADMISSION_NO_TESTS'
        print(json.dumps({'run':r['id'],'jobs':len(r['jobs']),'classification':r['failure_classification']}))
p.write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
p=pathlib.Path('handoff/QUALIFICATION_COORDINATION.md');s=p.read_text(encoding='utf-8');s=s.replace('Next independent action: create the planned disposable\nJS-only canary PR against this migration branch, retain its actual event/run/source\nproof, then review two complete green native F7 executions before any retirement.','Disposable JS-only PR469 is open against this migration branch. Its automatic\nF7 run34690234286 proves the path event, with unchanged workflow blob and just\none inert JS comment. Native matrix queued at11:08Z. Never merge PR469. Next:\nreview both F7 runs (34689990990 and34690234286) and their native logs/JUnit.\nTwo complete green native executions are still required before retirement.')
s+='''\n## 2026-09-12T11:09Z — actual JS-only trigger demonstrated\n\nDisposable draft [PR469](https://github.com/Nettking/msh/pull/469), head\n660bf23269893305c7e8ffcc4c910a7efaed567d, targets PR468 branch at f3abe545.\nGitHub confirms exactly one changed file and one inert comment, with identical\nF7 workflow blob143db0b43c870bc5791c3629e34fd61a72293c47 at base/head.\nActual pull_request event started F7 run34690234286 (both native jobs queued).\nReceipt: diagnostics/ci-f7-js-only-trigger-proof.json. This proves triggering,\nnot green equivalence or acceptance. Do not merge canary or deploy its source.\nLegacy F7.7 run34690234289 hit the existing hosted billing admission failure;\nzero-step/runner0 annotations retained. Automatic replacement/release runs remain\nuntouched. Next: inspect the two bounded F7 completions near their expected finish,\nretain/review native source/command/JUnit proofs, and keep long release checks hourly.\nNo protected Recorder or physical state changed.\n''';p.write_text(s,encoding='utf-8')
p=pathlib.Path('handoff/diagnostics/ci-migration-equivalence-matrix.json');o=json.loads(p.read_text());o['implementation']['js_only_event_verified']=True;o['implementation']['js_only_canary_pr']=469;o['implementation']['js_only_canary_head']='660bf23269893305c7e8ffcc4c910a7efaed567d';o['implementation']['js_only_canary_run']=34690234286;p.write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')

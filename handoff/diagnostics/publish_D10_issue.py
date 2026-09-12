import sys,json,pathlib
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();title='ci: Beast checkout blocked by inaccessible runner-owned pytest cache (D10)'
issues=api('/issues?state=open&per_page=100');existing=[i for i in issues if i['title']==title]
body='''D10 is a confirmed host/environment checkout blocker. The native F7 replacement proof is separately complete (2/2); this is not a product test failure or physical acceptance observation.

Three self-hosted Windows jobs on Beast failed before completing candidate checkout:

- PR468 intended head `f3abe5452db2f21593a688bc62bc5f4b22d5c40e`: [capability-product job103543444403](https://github.com/Nettking/msh/actions/runs/34689991000/job/103543444403) and [journal-artifacts job103543444443](https://github.com/Nettking/msh/actions/runs/34689991000/job/103543444443).
- Canary469 intended head `660bf23269893305c7e8ffcc4c910a7efaed567d`: [journal-artifacts job103544075720](https://github.com/Nettking/msh/actions/runs/34690234261/job/103544075720).

`actions/checkout@v4` could not clean/remove `C:\\actions-runner\\_work\\msh\\msh\\.pytest_cache`: permission denied, followed by `EPERM: operation not permitted, rmdir`. Checkout fallback recreation also failed. Python setup/tests did not execute; missing JUnit and red release aggregates are downstream consequences. Do not report the intended source as actually checked out in these jobs.

[Persistent finding](https://github.com/Nettking/msh/blob/3ba6cfbb/handoff/diagnostics/D10.md) and [exact native excerpts/log hashes/source records](https://github.com/Nettking/msh/blob/3ba6cfbb/handoff/diagnostics/D10-evidence.json).

Immediate filesystem access failure is confirmed. ACL/ownership versus an open handle or other host cause is not yet established. Before retrying Beast work, inspect only this CI-owned cache, its ownership/ACL/reparse/open-handle state and current jobs through an authorized read-only channel. Any recovery must preserve concurrent/unrelated state and runner qualification/account rules. No product or runner-policy change is proposed.

Safe continuation: independent healthy CI and D09 documentation-only integration. Preserve passing source-specific evidence; no full-suite rerun to mask this failure. Investigation has read logs only. Physical runtime and protected Recorder data remain untouched; no manual deletion/prune/account/label change occurred.'''
issue=existing[0] if existing else api('/issues',{'title':title,'body':body})
root=pathlib.Path('handoff');p=root/'diagnostics/D10-evidence.json';o=json.loads(p.read_text());o['github_artifact']=issue['html_url'];p.write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')
p=root/'diagnostics/D10.md';s=p.read_text(encoding='utf-8').replace('**GitHub artifact:** Issue publication is the immediate next action.','**GitHub artifact:** [#'+str(issue['number'])+']('+issue['html_url']+').');p.write_text(s,encoding='utf-8')
p=root/'FEDERATION_V1_DIAGNOSTIC_SWEEP.md';s=p.read_text(encoding='utf-8').replace('[Evidence](diagnostics/D10.md); issue pending; no product repair','[Evidence](diagnostics/D10.md); [#'+str(issue['number'])+']('+issue['html_url']+'); no product repair');p.write_text(s,encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8')+'\nD10 durable GitHub artifact: '+issue['html_url']+'. No product repair proposed.\n';p.write_text(s,encoding='utf-8')
print(json.dumps({'issue':issue['number'],'url':issue['html_url']}))

import json,pathlib,sys,datetime
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client()
body='''First native replacement proof is complete for exact PR source `f3abe5452db2f21593a688bc62bc5f4b22d5c40e`.

- [F7 run34689990990](https://github.com/Nettking/msh/actions/runs/34689990990): Linux713 passed/9 platform skips; native Windows717 passed/5 symlink-privilege skips. All722 identities pass across the native pair. All12 previously missing Windows modules and the3 transfer modules passed without skips on both OS.
- Both actual checkouts are `b0fbb8a1a4e216b8696b8015a35594c1007319de`, with a tree identical to this PR head. Native Python/shell/storage/temp preconditions, all7 exact command invocations, strictUP035/relay lint, Compose and diff hygiene are verified. [Durable native receipt and original JUnit references](https://github.com/Nettking/msh/blob/1192f87c/handoff/diagnostics/ci-f7-pr468-native-proof.json).
- Disposable #469 has exactly one inert comment in the AI JavaScript file and an identical F7 workflow blob. Its real automatic event triggered [F7 run34690234286](https://github.com/Nettking/msh/actions/runs/34690234286); native completion is pending. Do not merge the canary.

Retirement gate: **1/2 reviewed native green runs**, with the real JS-only trigger demonstrated. All8 legacy workflows remain until the second complete proof and final reference/status-contract review. Existing automatic jobs and prior main qualification are preserved; no physical acceptance is claimed. Hosted billing admission failures remain infrastructure observations, with zero executed test steps.'''
existing=api('/issues/468/comments?per_page=100')
match=next((c for c in existing if c['body'].startswith('First native replacement proof is complete for exact PR source')),None)
if match is None:match=api('/issues/468/comments',{'body':body})
out={k:match[k] for k in ['id','html_url','created_at','body']};out['recorded_at']=datetime.datetime.now(datetime.timezone.utc).isoformat()
pathlib.Path('handoff/diagnostics/ci-f7-first-proof-pr-comment.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
p=pathlib.Path('handoff/QUALIFICATION_COORDINATION.md');s=p.read_text(encoding='utf-8').replace('then run review_f7_native_proof.py469\n(the CLI requires a space before469).','then run `review_f7_native_proof.py 469`.');s+='\nPR468 proof comment: '+out['html_url']+' . Heartbeat updated to this\ncheckpoint; next scheduled inspection11:55Z, quiet unless actionable.\n';p.write_text(s,encoding='utf-8')
print(json.dumps({'comment_url':out['html_url']}))

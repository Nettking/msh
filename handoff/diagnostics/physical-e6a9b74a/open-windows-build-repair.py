import json,pathlib,subprocess,sys
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
a=client();root=pathlib.Path('C:/wsl/fcp-v1-windows-build-exit-status-20260913');branch='codex/windows-build-exit-status-20260913'
sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip();assert a('/git/ref/heads/'+branch)['object']['sha']==sha
body='''A failed Docker build can be reported as successful on Windows. The real missing-Dockerfile case on the current candidate returned exit 0 and wrote the success marker because Start-Process exposed a null ExitCode, which was cast to zero; existing images then satisfied the image-identity check.

Retain the native process handle before polling, wait for completion, and send unavailable/nonzero exit status through the existing bounded failure cleanup. Build output, mutex ownership, cache limits, deadlines and image-identity requirements are unchanged.

Validation:
- Native regression: a real child exiting 7 falsely succeeded before the repair; exit 0 and exit 7 now take their correct paths, preserving stdout/stderr.
- All 20 controlled-build and quiescence tests pass on Windows Python 3.12; Ruff and diff hygiene pass.
- Identical real Docker missing-file fault: before, controller exit 0 with success marker; after, exit 1 with core_image_build_failed:1 and no marker. Running core containers were unchanged.

This fixes a demonstrated release blocker. The prior frozen candidate is blocked; no replacement candidate or complete physical PASS is claimed.'''
existing=a('/pulls?state=open&head=Nettking:'+branch)
p=existing[0] if existing else a('/pulls',{'title':'Preserve Windows build process exit status','head':branch,'base':'main','body':body,'draft':False})
out={'number':p['number'],'url':p['html_url'],'head':sha,'base':p['base']['sha'],'draft':p['draft']}
(pathlib.Path(__file__).parent/'windows-build-repair-pr.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out))

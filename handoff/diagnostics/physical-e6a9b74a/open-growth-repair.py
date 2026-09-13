import json,pathlib,subprocess,sys
sys.path.insert(0,'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client
a=client(); root=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913')
sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip()
branch='codex/p01-filesystem-growth-20260913'
assert a('/git/ref/heads/'+branch)['object']['sha']==sha
assert a('/git/ref/heads/main')['object']['sha']=='e6a9b74a1d555609eed6bf40c800e1258f1c9077'
body='''The P01 growth probe counts a backing volume once for every bound directory. On the frozen candidate, data and results share C:, so 41,558,016 bytes of actual growth became 83,116,032 bytes and incorrectly exceeded the unchanged 1 GiB/hour ceiling.

Samples now publish an opaque identity from the existing filesystem measurement API and share one observation for aliases of one volume. Both growth probes sum distinct volumes once. Missing identities, contradictory observations, or changes to roots, volumes or capacity remain unavailable evidence; old packets are preserved and require fresh measurement. No product runtime, thresholds, deadlines, authority rules or timed acceptance requirements change.

Validation: the initial 22 regression cases fail on the frozen implementation; the repaired harness passes all 224 CF7 tests on native Windows Python 3.12, including 23 focused growth/identity cases. Ruff and diff hygiene pass. The observed interval now computes 693,860,673 bytes/hour; distinct volumes with the same numbers still exceed the ceiling.

This is acceptance tooling, independently pinned through the existing runtime binding. Product candidate e6a9b74a1d555609eed6bf40c800e1258f1c9077 remains frozen with its existing exact-source qualification. Physical acceptance is incomplete; P07/P12 have not started.'''
existing=a('/pulls?state=open&head=Nettking:'+branch)
p=existing[0] if existing else a('/pulls',{'title':'Fix physical growth accounting for shared backing volumes','head':branch,'base':'main','body':body,'draft':True})
out={'number':p['number'],'url':p['html_url'],'head':sha,'draft':p['draft'],'candidate_unchanged':True}
(pathlib.Path(__file__).parent/'growth-repair-pr.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps(out))

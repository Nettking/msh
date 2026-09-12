"""Checkpoint the first independent-context reproduction before more work."""
import datetime
import json
import pathlib
root=pathlib.Path('handoff');dest=root/'diagnostics'
p=dest/'D13-evidence.json';e=json.loads(p.read_text(encoding='utf-8'))
e['reproduction']='Same WinError10053 on iteration2 of unchanged test in clean440123f6 source under NETTKING/Martin; stopped immediately. Separate Python3.12.10 installation from Actions; no production access.'
e['evidence'].append('D13-unchanged-local-reproduction.json')
e['next_diagnostic_action']='Compare bounded empty-body, ordinary two-byte POST, and single-send two-byte POST against unchanged handler on ephemeral loopback sockets, keeping5s deadline.'
p.write_text(json.dumps(e,indent=2)+'\n',encoding='utf-8')
for p in [dest/'D13.md',root/'QUALIFICATION_COORDINATION.md']:
    with p.open('a',encoding='utf-8') as f:
        f.write('\n## '+datetime.datetime.now(datetime.timezone.utc).isoformat()+' — D13 reproduced outside Actions\n\n'+e['reproduction']+' [Exact receipt](diagnostics/D13-unchanged-local-reproduction.json). Root-cause classification remains unresolved; preserve failure, do not retry CI. Next: '+e['next_diagnostic_action']+'\n')
p=root/'FEDERATION_V1_DIAGNOSTIC_SWEEP.md'
s=p.read_text(encoding='utf-8').replace('One native failure; focused reproduction pending','Native CI plus unchanged local test iteration2')
p.write_text(s,encoding='utf-8')

"""Select already-completed F8 evidence; never dispatch or poll qualification."""
import json
import pathlib

dest = pathlib.Path('handoff/diagnostics')
private = pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
old = json.loads((dest/'ci-migration-auto-runs-20260912T125703.json').read_text())
new = json.loads((dest/'pr468-ba44100e-auto-20260912T135538.json').read_text())
for label, data, head, checkout in [
    ('f84-existing-old', old, 'f3abe5452db2f21593a688bc62bc5f4b22d5c40e', 'b0fbb8a1a4e216b8696b8015a35594c1007319de'),
    ('f84-existing-final', new, 'ba44100ec1e4cde19daba0d3723b991c11742316', 'f18e91adae2cb198fb5cebad61ee202a38d2bd8e')
]:
    run = next(r for r in data['runs'] if r['head_sha']==head and r['path'].endswith('/phase-f8-closeout.yml'))
    assert run['conclusion']=='success' and len(run['jobs'])==2 and all(j['conclusion']=='success' for j in run['jobs'])
    workflows=[{'workflow':'phase-f8-closeout.yml','run_id':run['id'],'jobs':run['jobs']}]
    if True:
        release=next(r for r in data['runs'] if r['head_sha']==head and r['path'].endswith('/federation-v1-release.yml'))
        jobs=[j for j in release['jobs'] if j['name'] in ['Release checks (Linux)','Release checks (Windows)']]
        assert len(jobs)==2 and all(j['conclusion']=='success' for j in jobs)
        workflows.append({'workflow':'federation-v1-release.yml','run_id':release['id'],'jobs':jobs})
    receipt={'source_commit':checkout,'review_head':head,'workflows':workflows}
    (private/(label+'-qualification-latest.json')).write_text(json.dumps(receipt,indent=2)+'\n',encoding='utf-8')
    print(json.dumps({'label':label,'runs':[{'id':w['run_id'],'jobs':[{k:j.get(k) for k in ['id','name','runner_name']} for j in w['jobs']]} for w in workflows]}))

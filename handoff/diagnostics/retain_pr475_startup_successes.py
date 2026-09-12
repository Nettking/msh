"""Retain only completed startup successes; no reread of active jobs."""
import json,pathlib,subprocess,sys,zipfile
dest=pathlib.Path(__file__).resolve().parent;private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
snapshot=json.loads((dest/'pr475-initial-ci.json').read_text())
label='pr475-startup-successes';workflows=[]
for r in snapshot['runs']:
    jobs=[j for j in r['jobs'] if j['conclusion']=='success']
    if jobs:workflows.append({'workflow':r['path'].split('/')[-1],'run_id':r['id'],'jobs':jobs})
(private/(label+'-qualification-latest.json')).write_text(json.dumps({'source_commit':snapshot['merge_checkout'],'review_head':snapshot['head_sha'],'workflows':workflows},indent=2)+'\n',encoding='utf-8')
subprocess.run([sys.executable,'-B',str(private/'retain_qualification_logs.py'),label],check=True)
(dest/(label+'-native-retention.json')).write_bytes((private/(label+'-native-retention.json')).read_bytes())
with zipfile.ZipFile(dest/(label+'-logs.zip'),'w',zipfile.ZIP_DEFLATED) as archive:
    for p in sorted((private/(label+'-native-logs')).glob('*.log')):archive.write(p,p.name)

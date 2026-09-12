"""Preserve newly completed successes and all native artifacts once."""
import json,pathlib,subprocess,sys,zipfile
dest=pathlib.Path(__file__).resolve().parent;private=pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
snapshot=json.loads((dest/'pr475-ci-20260912T175749.json').read_text());label='pr475-completed-successes';workflows=[]
new_ids={i['job']['id'] for i in snapshot['new_completed_jobs']}
for r in snapshot['runs']:
    jobs=[j for j in r['jobs'] if j['conclusion']=='success' and j['id'] in new_ids]
    if jobs:workflows.append({'workflow':r['path'].split('/')[-1],'run_id':r['id'],'jobs':jobs})
(private/(label+'-qualification-latest.json')).write_text(json.dumps({'source_commit':snapshot['merge_checkout'],'review_head':snapshot['head_sha'],'workflows':workflows},indent=2)+'\n',encoding='utf-8')
subprocess.run([sys.executable,'-B',str(private/'retain_qualification_logs.py'),label],check=True)
(dest/(label+'-native-retention.json')).write_bytes((private/(label+'-native-retention.json')).read_bytes())
with zipfile.ZipFile(dest/(label+'-logs.zip'),'w',zipfile.ZIP_DEFLATED) as archive:
    for p in sorted((private/(label+'-native-logs')).glob('*.log')):archive.write(p,p.name)
subprocess.run([sys.executable,'-B',str(private/'retain_workflow_artifacts.py'),'pr475-completed',*[str(r['id']) for r in snapshot['runs']]],check=True)
(dest/'pr475-completed-raw-artifact-retention.json').write_bytes((private/'pr475-completed-raw-artifact-retention.json').read_bytes())

import sys,json,pathlib,datetime
sys.path.insert(0,r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
from github_qualification import client
api=client();p=pathlib.Path('handoff/diagnostics/pr468-initial-ci-snapshot.json');o=json.loads(p.read_text(encoding='utf-8'))
for r in o['runs']:
    if r['conclusion']=='failure':
        r['jobs']=api('/actions/runs/'+str(r['id'])+'/jobs?per_page=100')['jobs']
        for j in r['jobs']:
            j['annotations']=api(j['check_run_url'].split('/repos/Nettking/msh')[1]+'/annotations')
            print(json.dumps({'run':r['id'],'job':j['id'],'name':j['name'],'steps':len(j['steps']),'runner_id':j['runner_id'],'annotations':j['annotations']}))
p.write_text(json.dumps(o,indent=2)+'\n',encoding='utf-8')

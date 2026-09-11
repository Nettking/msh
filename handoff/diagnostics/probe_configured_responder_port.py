"""Render checked-in Compose only; never creates or changes a container."""
import datetime
import hashlib
import json
import os
import pathlib
import subprocess
import sys

source=pathlib.Path('C:/wsl/fcp-v1-0536f03d-nettking-20260911')
n='0536f03d67eb277e11573c2188d8e820399627e3'
assert subprocess.check_output(['git','rev-parse','HEAD'],cwd=source,text=True).strip()==n
assert not subprocess.check_output(['git','status','--porcelain'],cwd=source,text=True).strip()
assert not (source/'.env').exists()
sys.path.insert(0,str(source))
from catalog.federation.tailnet_join_bridge import auto_join_port
env={k:v for k,v in os.environ.items() if not k.startswith(('FCP_','COMPOSE_'))}
env.update(FCP_AUTO_JOIN_PORT='5152',FCP_BUILD_COMMIT=n)
command=['docker','compose','-f',str(source/'docker-compose.yml'),'--project-directory',str(source),'config','--format','json']
result=subprocess.run(command,cwd=source,env=env,capture_output=True,text=True,timeout=30)
assert result.returncode==0, result.stderr
config=json.loads(result.stdout)
flask_env=config['services']['flask'].get('environment',{})
report=dict(candidate_sha=n,host='nettking',physical_stage='D04 prospective isolated-port admission; supported Compose/rendering boundary, no deployment',
 mode='DIAGNOSTIC ONLY - NOT PHYSICAL ACCEPTANCE EVIDENCE',observed_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
 command=command,compose_file_sha256=hashlib.sha256((source/'docker-compose.yml').read_bytes()).hexdigest(),
 host_configured_port=auto_join_port(env),flask_environment_contains_port='FCP_AUTO_JOIN_PORT' in flask_env,
 flask_advertised_port=auto_join_port(flask_env),expected='Host responder and Flask advertised auto_join_port both equal configured 5152',
 source_boundary=['start-tailscale.sh: host responder --port FCP_AUTO_JOIN_PORT','start-tailscale.cmd: same Windows host port','docker-compose.yml: flask environment omits FCP_AUTO_JOIN_PORT','catalog/flask_app/federation_pairing_routes.py: advertisement uses auto_join_port()'],
 state_changed=False,containers_created=False,protected_recorder_data_untouched=True,physical_acceptance=False)
out=pathlib.Path(__file__).resolve().parent/'D05-configured-port.json'
out.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
print(json.dumps(report))

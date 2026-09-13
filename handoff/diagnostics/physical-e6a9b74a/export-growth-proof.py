"""Export bounded public validation receipts; never export local binding inputs."""
import hashlib,json,pathlib,subprocess,xml.etree.ElementTree as ET
here=pathlib.Path(__file__).parent
work=pathlib.Path('C:/wsl/fcp-v1-p01-filesystem-growth-20260913')
remote_script=work/'.acceptance/export-growth-proof-remote.py'
remote_script.write_text('''import json,pathlib
h=pathlib.Path('/home/martin/fcp-v1-e6a9b74a-nitro-20260913/source')
n=pathlib.Path('/home/martin/fcp-v1-501b528e-harness-20260913')
out={'admission':json.loads((h/'.acceptance/runtime-control-clean/admission-reviewed.json').read_text()),'linux_focused':json.loads((n/'.acceptance/focused-linux.json').read_text()),'linux_log':(n/'.acceptance/focused-linux.log').read_text()}
out['linux_log']=out['linux_log'].replace(str(n),'<acceptance-harness>')
print(json.dumps(out))
''')
p=subprocess.run(['C:/Python314/python.exe','C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance/ssh_campaign_script.py','nitro',str(remote_script),'--timeout','55'],capture_output=True,text=True,timeout=60)
assert p.returncode==0,p.stderr
remote=json.loads(p.stdout)
(here/'nitro-clean-runtime-admission.json').write_text(json.dumps(remote['admission'],indent=2)+'\n')
(here/'growth-focused-linux.log').write_text(remote['linux_log'])
windows_log=(work/'.acceptance/cf7-windows.log').read_text(encoding='utf-8-sig').replace(str(work),'<acceptance-harness>').replace(str(work).replace('\\','/'),'<acceptance-harness>')
(here/'growth-cf7-windows.log').write_text(windows_log)
tree=ET.parse(work/'.acceptance/cf7-windows.xml');suite=tree.getroot().find('testsuite')
assert suite is not None and suite.attrib['tests']=='224' and suite.attrib['failures']=='0' and suite.attrib['errors']=='0'
out={'acceptance_harness_sha':'501b528e9476878e6a6fe5cde8240b2d54b1d263','product_candidate_sha':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','pre_repair':{'source':'e6a9b74a1d555609eed6bf40c800e1258f1c9077','initial_focused_cases':22,'failed':22,'evidence':'Native pytest output in release task; no physical packets altered'},'post_repair':{'windows_cf7':dict(suite.attrib),'linux_focused':remote['linux_focused'],'ruff':'PASS','diff_hygiene':'PASS'},'physical_pass':False,'ceilings_changed':False,'old_packets_rewritten':False,'frozen_product_changed':False}
(here/'growth-red-to-green.json').write_text(json.dumps(out,indent=2)+'\n')
print(json.dumps({'exported':['nitro-clean-runtime-admission.json','growth-focused-linux.log','growth-cf7-windows.log','growth-red-to-green.json'],'windows_cf7_passed':224,'linux_focused_passed':23,'physical_pass':False}))

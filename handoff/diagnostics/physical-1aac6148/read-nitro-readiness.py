import json,pathlib
root=pathlib.Path('/home/martin/fcp-v1-1aac6148-main-20260913/source')
status=root/'.acceptance/native-readiness/status.json'
print(status.read_text() if status.exists() else json.dumps({'status':'NOT_STARTED'}))

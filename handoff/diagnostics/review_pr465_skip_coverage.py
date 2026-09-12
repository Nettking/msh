"""Inspect retained current-head skips and native proof; never rerun tests."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import xml.etree.ElementTree as ET
import zipfile

root = Path(__file__).resolve().parent
audit = Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
label = 'pr465-head-4749ab66-release-final'
sha = '4749ab6689315a7ad953c11ce4b2ec942433d71a'
artifacts = json.loads((root/'pr465-final-release-artifact-review.json').read_text())
assert artifacts['source_commit'] == sha
manifest = json.loads((audit/(label+'-release-artifact-review')/'shards/shard-0.json').read_text())
assert manifest['source_sha'] == sha
def identity(nodeid):
    filename, sep, qualified = nodeid.partition('::')
    assert sep and filename.endswith('.py')
    base, paramsep, params = qualified.partition('[')
    names = base.split('::')
    return '.'.join([filename[:-3].replace('/', '.'), *names[:-1]]), names[-1] + (paramsep+params if paramsep else '')
expected = Counter(identity(n) for n in manifest['collected'])
passed, skipped = set(), {}
for row in artifacts['records']:
    assert row['source_checkout_verified'] and row['failures'] == row['errors'] == 0
    with zipfile.ZipFile(audit/(label+'-raw-artifacts')/(str(row['artifact_id'])+'.zip')) as archive:
        xmls = [n for n in archive.namelist() if n.endswith('.xml')]
        assert len(xmls) == 1
        cases = list(ET.fromstring(archive.read(xmls[0])).iter('testcase'))
    if row['name'].startswith('full-suite-order-'):
        assert Counter((c.get('classname'), c.get('name')) for c in cases) == expected
    for case in cases:
        key = (case.get('classname'), case.get('name'))
        skip = case.find('skipped')
        if skip is None:
            passed.add(key)
        else:
            skipped[key] = skip.get('message')
uncovered = [dict(test='.'.join(k), reason=v) for k,v in skipped.items() if k not in passed]
modules = sorted({x['test'].rsplit('.test_',1)[0].replace('.','/')+'.py' for x in uncovered})
state = json.loads((root/'pr465-qualification-state.json').read_text())['prs']['465']
selected = {j['id'] for w in state['workflows'] for j in w['jobs']}
native = json.loads((root/'pr465-native-provenance.json').read_text())
matched = []
for row in native['records']:
    if row['job_id'] not in selected or row['checkout_matches'] is not True:
        continue
    path = audit/'pr465-head-4749ab66-delta-native-logs'/(str(row['job_id'])+'.log')
    data = path.read_bytes()
    assert hashlib.sha256(data).hexdigest() == row['sha256']
    text = re.sub(r'\x1b\[[0-9;]*m', '', data.decode('utf-8'))
    commands = [line for line in text.splitlines() if 'pytest' in line and any(m in line for m in modules)]
    if commands:
        matched.append(dict(job_id=row['job_id'],name=row['name'],sha256=row['sha256'],
            modules=[m for m in modules if any(m in line for line in commands)],commands=commands,
            summaries=[line for line in text.splitlines() if re.search(r'\d+ passed',line)]))
result = dict(source_sha=sha,collected=len(expected),unique_skips=len(skipped),
              skips_with_pass_elsewhere=len(skipped)-len(uncovered),uncovered=uncovered,matched=matched)
(root/'pr465-skip-native-map.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result))

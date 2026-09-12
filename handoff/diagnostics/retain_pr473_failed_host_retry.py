"""Retain the completed single-job retry without dispatching more work."""
import json
import pathlib
import subprocess
import sys
import zipfile

dest = pathlib.Path('handoff/diagnostics')
private = pathlib.Path(r'C:\wsl\fcp-v1-fba508-nettking-20260910\.acceptance')
source = json.loads((dest / 'pr473-D12-retry-20260912T163928.json').read_text())
label = 'pr473-corrected-host-retry-failed'
jobs = source['actual_new_executions']
assert all(j['status'] == 'completed' for j in jobs)
(private / (label + '-qualification-latest.json')).write_text(json.dumps({
    'source_commit': '0355023f27c4c6886ab6beb48bb9a998b3ab2904',
    'review_head': source['run']['head_sha'],
    'workflows': [{'workflow': 'federation-v1-release.yml', 'run_id': 34701429430, 'jobs': jobs}],
}, indent=2) + '\n', encoding='utf-8')
subprocess.run([sys.executable, '-B', str(private / 'retain_qualification_logs.py'), label], check=True)
receipt = private / (label + '-native-retention.json')
(dest / (label + '-native-retention.json')).write_bytes(receipt.read_bytes())
with zipfile.ZipFile(dest / (label + '-logs.zip'), 'w', zipfile.ZIP_DEFLATED) as archive:
    for path in sorted((private / (label + '-native-logs')).glob('*.log')):
        archive.write(path, path.name)

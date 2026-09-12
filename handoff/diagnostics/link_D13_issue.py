"""Bind the just-published durable issue to the accumulating checkpoint."""
import json
import pathlib

root = pathlib.Path('handoff')
issue = 'https://github.com/Nettking/msh/issues/474'
p = root / 'diagnostics/D13-evidence.json'
data = json.loads(p.read_text(encoding='utf-8'))
data['github_artifact'] = issue
p.write_text(json.dumps(data, indent=2) + '\n', encoding='utf-8')
p = root / 'diagnostics/D13.md'
p.write_text(p.read_text(encoding='utf-8').replace('PENDING immediate issue publication', '[#474](' + issue + ')'), encoding='utf-8')
p = root / 'FEDERATION_V1_DIAGNOSTIC_SWEEP.md'
p.write_text(p.read_text(encoding='utf-8').replace('NONE; issue publication next|', '[#474](' + issue + '); no repair|'), encoding='utf-8')
p = root / 'QUALIFICATION_COORDINATION.md'
p.write_text(p.read_text(encoding='utf-8').replace('Next publish D13 issue immediately, then inspect unchanged socket lifecycle and', 'D13 issue474 is published. Next inspect unchanged socket lifecycle and'), encoding='utf-8')

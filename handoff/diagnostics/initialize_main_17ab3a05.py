"""Archive previous main receipts and select the verified new main; no CI dispatch."""
import datetime
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parent
REPO = Path('C:/wsl/fcp-v1-17ab3a05-merged-main-20260912')
SHA = '17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05'
HEAD = '84c66f8185c1411d9dc8c5c33244a2f564845ce7'
PREVIOUS = '2a9c9b8eb53edff74c2de23570ec56e054d29b22'

def git(*args):
    return subprocess.check_output(['git', *args], cwd=REPO, text=True).strip()

def write(name, value):
    (ROOT / name).write_text(json.dumps(value, indent=2) + '\n', encoding='utf-8')

assert git('rev-parse', 'HEAD') == git('rev-parse', 'origin/main') == SHA
assert not git('status', '--porcelain')
assert git('rev-parse', SHA + '^{tree}') == git('rev-parse', HEAD + '^{tree}')
qualified = json.loads((ROOT / 'pr467-final-qualification.json').read_text())
assert qualified['source_commit'] == HEAD
assert qualified['status'] == 'QUALIFIED_REQUIRED_PR_HEAD_SCOPE'
assert qualified['required_jobs'] == 37 and qualified['additional_cfi2_registry_jobs'] == 3
old = json.loads((ROOT / 'merged-main-target.json').read_text())
assert old['source_commit'] == PREVIOUS, 'Already initialized or different campaign; inspect first'
heads = [
    '1a0c634f47f8a247b6d1d2d1a219f5c12590587d',
    '143fe7a9082193114af3d34dc437b85f845849a2',
    '5b826c6806ab1bdb960412ba20ca78192971fb1d',
    '4749ab6689315a7ad953c11ce4b2ec942433d71a',
    '2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee', HEAD,
]
for head in heads:
    subprocess.run(['git', 'merge-base', '--is-ancestor', head, SHA], cwd=REPO, check=True)
archive = ROOT / 'archive-main-2a9c9b8e'
assert not archive.exists(), 'Preserve existing archive'
archive.mkdir()
inventory = []
for path in sorted(ROOT.glob('merged-main-*.json')):
    raw = path.read_bytes()
    (archive / path.name).write_bytes(raw)
    inventory.append(dict(file=path.name, sha256=hashlib.sha256(raw).hexdigest()))
(archive / 'README.md').write_text(
    '# Archived main coordination state before selecting17ab3a05\n\n'
    'Main2a9c9b8e failed required F85-Windows dueD08; it was never fully qualified.\n'
    'The legacy merged-main-final-qualification.json still describes earlier9b286f93,\n'
    'not2a9c9b8e. Preserve each receipt under its own embedded source SHA.\n', encoding='utf-8')
stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
pending = dict(recorded_at=stamp, source_commit=SHA, status='PENDING',
    previous_main_receipts=archive.name, physical_acceptance=False, candidate_frozen=False)
for item in inventory:
    write(item['file'], pending)
write('merged-main-target.json', dict(recorded_at=stamp, source_commit=SHA, worktree=str(REPO),
    clean=True, qualified_heads_are_ancestors=heads, tree_identical_to_qualified_pr467=True,
    final_merged_main_qualified=False, candidate_frozen=False,
    runtime_sha='9b286f931497bf6291e215f6340443c5162826b0', previous_main_receipts=archive.name,
    archive_inventory=inventory,
    next_action='Select existing actual-main push runs; dispatch only absent required gates once; no PR evidence reuse'))
write('merged-main-qualification-state.json', dict(source_commit=SHA, actual_main=SHA,
    main_matches=True, recorded_at=stamp, workflows=[], missing_workflows=[]))
write('merged-main-native-provenance.json', dict(source_commit=SHA, records=[]))
write('merged-main-gap-dispatches.json', dict(source_commit=SHA, dispatches=[]))
print(json.dumps(dict(source_commit=SHA, clean=True, archive=archive.name, archived_files=len(inventory),
    status='READY_FOR_ACTUAL_MAIN_QUALIFICATION', qualified=False)))

"""Audit retained Q artifacts without rerunning tests or rebuilding publication ZIPs."""
import datetime
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import xml.etree.ElementTree as ET
import zipfile

ROOT=Path('C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
SOURCE='2c1a8d9389a75fcaf4dd224ba63c3f83f02a0cee'
REPO=Path(r'C:\wsl\fcp-responder-exit-wait-20260911')
RAW=ROOT/'pr463-head-2c1a8d93-icse-final-raw-artifacts'
OUTPUT=Path(__file__).resolve().parent
OUTPUT.mkdir(exist_ok=True)

def sha(raw): return hashlib.sha256(raw).hexdigest()

def files(archive):
    entries=archive.infolist()
    assert len(entries)<10000
    assert sum(e.file_size for e in entries)<200*1024*1024
    names=[e.filename for e in entries]
    assert len(names)==len(set(names))==len({n.casefold() for n in names})
    for item in entries:
        path=PurePosixPath(item.filename)
        assert not path.is_absolute() and '..' not in path.parts and '\\' not in item.filename
        assert not any(':' in part for part in path.parts)
        assert (item.external_attr >> 16)&0o170000 != 0o120000
    return {e.filename:archive.read(e) for e in entries if not e.is_dir()}

with zipfile.ZipFile(RAW/'10290828340.zip') as archive:
    outer=files(archive)
manifest=json.loads(outer['artifact-manifest.json'])
assert manifest['source_revision']==SOURCE
assert manifest['source_ref']=='refs/heads/codex/responder-replacement-exit-wait'
assert manifest['workflow_run']=='34669939857'
assert manifest['artifact_version']=='candidate'
checksum_entries={}
for line in outer['SHA256SUMS'].decode().splitlines():
    digest,name=line.split('  ',1)
    assert re.fullmatch('[0-9a-f]{64}',digest)
    assert sha(outer[name])==digest
    checksum_entries[name]=digest
publication=manifest['publication_archive']['file']
zip_digest,zip_name=outer['ZENODO_SHA256'].decode().strip().split('  ',1)
assert zip_name==publication and sha(outer[publication])==zip_digest
with zipfile.ZipFile(io.BytesIO(outer[publication])) as archive:
    nested=files(archive)
sp=manifest['publication_archive']['source_prefix']
ap=manifest['publication_archive']['artifact_prefix']
assert all(n.startswith(sp) or n.startswith(ap) for n in nested)
public_source={n.removeprefix(sp):b for n,b in nested.items() if n.startswith(sp)}
public_metadata={n.removeprefix(ap):b for n,b in nested.items() if n.startswith(ap)}
expected_metadata={n:b for n,b in outer.items() if n not in (publication,'ZENODO_SHA256')}
assert public_metadata==expected_metadata
assert 'example-data/2026-03-23.jsonl' not in public_source
assert 'docs/implementation/digital_twin_from_recorder_data.md' not in public_source
assert not any('.git' in PurePosixPath(n).parts or 'private-state' in PurePosixPath(n).parts for n in nested)

# Compare each exported source byte with the actual source commit's Git export.
# This is a reference export in memory, not a reconstruction of the publication ZIP.
reference=subprocess.run(['git','-c','core.autocrlf=false','-c','core.eol=lf','archive','--format=tar',SOURCE],cwd=REPO,capture_output=True,check=True).stdout
with tarfile.open(fileobj=io.BytesIO(reference),mode='r:') as archive:
    assert all(e.isdir() or e.isfile() for e in archive.getmembers())
    expected_source={e.name:archive.extractfile(e).read() for e in archive.getmembers() if e.isfile()}
assert public_source==expected_source

component=[]
assert {e['execution'] for e in manifest['evidence']}=={'Linux','Windows','compose'}
for entry in manifest['evidence']:
    raw=outer[entry['file']]
    assert sha(raw)==entry['sha256']
    result=json.loads(raw)
    assert result['implementation_commit']==SOURCE and result['passed']==result['total']==4
    assert all(s['result']=='pass' for s in result['scenarios'])
    assert [s['scenario'] for s in result['scenarios']]==manifest['scenarios']
    component.append({'execution':entry['execution'],'passed':4})

network=[]
assert {e['execution'] for e in manifest['network_evidence']}=={'Linux','Windows'}
for entry in manifest['network_evidence']:
    names={f['file'] for f in entry['files']}
    prefix='network-evidence/'+entry['execution']+'/'
    assert names=={prefix+n for n in ['summary.json','events.jsonl','operator-report.html']}
    for item in entry['files']:
        assert sha(outer[item['file']])==item['sha256']
    result=json.loads(outer[prefix+'summary.json'])
    assert result['source_sha']==SOURCE and result['result']=='PASS'
    assert set(entry['checks'])<=set(result['checks']) and len(entry['checks'])==10
    assert all(v=='PASS' for v in result['checks'].values())
    assert result['all_owned_processes_stopped'] is True
    assert not result.get('failure') and not result.get('shutdown_errors')
    events=[json.loads(line) for line in outer[prefix+'events.jsonl'].decode().splitlines() if line.strip()]
    assert events==result['events']
    assert outer[prefix+'operator-report.html'].strip()
    # Actual public runtime output must not expose credentials or PEM keys.
    for name in names:
        data=outer[name]
        assert b'-----BEGIN ' not in data and b'ghp_' not in data and b'github_pat_' not in data
        assert not re.search(rb'"(?:transport_secret|enrollment_token|invitation_token|private_key)"\s*:',data)
    network.append({'execution':entry['execution'],'required_checks_passed':10,'teardown':True})


report=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),source_sha=SOURCE,icse_run=34669939857,source_export_exact=True,source_files=len(public_source),outer_sha256=sha((RAW/'10290828340.zip').read_bytes()),publication_sha256=zip_digest,checksum_entries=len(checksum_entries),component=component,network=network,physical_acceptance=False)
(OUTPUT/'pr463-icse-artifact-review.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report))



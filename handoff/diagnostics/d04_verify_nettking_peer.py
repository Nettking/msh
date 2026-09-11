"""One real peer GET to the Nitro responder; no joining/grant request."""
import datetime, hashlib, json, pathlib, platform, subprocess, time, urllib.request
assert platform.node().casefold() == 'nettking'
tailnet = json.loads(subprocess.check_output(['tailscale', 'status', '--json']))
assert tailnet['BackendState'] == 'Running'
peer = next(x for x in tailnet['Peer'].values() if x['HostName'].casefold() == 'nitro' and x['Online'])
started = time.monotonic()
with urllib.request.urlopen('http://' + peer['TailscaleIPs'][0] + ':5151/fcp/federation/tailnet-join/health', timeout=10) as response:
    body = json.load(response)
    assert response.status == 200 and body.get('responder') == 'ready' and body.get('tailscale') is True
out = {'observed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'status': 'D04_REAL_PEER_HEALTH_VERIFIED', 'from_host': 'nettking', 'to_host': 'nitro',
    'fingerprint': hashlib.sha256(f'{platform.node()}|{platform.system()}|{platform.machine()}'.encode()).hexdigest()[:16],
    'current_nitro_runtime_sha': '0536f03d67eb277e11573c2188d8e820399627e3',
    'qualified_main': '9b286f931497bf6291e215f6340443c5162826b0',
    'port': 5151, 'http_status': 200, 'response': body, 'elapsed_seconds': time.monotonic() - started,
    'physical_acceptance': False, 'request_method': 'GET', 'enrollment_or_grant_requested': False,
    'state_changed': False, 'protected_recorder_data_untouched': True}
pathlib.Path(__file__).with_name('d04-real-peer-health.json').write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps(out))

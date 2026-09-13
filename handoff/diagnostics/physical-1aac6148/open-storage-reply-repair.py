"""Open the reviewed, exact-head minimal product repair once."""
import json
import pathlib
import sys

sys.path.insert(0, 'C:/wsl/fcp-v1-fba508-nettking-20260910/.acceptance')
from github_qualification import client

api = client()
sha = '76ad339f1631e136bba7a8a85973bddb1650d570'
branch = 'codex/federation-v1-storage-reply-repair'
assert api('/git/ref/heads/' + branch)['object']['sha'] == sha
assert api('/git/ref/heads/main')['object']['sha'] == '1aac6148759d7b2fd488ec26b97e1a786bdafa80'
body = '''Creator analysis can bind to the AI relay queue before storage inserts its own message stage. Analysis then competes with storage for provider replies. The focused reproduction writes the batch through the real local provider, diverts its authenticated successful reply into analysis's unrelated-message queue, and times out after the unchanged 15-second deadline. This matches the physical native-startup storage timeout path.

Creator analysis now binds only to the currently installed storage view owned by the same relay client. Storage installation, restoration and replacement advance the analysis generation, and runtime construction retains the generation it began binding. Non-creator and disabled-storage paths keep their existing composition. Authentication, authority, message bounds and deadlines are unchanged.

Validation: 7 new regressions cover initial ordering, stale/replaced views, generation replacement during construction, three real local-provider commits across storage restarts, unrelated downstream traffic, and direct non-creator/disabled-storage paths. All 77 focused and related tests pass on native Windows Python 3.12; Ruff and diff checks pass. The original exact-source reproduction times out in 15.031 seconds while the ordered control commits in 0.125 seconds.

The physical campaign is paused for this demonstrated defect. This PR does not claim physical acceptance: the resulting main must be qualified and frozen before fresh P01-P12, CF7 and B01-B09 evidence. P07/P12 retain their real one-hour/24-hour requirements. Protected Recorder data is untouched and AQG remains off.'''
existing = api('/pulls?state=open&head=Nettking:' + branch)
pr = existing[0] if existing else api('/pulls', {'title': 'Preserve storage replies when analysis joins the shared relay', 'head': branch, 'base': 'main', 'body': body, 'draft': False})
out = {'number': pr['number'], 'url': pr['html_url'], 'head': pr['head']['sha'], 'base': pr['base']['sha'], 'draft': pr['draft'], 'physical_pass': False}
assert out['head'] == sha
(pathlib.Path(__file__).parent / 'storage-reply-repair-pr.json').write_text(json.dumps(out, indent=2) + '\n')
print(json.dumps(out))

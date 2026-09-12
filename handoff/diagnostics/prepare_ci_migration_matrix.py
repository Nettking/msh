"""Persist proposed coverage mapping from the accepted audit; no workflow writes."""
import datetime
import fnmatch
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parent
audit=json.loads((ROOT/'phase-workflow-coverage-audit.json').read_text())
source='17ab3a05c9c506e0f92adfaa4fa0bac231ac2c05'
assert audit['source_commit']==source
workflows=audit['inventory']
f7=workflows['phase-f7-closeout.yml']
legacy=[r['workflow'] for r in audit['candidate_workflows'] if not r['workflow'].startswith('phase-f84')]
existing=f7['events']['pull_request']['paths']
added=sorted({p for n in legacy for p in workflows[n]['events']['pull_request']['paths']
    if p not in existing and not p.startswith('.github/workflows/')})
assert len(added)==8
transfer=['catalog/federation/tests/test_phase_f641_chunk_contract.py',
 'catalog/node/tests/test_phase_f642_verified_chunk_transfer.py',
 'catalog/node/tests/test_phase_f65_resumable_chunk_transfer.py']
relay_lint=['catalog/relay/tests/test_phase_f74_capability_dispatch.py',
 'catalog/relay/tests/test_phase_f75_retry_cancellation.py']
strict='I001,RUF022,B008,C408,PLC0206'
rows=[]
for old in audit['candidate_workflows']:
    name=old['workflow']
    rows.append(dict(legacy=name,old_workflow_blob=workflows[name]['blob'],
        old_triggers=workflows[name]['events'],old_jobs=workflows[name]['jobs'],
        os=['Linux','Windows'],
        replacement=('unchanged F8 closeout + release' if name.startswith('phase-f84') else
                     'extended F7 closeout + unchanged release/F6/F8'),
        trigger_mapping=('All remaining product/docs paths already trigger F8/release; independently prove before partial retirement.'
            if name.startswith('phase-f84') else 'F7 preserves existing paths and adds union of legacy product paths for both pull_request and main push.'),
        lint_mapping=('Original F8.4 ignore set/scopes covered by unchanged release.' if name.startswith('phase-f84') else
            'Existing F7/release lint retained; separate capabilities UP035 guard preserves stricter F7.1-F7.6 rules; strict two-file relay lint preserves F7.4/F7.5.'),
        test_mapping=('F8 covers old full explicit set on both OS; release remains full Linux and selected Windows.'
            if name.startswith('phase-f84') else 'F7 existing modules plus three transfer modules; release covers AI candidate-visibility module. No module removed.'),
        runtime_assumptions=dict(python='existing pinned Python action: Linux3.12.13/Windows3.12.10',
            dependencies='existing requirements.txt + constraints-phase2.txt; no product dependency changes',
            storage='existing Linux storage precondition and native Windows long-path temp policy',
            services='existing owned test fixtures and Compose config validation; no Recorder/runtime data',
            runners=['self-hosted/fcp-linux-fast','self-hosted/fcp-windows']),
        legacy_self_path='After retirement the deleted file is not an input; retirement diff itself requires release and targeted replacement validation. No dangling legacy trigger reference retained.',
        retirement_status='NOT YET AUTHORIZED BY EVIDENCE: needs exact replacement green runs and final reference checks'))
probes=['catalog/capabilities/analysis/content_store.py','catalog/relay/tests/test_phase_f74_capability_dispatch.py',
    'catalog/relay/tests/test_phase_f75_retry_cancellation.py','catalog/federation/object_transfer.py',
    'catalog/federation/resumable_chunk_transfer.py',*transfer,
    'catalog/flask_app/static/js/ai-explainer.js','catalog/flask_app/services/server_setup_service.py',
    'catalog/flask_app/tests/test_ai_explainer_candidate_visibility.py']
for probe in probes:
    assert any(fnmatch.fnmatchcase(probe,p) for p in existing+added)
result=dict(recorded_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),qualified_baseline=source,
    status='PROPOSED_NOT_IMPLEMENTED',changed_retained_workflow='phase-f7-closeout.yml',
    new_workflows=0,new_job_definitions=0,existing_matrix_executions_per_run=2,
    add_paths_to_pull_request_and_main_push=added,add_test_modules=transfer,
    add_lint_commands=['python -m ruff check catalog/capabilities --select UP035',
        'python -m ruff check '+' '.join(relay_lint)+' --ignore '+strict],
    add_workflow_dispatch='For controlled exact-source replacement qualification; never treat dispatch as automatic-path trigger proof.',
    unchanged=['federation-v1-release.yml','phase-f6-closeout.yml','phase-f8-closeout.yml',
        '.github/actions/self-hosted-python/action.yml','product/runtime source','runner labels/accounts/pool admission'],
    rows=rows,proposed_trigger_probes=[dict(path=p,f7_would_match=True) for p in probes],
    proof_boundaries=['Static proposed mapping only; validate actual edited YAML later.',
        'Real JS-only pull-request event required on a disposable non-release canary branch; no canary product change is merged.',
        'Two full green native replacement matrix executions with source/workflow/command provenance before retirement.',
        'Record every exact SHA and never relabel17ab or467 proof as migration-source qualification.'],
    physical_acceptance=False,state_changed=False)
(ROOT/'ci-migration-equivalence-matrix.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:result[k] for k in ['status','new_workflows','new_job_definitions',
 'existing_matrix_executions_per_run','add_paths_to_pull_request_and_main_push','add_test_modules']}))

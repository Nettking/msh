"""Read-only source comparison before the canary gate; not final-head approval."""
import datetime, hashlib, json, pathlib, subprocess
ROOT=pathlib.Path(__file__).resolve().parent
REPO=r'C:\wsl\fcp-ci-f7-consolidation-20260912'
BASE='f3abe5452db2f21593a688bc62bc5f4b22d5c40e'
CANARY='660bf23269893305c7e8ffcc4c910a7efaed567d'
REPAIR='ba44100ec1e4cde19daba0d3723b991c11742316'
DOC='docs/implementation/f7_ci_consolidation.md'
JS='catalog/flask_app/static/js/ai-explainer.js'
def git(*args):return subprocess.check_output(['git','-C',REPO,*args])
def tree(ref):
    result={}
    for entry in git('ls-tree','-r','-z',ref).split(b'\0'):
        if not entry:continue
        meta,path=entry.split(b'\t',1);result[path.decode()]=meta.decode()
    return result
b,c,r=[tree(ref) for ref in [BASE,CANARY,REPAIR]]
assert b.keys()==r.keys()==c.keys()
delta=[p for p in b if b[p]!=r[p]];assert delta==[DOC]
assert git('rev-parse',REPAIR+'^').decode().strip()==BASE
assert [p for p in b if b[p]!=c[p]]==[JS]
assert git('show',CANARY+':'+JS)==b'// Disposable CI path-trigger canary; never merge this comment.\n'+git('show',BASE+':'+JS)
assert git('show',REPAIR+':'+JS)==git('show',BASE+':'+JS)
policy_paths=['docs/implementation/federation_v1_cleanup_manifest.md','docs/implementation/phase_f7_closeout.md','docs/implementation/f7_ci_consolidation.md','.github/workflows/phase-f7-closeout.yml']
out={'recorded_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'status':'PRE_INTEGRATION_READ_ONLY_PROOF_NOT_FINAL_HEAD_VALIDATION','original_pr468_source':BASE,'actual_canary_head':CANARY,'proposed_documentation_repair':REPAIR,'repair_parent_is_original_head':True,'delta_original_pr_to_repair':delta,'all_other_tracked_entries_identical':len(b)-1,'identical_entry_metadata':'Git mode, object type, blob/object id, and path; includes workflows, triggers, test/product sources, runner configuration and dependencies','workflow_blob':b['.github/workflows/phase-f7-closeout.yml'].split()[-1],'canary_delta_from_original':[JS],'canary_change':'one known inert comment; excluded from integration, never merged','delta_actual_canary_to_repair':[p for p in c if c[p]!=r[p]],'source_attribution_note':'The documentation-only statement compares the proven original PR468 source f3abe545 to ba44100e. Actual canary660bf232 additionally contains its disposable comment; direct canary-to-repair diff removes that comment. Do not falsely describe those two complete trees as differing only in documentation.','policy_review':{'source':BASE,'paths_and_blobs':{p:b[p].split()[-1] for p in policy_paths},'cleanup_manifest_requirement':'Run the equivalent replacement at least twice green, then remove superseded workflows in a separate change.','finding':'No requirement in the reviewed cleanup/F7 contracts to repeat both complete native runs solely for a documentation-only delta with byte-identical executable/workflow source. Retain exact original provenance and separately validate final PR head.','physical_revalidation':'Physical impact-map tooling does not authorize CI qualification or physical PASS here; no physical evidence is carried forward.'},'required_next_gate':'Wait for canary Windows; retain/review 2/2 proof or diagnose failure on unchanged head. Only then integrate docs repair, verify actual PR head and full tree delta, rerun branding and required lightweight final checks.','current_pr_head_not_changed':True,'legacy_retired':[],'protected_recorder_data':'UNTOUCHED'}
(ROOT/'D09-preintegration-source-comparison.json').write_text(json.dumps(out,indent=2)+'\n',encoding='utf-8')
print(json.dumps({'status':out['status'],'original_to_repair_delta':delta,'unchanged_tracked_entries':len(b)-1,'actual_canary_to_repair_delta':out['delta_actual_canary_to_repair'],'canary_inert_comment_verified':True}))

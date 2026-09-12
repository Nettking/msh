"""Persist bounded framing evidence without promoting a hypothesis to root cause."""
import datetime
import json
import pathlib

root=pathlib.Path('handoff');dest=root/'diagnostics'
now=datetime.datetime.now(datetime.timezone.utc).isoformat()
p=dest/'D13-evidence.json';e=json.loads(p.read_text(encoding='utf-8'))
e['evidence'].append('D13-request-framing-contrast.json')
e['framing_contrast']='Unchanged handler, fresh loopback sockets,5s client deadline, explicit teardown: empty-body urllib20/20PASS; two-byte-body urllib19PASS/1WinError10053; single-send headers+two-byte body20/20PASS. Same refused path; no grant invocation.'
e['classification']='UNRESOLVED cause; confirmed reproducible Windows refusal-response failure. Request framing narrows product/socket lifecycle hypothesis; no evidence of Git ownership or capacity/queue cause.'
e['hypotheses']=['Unknown-path do_POST returns404 before consuming Content-Length bytes. Python http.client sends headers and body separately; socketserver shuts down write then closes. Native framing contrast supports an unread-body/close race, but exact socket-level causation is not yet demonstrated. Do not infer a safe fix from merely draining an unbounded body.']
e['next_diagnostic_action']='Run one bounded controlled split-request transport diagnosis against unchanged440123f6 handler to establish whether unread request data at close causes the response abort. Preserve5s client deadline, refusal/authority guards and ephemeral isolation. Develop any demonstrated repair in a separate PR/worktree; never amend473 product source or retry full CI to mask D13.'
p.write_text(json.dumps(e,indent=2)+'\n',encoding='utf-8')
p=dest/'D13.md';s=p.read_text(encoding='utf-8').replace('(diagnostics/D13-unchanged-local-reproduction.json)','(D13-unchanged-local-reproduction.json)')
s+='\n## '+now+' — bounded request-framing contrast\n\n'+e['framing_contrast']+' [Receipt](D13-request-framing-contrast.json). '+e['classification']+'\n\nNext: '+e['next_diagnostic_action']+'\n'
p.write_text(s,encoding='utf-8')
p=dest/'D12.md';s=p.read_text(encoding='utf-8').replace('**Status:** HOST CONFIGURATION REPAIRED AND VERIFIED on Beast; affected PR473 CI check pending','**Status:** HOST CONFIGURATION REPAIRED AND VERIFIED on Beast; retry on Nettking reached a separate D13 failure')
s+='\n## '+now+' — corrected-host retry has distinct D13 outcome\n\nPR473 attempt2 job103583206021 ran on Nettking. The former three launcher readable-commit failures did not recur; the run instead failed unknown-path response delivery (D13/#474,149passed/7skipped/1failed). Beast trust repair remains verified on Beast; a different-host launcher pass is not original-host CI proof. No further blind retry and no claim D12 caused D13. See [retained native evidence](pr473-corrected-host-retry-failed-native-retention.json).\n'
p.write_text(s,encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8')
start=s.index('**Current actionable checkpoint');end=s.index('Current user direction, September 11 2026:')
s=s[:start]+f'''**Current actionable checkpoint ({now}):** PR473 stays fixed at
440123f6bc6dc358eef3d233236bc14f91af60e0. Its native F7 final-source proof,
original2/2 replacement proof and real JS-only event remain valid and retained.

The single affected-check retry34701429430/attempt2/job103583206021 completed
on Nettking with a DIFFERENT failure, D13/#474: unknown-path refusal logged404
but client received WinError10053.149passed/7skipped/1failed. Three red aggregates
are dependent consequences. Actual0355023f checkout/tree==440 verified in native
logs. No live jobs remain in this run; no further CI retry. PR473 remains draft.

D13 reproduced on iteration2 of the unchanged test in clean440 under NETTKING/
Martin, separate from Actions. Bounded framing contrast: empty-body20/20PASS,
ordinary two-byte POST19PASS/1same failure, single-send headers+body20/20PASS.
Unknown path returns before reading request body; standard client sends headers
and body separately. Unread-body/close race is a supported hypothesis, not a
proven safe repair. Cause remains unresolved; do not classify as capacity/Git.
Receipts: diagnostics/D13.md, D13-unchanged-local-reproduction.json,
D13-request-framing-contrast.json; full original logs archived and issue474 linked.
NEXT: one bounded controlled split-request transport diagnosis against unchanged
440 handler. Preserve5s deadline and authority guards; ephemeral loopback only.
Any repair belongs in a separate PR/worktree, not in CI cleanup473. Do not merge
or dispatch another full qualification merely because runners are idle.

D12 Beast scoped Git trust repair remains verified on original runner. Its former
launcher failures did not recur on Nettking; this is not original-host CI proof.
D11/#471 remains unresolved on AQG; D10 remains separate. Main b719 release16/16
native/artifact proof is archived, companion failures preserved. Last fully
qualified candidate17ab3a05(37+3) remains. No new candidate or physical acceptance.
Physical runtime9b286f93 and protected Recorder data untouched. No P07/P12,
deployment, Docker reset/prune or new runner/account/pool changes. Coordination-
only Beast maintenance workflow must never enter a release candidate.

'''+s[end:]
p.write_text(s,encoding='utf-8')
p=root/'FEDERATION_V1_DIAGNOSTIC_SWEEP.md';s=p.read_text(encoding='utf-8').replace('Native CI plus unchanged local test iteration2','Native CI, unchanged local iteration2, body-framing contrast')
p.write_text(s,encoding='utf-8')
print(json.dumps({'next':e['next_diagnostic_action'],'recorded_at':now}))

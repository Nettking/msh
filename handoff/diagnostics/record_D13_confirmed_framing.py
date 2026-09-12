"""Promote D13 only after controlled valid-request reproduction is preserved."""
import datetime
import json
import pathlib
root=pathlib.Path('handoff');dest=root/'diagnostics';now=datetime.datetime.now(datetime.timezone.utc).isoformat()
p=dest/'D13-evidence.json';e=json.loads(p.read_text(encoding='utf-8'))
e['evidence'].append('D13-controlled-split-request.json')
e['classification']='PRODUCT DEFECT: valid segmented POST refusal response is unreliable on native Windows; unchanged handler closes without consuming the declared request body.'
e['controlled_reproduction']='Identical HTTP/1.1 POST with Content-Length2 and body{}: coalesced5/5PASS; split without imposed delay5/5PASS; split with10ms body delay0PASS/5WinError10053. Server404 logged on every failed attempt. No authority invocation or source change.'
e['mechanism']='Unknown-path do_POST writes404 and returns before reading declared request body; default socketserver teardown closes the connection. A short, valid header/body split reliably causes Windows response read abort. The defect is client-visible response delivery, not authority bypass.'
e['hypotheses']=['Unread data arriving during close is the likely OS-level reset mechanism; packet-level close/reset sequence has not been captured. This remaining low-level detail does not invalidate the controlled product behavior defect.']
e['repair']='NONE; separate product repair required, outside PR473'
e['next_diagnostic_action']='Prepare an isolated D13 repair branch from the intended base after reviewing bounded rejection-response teardown. Preserve immediate refusal, request-size bounds, deadlines and no-authority behavior. Add regression with deterministic segmented POST plus incomplete/oversized-body safety coverage; do not alter PR473 head, deploy repair or launch full candidate qualification.'
p.write_text(json.dumps(e,indent=2)+'\n',encoding='utf-8')
p=dest/'D13.md';s=p.read_text(encoding='utf-8')
s=s.replace('**Classification:** UNRESOLVED: product HTTP lifecycle, test lifecycle, or host/transient transport cause requires diagnosis.','**Classification:** '+e['classification'])
s+='\n## '+now+' — controlled segmentation confirms product failure\n\n'+e['controlled_reproduction']+' [Receipt](D13-controlled-split-request.json).\n\n'+e['mechanism']+' No relaxed assertion/deadline or modified product source.\n\nNext: '+e['next_diagnostic_action']+'\n'
p.write_text(s,encoding='utf-8')
p=root/'FEDERATION_V1_DIAGNOSTIC_SWEEP.md';s=p.read_text(encoding='utf-8').replace('|Unresolved transport/lifecycle|Native CI, unchanged local iteration2, body-framing contrast|','|Product: segmented refusal response|Native CI/local plus controlled10ms split5/5fail|')
p.write_text(s,encoding='utf-8')
p=root/'QUALIFICATION_COORDINATION.md';s=p.read_text(encoding='utf-8')
start=s.index('D13 reproduced on iteration2');end=s.index('D12 Beast scoped Git trust repair remains')
s=s[:start]+'''D13/#474 is now a CONFIRMED PRODUCT DEFECT: same valid HTTP request passes
coalesced5/5, passes split without delay5/5, fails split with10ms body delay5/5.
All failed cases log404 but client raises WinError10053. Handler returns before
reading declared body; the default server closes the connection. Authority stays
fail-closed. Packet-level reset details are not captured, but client-visible
segmentation failure is demonstrated on unchanged440 native Windows.
Original test also failed on iteration2 outside Actions. Full receipts:
diagnostics/D13.md and D13-controlled-split-request.json; issue474 linked.
NEXT: isolated separate D13 product repair branch, reviewing bounded rejection
teardown before edits. Preserve immediate refusal, size/deadline bounds and
authority guards; regress segmented, incomplete and oversized requests. No PR473
head/product changes, deployment or full candidate qualification merely because
runners are idle. PR473 merge remains paused; all prior valid proof retained.

'''+s[end:]
s=s.replace('**Current actionable checkpoint (', '**Current actionable checkpoint (',1)
p.write_text(s,encoding='utf-8')
print(json.dumps({'classification':e['classification'],'next':e['next_diagnostic_action']}))

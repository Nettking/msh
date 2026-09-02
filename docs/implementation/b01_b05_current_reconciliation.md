# B01 / B05 current-main reconciliation

Base: `87f670fa7aa6a23da3ae63990886c5e4ff522265`

Status: **reconciliation in progress; do not use this note as release evidence yet**.

This lane intentionally re-audits current merged code instead of copying the older blocker score text forward.

## B05 current evidence to reconcile

Merged deliveries already cover the named software properties:

- optional model absence/failure isolation: #341;
- host-resource admission for supported model-install paths: #342;
- supported start/update mutation/resource identity boundary: #343/#345/#347/#350/#352 as applicable;
- failed target activation recovery and deterministic same-approved-apply retry: #382.

The remaining task is to verify those properties still compose on current `main` and that no later merge regressed them. Physical model/resource and update/failure injection remain part of P02/P03/P05/P10 rather than a reason to duplicate software implementation.

## B01 current evidence to reconcile

Merged host-resource work includes the shared pressure/admission foundation, controlled Docker build pressure handling, model pull admission, recorder transaction admission/storage-exhaustion attribution, and the broad writer-boundary reconciliation in #383.

PR #383's own scorecard distinguished bounded active writes from lifetime aggregate retention. This reconciliation treats lifetime history/retention as B07/B09 work when the write boundary is already admitted, rather than calling the same issue a second B01 missing-admission defect.

The remaining task is to verify current `main` has no supported writer boundary with **no effective admission at its actual write boundary**, and to separate physical aggregate-resource evidence (P01/P02/P04/P08/P10) from software gaps.

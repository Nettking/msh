# B01 post-merge shared host-operation admission

Status: **draft follow-up to merged PR #383**

Baseline: `main` at `49027ca4a58eeadc6426a5d3908b32012c2b3af5`.

A final adversarial continuation after PR #383 merged found one process-wide coordination bypass in the supported host-owned Docker operations.

`catalog/federation/host_build.py` and `catalog/federation/model_resource_pull.py` previously constructed private `ProcessResourceAdmission` instances when their optional controller argument was absent. Their free-space assessments therefore did not include reservations held by other in-process writers using `PROCESS_RESOURCE_ADMISSION`.

This follow-up changes the production defaults for:

- Docker backing-resource assessment;
- build preflight;
- controlled core builds; and
- supported model pulls

to use the serialized process-wide `PROCESS_RESOURCE_ADMISSION` controller. Explicit controller injection remains available for deterministic tests.

Builds and model pulls remain unknown-size operations: this change does not invent a byte-size reservation for them. They continue to start only above `PRESSURE`, remeasure while active, and stop/refuse at the existing pressure boundary. The change makes those measurements account for reservations held by bounded writers in the same process.

No retention policy, physical acceptance state, Federation machine, B03 publication behavior, or unrelated repository content is changed by this follow-up.

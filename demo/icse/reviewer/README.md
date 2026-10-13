# FCP ICSE: prebuilt, ready-to-run reviewer package

This is a no-build option for reviewers. The CI-created package includes a prebuilt **Linux/amd64 Docker image**, the **same commit-bound source/evidence ZIP** used by the paper artifact, cryptographic checksums, and launchers. Reviewers install **Docker Engine** (Linux) or **Docker Desktop with Linux containers** (Windows); there is no Python installation, source build, account, or external service.

## Run

1. Extract the complete **fcp-icse-ready-run-...** GitHub Actions artifact to a new directory.
2. On Windows open PowerShell in that directory and run **.\run.ps1**. On Linux/x86-64 use **sh run.sh**. Do not run docker build or pip install.
3. Read **results/network/operator-report.html** and **results/PROVENANCE.json**. Successful execution also creates **results/components.json**, **results/network/summary.json** and **results/network/events.jsonl**.

Launchers check published file checksums, load the prebuilt image, and run it without external networking. The runtime checks the SHA-256 of the canonical publication ZIP, extracts it with explicit regular-file/path limits, compares the immutable source revision against the image, and runs **actual** FCP E1–E4 component code and independently running network processes from that archive. The network test verifies the extracted source byte-by-byte through its own provenance checks. Only the sanitized public report leaves the container; synthetic inputs, independent local processes, and ephemeral private node state are not physical-hardware validation.

A green run means **E1–E4 4/4** and **local network 10/10**, for that exact source SHA. It is not a claim about production Recorder capture, distributed compute/storage workloads, browser UI, physical host failure, industrial performance or developer-productivity improvement.

## Release status and provenance

The CI package is a candidate until produced by the **immutable ICSE release tag** corresponding to the physically accepted product source. Package checksums detect accidental modifications; authenticate the package using the trusted GitHub Release/Zenodo DOI rather than trusting a sidecar downloaded from an untrusted host. The canonical research artifact remains the validated source-and-evidence ZIP. A prebuilt runtime is a supplementary distribution.

The GitHub Actions job runs an offline smoke test using the package source before publishing the ready-run artifact. It exits nonzero on failed assertions, mismatched revisions, archive corruption, or failed process teardown. Its Linux smoke test does not by itself validate Docker Desktop on Windows. Windows PowerShell launcher also needs a Windows/Docker Desktop test before claiming it is independently validated.

For another run, rename or remove the **results** directory first. A failed or partial run must never be presented as PASS.

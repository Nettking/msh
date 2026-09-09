# Capture distribution

`2026-03-23.jsonl` is retained in the repository for existing local use. Its
provenance and permission for public redistribution have not been documented,
so the tracked `.gitattributes` excludes that capture from `git archive` source
downloads and the ICSE publication ZIP. The separate implementation note
`docs/implementation/digital_twin_from_recorder_data.md` contains identifying
details from another production capture and is excluded as well. Both originals
remain available in repository clones.

The reproducible ICSE component and network demonstrations generate their own
synthetic inputs and do not require this capture. Archive users can run those
demonstrations without access to private recordings or physical hosts. The
legacy Termux sample-data seeding path has no bundled telemetry in these
archives; use separately authorized inputs for that optional path.

Including a recorded capture in a future public artifact requires documented
redistribution permission and a review of identifying machine, program, tool,
part and production-activity fields. Excluding a file from an archive does not
remove it from Git history or establish that a repository clone is public-safe.

# Network demonstration video storyboard

Use `DEMONSTRATION.md` for the primary five-minute story and `network/README.md`
for runnable setup and boundaries. These are preparation materials. Release
identity, recordings and screenshots remain **PENDING** until produced from the
actual released source. Verify the selected ICSE venue/year's submission rules;
the times below are editorial targets, not claimed conference limits.

| Shot | Target | Content |
| --- | --- | --- |
| Source and topology | 30 seconds | Release SHA/tag or verified archive, conceptual figure, four real process IDs |
| Bootstrap and join | 45 seconds | One quorum-backed Federation and authenticated joining member |
| Discovery and ownership | 45 seconds | Illustrative capability owners and rejection of an invalid owner assertion |
| Useful interaction | 40 seconds | Public synthetic readings delivered over the production generic relay API |
| Leader loss and recovery | 60 seconds | Actual leader termination, observed new authority, explicit reconnect and repeated delivery |
| Return and quorum boundary | 40 seconds | Returning follower, minority enrollment refusal, process teardown |
| Reproduction and limits | 40 seconds | Current offline report, network/E1–E4 evidence, archive/checksums and precise scope |

Capture actual terminal output and the generated offline operator report. It is
an observation report, not the product's Flask UI. Do not fabricate product
screenshots from scenario JSON. Capability declarations are illustrative; the
video must not depict them as activated compute/storage/Recorder providers.
Retain E1–E4 as supporting authority-boundary evidence rather than a second
competing demonstration story.

## Figure and screenshot targets

- Use `figures/federation-v1-overview.svg` and `ARCHITECTURE.md` as explanatory
  system figures. Render to paper-compatible PDF and inspect readability.
- Capture the actual initial report showing process identities and authenticated
  membership/discovery, with pairing secrets excluded.
- Capture matched payload observations before and after observed quorum
  recovery, and the final ten-check summary with its exact source identity.
- Capture the separate current E1–E4 result only as component evidence.
- Present physical acceptance in a separate evidence table after its actual
  completion; local processes cannot stand in for physical hosts or durations.

For every capture retain UTC time, release SHA/tag, archive digest, execution
kind, scenario/action, raw evidence reference, media SHA-256, redactions and
caption claim. Keep unaltered evidence separate from edited video. Disclose
compressed waits; video length never substitutes for duration-test evidence.

Publish only `public/` network output. `private-state/` contains node identities,
transport secrets, databases and logs and must stay outside the publication
bundle. Exclude passwords, pairing material, private endpoints and unrelated
desktop content from captures. Add a release URL or DOI only after it exists.

Before delivery, review the entire video for legibility, narration/caption
agreement, real source identity and the network guide's limitations. A passing
video demonstration is not a general availability, storage-safety, performance
or security result.

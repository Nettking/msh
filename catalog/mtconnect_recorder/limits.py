"""Finite transaction limits for the production MTConnect recorder."""
from __future__ import annotations

MEBIBYTE = 1024 * 1024

# Eight source workers may be active at once. These per-response ceilings make
# their aggregate body footprint finite so the host resource envelope can
# reserve for a real maximum rather than an Agent-controlled value.
MAX_CURRENT_RESPONSE_BYTES = 8 * MEBIBYTE
MAX_PROBE_RESPONSE_BYTES = 16 * MEBIBYTE
MAX_SAMPLE_RESPONSE_BYTES = 16 * MEBIBYTE

# A normal request uses ten seconds. Sixty seconds remains available for slow
# private links without allowing a configured source to occupy a worker for an
# arbitrary period.
MAX_REQUEST_DEADLINE_SECONDS = 60.0

# The default requested count remains 1,000. The hard ceiling protects parsing
# and recovery even when an Agent ignores that requested count.
MAX_OBSERVATIONS_PER_BATCH = 10_000
MAX_SEQUENCE_SPAN = 10_000

# Byte-bounded XML still needs structural ceilings: a small-tag document can
# otherwise turn one accepted response into an excessive ElementTree object
# graph before observation-count checks run.
MAX_XML_ELEMENTS = 50_000
MAX_XML_DEPTH = 64
MAX_XML_ATTRIBUTES_PER_ELEMENT = 64
MAX_XML_TEXT_BYTES = 16 * MEBIBYTE

# Bounded ingress is not enough by itself: the compatibility renderer carries
# forward machine state into every row, so one bounded XML document could still
# amplify into effectively quadratic memory/disk work as new signal names appear.
# These ceilings bound the derived transaction while B01 separately supplies
# host-wide resource admission across concurrently active writers.
MAX_OBSERVATION_ARCHIVE_BYTES = 32 * MEBIBYTE
MAX_COMPATIBILITY_BATCH_BYTES = 64 * MEBIBYTE
MAX_COMPATIBILITY_STATE_BYTES = 16 * MEBIBYTE

"""Product policy for durable capability evidence lifetimes."""

# Inspection and standard benchmark evidence is explicitly refreshed by the
# operator. Keep a finite ten-year safety horizon so normal restarts and idle
# time do not turn evidence collection into a recurring startup task.
DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS = 315_360_000

__all__ = ["DURABLE_CAPABILITY_EVIDENCE_TTL_SECONDS"]

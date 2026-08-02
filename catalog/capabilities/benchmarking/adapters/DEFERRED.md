# CF2-B deferred production bindings

CF2-B adds adapter-private, explicitly registered trusted local probes. It does
not add a second provider runtime, authority model, discovery mechanism, or
protocol message.

The following direct bindings are intentionally deferred:

1. **Compute synthetic execution.** The stable local inventory exposes trusted
   descriptors and handler objects, but execution is coupled to the existing
   dispatch/ownership authority. Calling handlers directly would bypass that
   authority; dispatching a benchmark would violate CF2-B. The concrete compute
   adapter therefore emits descriptor, availability, revision, and fingerprint
   evidence only.
2. **Existing `StorageProvider` round trip.** The stable provider contract has
   `write` and `retrieve`, but no bounded cleanup operation. A benchmark bound
   directly to it could leave durable probe records. The concrete storage
   adapter requires an existing authority-free probe seam with explicit
   write/read/cleanup callbacks before it can be registered.
3. **Direct/relay transport measurement.** Existing authenticated transports do
   not expose a non-mutating authenticated ping/sample operation. Adding one
   would change protocol schemas. The network adapter therefore accepts only an
   already-authenticated local sample callback and never receives or publishes
   peer addresses, relay URLs, tokens, or route descriptors.

The runner uses cooperative cancellation and bounded waits in daemon threads.
These adapters pass the remaining deadline to trusted callbacks and check
cancellation between operations. They do not claim hard process isolation and
must not be registered with callbacks that ignore their timeout/cancellation
contract.

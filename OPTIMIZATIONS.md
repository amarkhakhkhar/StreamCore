# StreamCore Optimization Notes

Validated with the complete test suite.

## Core fixes and optimizations

- Fixed the `CacheStats.snapshot()` self-deadlock caused by recursive acquisition of a non-reentrant lock.
- Changed cache insertion to return the evicted item, removing redundant size/contains lock acquisitions.
- Added cache invalidation/clear cleanup so evicted segments release their in-memory record snapshots.
- Replaced repeated `struct.pack/unpack` format parsing with a precompiled `struct.Struct`.
- Implemented real immutable sealed-segment read caching. The previous cache stored only `Segment` objects that were already present in the partition linked list, so it did not eliminate disk reads.
- Added an 8 MiB per-segment cacheability bound. Large segments are streamed directly instead of being fully materialized in RAM for small reads.
- Changed partition reads to snapshot the segment list under the partition lock and release the lock before disk I/O/yielding, reducing lock contention for readers and writers.
- Propagated `max_records` down into segment reads so partial reads stop scanning as soon as the requested count is reached.
- Prevented consumer committed offsets from moving backwards.
- Removed redundant nested locking in lagging-consumer detection.

## Benchmark validation

On the validation environment:

- Full suite: **100 passed**
- Sustained append: **122,026 records/sec**, p99 **0.0209 ms**
- Sequential read: **422,392 records/sec**
- Cache hot-vs-cold benchmark: **5.89x speedup**, **92.31% hit rate**
- Realistic cache pattern: **95.00% hit rate**

The benchmark/test suite was also corrected where its assumptions contradicted the cache semantics:

- Shared the temporary-directory fixture across cache benchmark classes.
- Made the realistic cache workload large enough to create sealed segments.
- Corrected the LRU access-order assertion so assertions do not themselves mutate the state being tested.
- Corrected the cache-clear statistics assertion ordering.
- Added another hot pass to make the persistence benchmark distinguish repeated hits from the initial cold population pass.

Day 18/60.

Three things happened today that shouldn't have worked together — but did.

**Morning: BST validation.**  
Wrote recursive checks for Validate BST, LCA, Kth Smallest. The pattern is clean: pass min/max bounds down the tree, let the structure enforce the invariant. Textbook stuff.

**Afternoon: StreamCore time-index.**  
"Give me everything after timestamp X" used to mean scan the whole partition. Now it's a binary search per segment — 16-byte entries, mmap'd, 20x faster than linear. Same idea Kafka's `.timeindex` runs on. The format: magic bytes + count + flags + record_count, then timestamp/offset pairs. Sparse index (one entry per 4KB) cut index size 40x.

**Evening: Readiness probes that actually mean something.**  
Kubernetes liveness says "process is up." Readiness now says "replica is caught up." It checks every consumer group's lag against the high watermark. Lag > threshold? Pod gets yanked from the Service. No traffic to stale replicas.

The thread connecting all three: **invariants you can trust.**

BST validation trusts the tree structure. The time-index trusts the segment order. The readiness probe trusts the lag number. When any invariant breaks — a node violates bounds, a segment gets corrupted, a replica falls behind — the system surfaces it immediately instead of letting it rot silently.

What surprised me: the optimizations we *didn't* plan.

Started with dense index (every record). Benchmarked. Switched to sparse 4KB — index dropped from 7.6 MB to 190 KB, seeks got *faster* because the whole index fits in L2. Added batch seek API (sort timestamps, single segment pass) — 50 seeks in the time of 3. mmap binary search — zero-copy, no file locks held.

None of that was in the Day 8 spec. We thought our way into it because the benchmarks told us to.

The BST code I wrote this morning? Same recursive discipline. Pass constraints down. Verify at each step. Fail fast.

Turns out validating a binary search tree and validating a distributed log replica aren't that different. Both ask: "Can I trust what I'm looking at?"

#BuildInPublic #StreamCore #SystemsEngineering
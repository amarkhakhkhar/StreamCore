Day 17/60. Trees showed up twice today — once finding a partition's leader, once explaining why a customer might churn.

🧠 DSA: Trees & Traversals
Solved Invert Binary Tree, Maximum Depth of Binary Tree, Subtree of Another Tree. All three are variants of the same question: walk the tree, return a property.

⚙️ StreamCore: Cluster Topology as a Tree
Modeled the cluster as Topic → Partition → Replica Brokers. Each partition has one leader and N replicas. To find who owns a partition right now, you traverse:
- BFS: queue, level by level — Topic → all Partitions → all Replicas
- DFS: stack, depth first — Topic → Partition 0 → Replica 0 → Replica 1 → back up

Both traversals return the same leader for every partition. Tested it: 6 partitions across 3 brokers, RF=2. BFS and DFS agreed on all 6. When they diverge, you have a split-brain bug hiding in your topology.

The real payoff is reassignment. Broker dies → walk the replica list, pick the next healthy one, promote it. Leadership election in 12 lines. The tree makes it O(depth) instead of O(cluster).

☁️ DevOps: Kubernetes Headless Service for Discovery
Deployed as a StatefulSet: streamcore-broker-0, -1, -2. Stable DNS names from the ordinal index.
Headless Service (clusterIP: None) resolves the service name straight to pod IPs — no load balancer, no VIP. Brokers discover each other by resolving that DNS. Spin up a 4th replica and it just appears in the topology.

The test: killed broker-0 (leader for partitions 0 and 3). Watched the topology reassign to broker-1 and broker-2. Killed broker-1 next. Leadership moved to broker-2. Cluster health went from healthy → under-replicated → degraded but still serving reads. All 6 partitions stayed available.

Day 17 down, 43 to go.

#StreamCore #GraphTraversal #Kubernetes
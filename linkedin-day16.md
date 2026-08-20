A queue is just a line. First in, first out.

A circular queue is a line that wraps around — the person at the front leaves, the person at the back moves up, and the line never grows. Fixed size. O(1) everything.

That's the whole data structure. Two pointers. One array. 40 lines of code.

---

**DSA:** Queues & Deques — solved Design Circular Queue (LeetCode 622), Moving Average from Data Stream (LeetCode 346).

**StreamCore:** Built the producer-consumer core. Each partition gets a circular-queue buffer (10k records). Head pointer advances on append. Global offsets assigned monotonically — 0, 1, 2, 3... never reused.

Two consumer groups read the same partition. Fast group reads every message. Slow group reads every 5th. Each tracks its own offset independently. One never blocks the other. The buffer doesn't care — it serves data at whatever offset you ask for.

**DevOps:** Deployed the cluster to Kubernetes as a StatefulSet. Three brokers: `streamcore-broker-0`, `-1`, `-2`. Stable DNS. Each owns a PersistentVolumeClaim so offsets survive restarts. `kubectl get pods` shows them running with stable identities.

---

Day 16/60 — "Moving Average from Data Stream" was today's LeetCode problem and also, coincidentally, today's Data Analyst deliverable. Not a coincidence. That's the whole point of this series.

Run the demo:
```bash
python scripts/demo_consumer_groups.py
```

Watch two consumers read the same partition at different speeds. Fast catches up at offset 30. Slow sits at 14. The queue is 10k slots. The head pointer is the only state that moves.

#StreamCore #CircularQueue #ConsumerGroups #Kubernetes #Day16of60
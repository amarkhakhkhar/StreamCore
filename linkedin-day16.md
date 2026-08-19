Three weeks into building StreamCore, and the producer-consumer mechanics that make it a real broker finally landed.

A circular queue buffer is a fixed-size array with a head pointer and a tail pointer. When you append, head moves forward. When you consume, tail moves forward. When head meets tail, the oldest data overwrites the slot. O(1) reads and writes. One integer per record. No allocations.

Same data structure in a DSA textbook. Different stakes when you're trying to keep a broker alive under load.

---

**DSA:** Circular Queue — solved Implement Queue using Stacks, Design Circular Deque, Sliding Window Maximum.

**StreamCore:** Built a circular-queue-backed in-memory buffer for each partition. Fixed capacity (10,000 records). Head pointer advances on append. Global offset assignment so the queue maps cleanly to Kafka-style partition offsets.

**Consumer Groups:** Two independent consumer groups reading the same partition at different speeds. Fast group reads every message. Slow group reads every 5th message. Each group tracks its own offset. One never affects the other's progress. Same data structure, same offset arithmetic.

**DevOps:** Deployed the multi-broker cluster to Kubernetes as a StatefulSet. Three brokers, each with stable network identity (`streamcore-broker-0`, `-1`, `-2`). `kubectl get pods` shows all brokers running with stable names. Each broker owns its own PersistentVolumeClaim so consumer group offsets survive pod restarts.

---

Day 16/60 — the circular queue from week 1 of Leetcode, now the thing that lets multiple consumer groups read the same partition independently.

Run the demo:
```bash
python scripts/demo_consumer_groups.py
```

Watch two consumers read the same partition at different speeds without interfering with each other. The queue is 10,000 records. The offsets are integers. The head pointer is the only state that matters.

#StreamCore #CircularQueue #ConsumerGroups #Kubernetes #Day16of60

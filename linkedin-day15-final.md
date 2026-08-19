The Daily Temperatures problem keeps showing up in production.

LeetCode 739: "Given an array of temperatures, return how many days until a warmer day."

The optimal solution uses a monotonic decreasing stack. You push indices. When a warmer day arrives, you pop everything cooler and calculate the span — that span is exactly "consecutive days below current."

Same pattern. Different stakes.

---

**DSA:** Monotonic Stack — solved Daily Temperatures, Car Fleet, Largest Rectangle in Histogram, Next Greater Element, Stock Span.

**StreamCore:** Built a monotonic decreasing stack of *latency windows*. Each window = 1 second of append latencies. When a new window's average exceeds the stack top, we pop and accumulate span. That span tells us: "how many consecutive windows were below this spike?"

If span ≥ 3 windows AND avg > 50ms → backpressure activates. Producers get 429 with Retry-After. Broker survives the burst instead of falling over.

**DevOps:** CI now runs a scripted load test — 30s sustained at 500 RPS, then 5s burst at 2000 RPS. Verifies backpressure kicks in AND throughput stabilizes (P99 < 500ms). Plus an alert script that fires when latency crosses threshold. Zero manual steps.

---

Day 15/60 — halfway through Part 2. The same pattern that finds the next warmer day is now the thing keeping the broker alive under load.

Run it yourself:
```bash
python scripts/load_test_backpressure.py --burst-rate 2000
python scripts/check_backpressure_alert.py --inject-latency 200 --wait-bp
```

Watch the broker absorb the burst, activate backpressure at ~12s, and stabilize. The stack span calculation is ~40 lines. The alternative is a crashed broker and a 3am page.

#StreamCore #Backpressure #MonotonicStack #DailyTemperatures #SystemsEngineering #Day15of60
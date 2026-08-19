The Daily Temperatures problem is everywhere.

LeetCode 739: "Given an array of temperatures, return an array where answer[i] is the number of days you have to wait after day i to get a warmer temperature."

The optimal solution uses a **monotonic decreasing stack**. You push indices onto the stack. When a warmer day arrives, you pop everything cooler and calculate the span. That span = consecutive days below current.

Same pattern. Different stakes.

---

**DSA:** Monotonic Stack — solved Daily Temperatures, Next Greater Element, Stock Span, Largest Rectangle in Histogram.

**StreamCore:** Built a monotonic decreasing stack of *latency windows*. Each window = 1 second of append latencies. When a new window's average exceeds the stack top, we pop and accumulate span. That span tells us: "how many consecutive windows were below this spike?"

If span ≥ 3 windows AND avg > 50ms → backpressure activates. Producers get 429 with Retry-After. Broker survives.

**DevOps:** CI now runs a scripted load test — 30s sustained at 500 RPS, then 5s burst at 2000 RPS. Verifies backpressure kicks in AND throughput stabilizes (P99 < 5s). Plus an alert script that fires when latency crosses threshold. Zero manual steps.

---

Day 15/60 — The Daily Temperatures problem showing up again, this time deciding when to tell producers "slow down" instead of computing days until warmer weather.

Run it yourself:
```bash
python scripts/load_test_backpressure.py --burst-rate 2000
python scripts/check_backpressure_alert.py --inject-latency 200 --wait-bp
```

Watch the broker absorb the burst, activate backpressure, and stabilize. The stack span calculation is 40 lines of code. The alternative is a crashed broker and a 3am page.

#StreamCore #Backpressure #MonotonicStack #DailyTemperatures #SystemsEngineering #Day15of60
# Backpressure benchmark (2026-11-11)

- Python 3.14.7 on macOS-27.2.1-arm64-arm-64bit-Mach-O, CPU: Apple M4

## Collector down (every delivery refused)

- Captured 30,000 real exceptions (avg serialised size 1,359 B).
- Queue capacity 2,048; high-water mark 2,048; dropped when full: 27,852.
- Traced memory growth: 2,000 events: 2.9 MB, 10,000 events: 3.2 MB, 30,000 events: 3.2 MB; peak 3.17 MB.
- An unbounded queue would have held ~40.8 MB of events.
- `capture_exception` latency while the collector was down: p50 140.6 µs, p99 155.4 µs.

## Slow collector (50 ms per batch)

- Offered 10,000 events/s for 3 s (27,800 events).
- Delivered during the load window: 5,500 (1,833/s); dropped 20,152 at the bounded queue.
- Queue high-water mark 2,048; median event-loop lag 0.85 ms per 10 ms tick.
- Theoretical collector capacity: 100 events per 50 ms batch = 2,000 events/s.

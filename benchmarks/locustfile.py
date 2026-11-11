"""HTTP load test for the example payment service (end-to-end, over real sockets).

    uv sync --group bench
    uv run locust -f benchmarks/locustfile.py --host http://localhost:8000 \
        --headless -u 100 -r 20 -t 60s --csv benchmarks/results/locust

Run it three times against the same service started with different settings to
compare: no PulseTrace (``PULSETRACE_ENDPOINT=`` empty disables the SDK),
instrumented with ``PULSETRACE_TRACES_SAMPLE_RATE=0``, and the full pipeline.
Record hardware, Python version, configuration and duration with the results.
"""

from __future__ import annotations

import random

from locust import HttpUser, between, task


class Shopper(HttpUser):
    wait_time = between(0.01, 0.1)

    @task(20)
    def checkout(self) -> None:
        self.client.post(
            "/checkout",
            json={
                "customer_id": f"c-{random.randint(1, 500)}",
                "amount": random.uniform(10, 5_000),
            },
            name="/checkout (ok)",
        )

    @task(2)
    def invalid_amount(self) -> None:
        with self.client.post(
            "/checkout",
            json={"customer_id": "c-x", "amount": -1},
            name="/checkout (error)",
            catch_response=True,
        ) as response:
            if response.status_code == 500:
                response.success()

    @task(1)
    def slow_report(self) -> None:
        self.client.get("/slow")

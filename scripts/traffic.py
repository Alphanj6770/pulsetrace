"""Send a realistic mix of requests to the payment service and print what the SDK did.

uv run python scripts/traffic.py --requests 60 --base-url http://127.0.0.1:8000
"""

from __future__ import annotations

import argparse
import random
import uuid

import httpx


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--requests", type=int, default=60)
    args = parser.parse_args()

    outcomes: dict[str, int] = {}
    traces: list[str] = []
    # No keep-alive: uvicorn closes a connection after an unhandled exception without
    # sending "Connection: close", so reusing it races the close (with or without
    # PulseTrace - verified). A fresh connection per request avoids spurious resets.
    limits = httpx.Limits(max_keepalive_connections=0)
    with httpx.Client(base_url=args.base_url, timeout=10, limits=limits) as client:
        for _ in range(args.requests):
            roll = random.random()
            trace_id = uuid.uuid4().hex
            headers = {"traceparent": f"00-{trace_id}-{uuid.uuid4().hex[:16]}-01"}
            if roll < 0.60:
                label, response = (
                    "checkout ok",
                    client.post(
                        "/checkout",
                        json={
                            "customer_id": f"c-{random.randint(1, 50)}",
                            "amount": random.uniform(10, 5000),
                        },
                        headers=headers,
                    ),
                )
                traces.append(trace_id)
            elif roll < 0.72:
                label, response = (
                    "checkout declined",
                    client.post(
                        "/checkout",
                        json={"customer_id": "c-risky", "amount": 22_000},
                        headers=headers,
                    ),
                )
            elif roll < 0.82:
                label, response = (
                    "invalid amount",
                    client.post(
                        "/checkout",
                        json={"customer_id": "c-typo", "amount": -random.randint(1, 99)},
                    ),
                )
            elif roll < 0.88:
                label, response = (
                    "fraud service failure",
                    client.post(
                        "/checkout",
                        json={"customer_id": "c-whale", "amount": 75_000},
                        headers=headers,
                    ),
                )
                traces.append(trace_id)
            elif roll < 0.94:
                label, response = (
                    "refund error",
                    client.post(f"/refunds/pay-{random.randint(1, 9)}"),
                )
            else:
                label, response = "slow report", client.get("/slow")
            key = f"{label} -> {response.status_code}"
            outcomes[key] = outcomes.get(key, 0) + 1

        for key, count in sorted(outcomes.items()):
            print(f"{count:4d}  {key}")
        stats = client.get("/debug/telemetry").json()
        print("\nSDK stats:", stats["events"])
        print(
            "Transport:",
            {
                k: stats["transport"].get(k)
                for k in ("queue_depth", "delivered", "retries", "dropped_queue_full", "last_error")
            },
        )
        if traces:
            print(f"\nOpen a trace: http://127.0.0.1:9000/ui/traces/{traces[-1]}")


if __name__ == "__main__":
    main()

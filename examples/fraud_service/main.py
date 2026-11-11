"""Sample downstream fraud-scoring service.

    uvicorn examples.fraud_service.main:app --port 8001

It continues the caller's trace from the ``traceparent`` header automatically
(via ``PulseTraceMiddleware``), so its spans and errors appear inside the
payment service's trace.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from pulsetrace import PulseTrace, PulseTraceMiddleware
from pulsetrace.integrations.logging import LoggingIntegration

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("fraud_service")

telemetry = PulseTrace(
    endpoint=os.environ.get("PULSETRACE_ENDPOINT", "http://localhost:9000/v1/events"),
    service_name="fraud-service",
    environment=os.environ.get("PULSETRACE_ENVIRONMENT", "development"),
    release="0.9.3",
    traces_sample_rate=float(os.environ.get("PULSETRACE_TRACES_SAMPLE_RATE", "1.0")),
    flush_interval=float(os.environ.get("PULSETRACE_FLUSH_INTERVAL", "1.0")),
    max_retries=int(os.environ.get("PULSETRACE_MAX_RETRIES", "5")),
    initial_backoff=float(os.environ.get("PULSETRACE_INITIAL_BACKOFF", "0.5")),
    max_backoff=float(os.environ.get("PULSETRACE_MAX_BACKOFF", "30")),
    integrations=[LoggingIntegration()],
)


class ScoreRequest(BaseModel):
    customer_id: str
    amount: float
    currency: str = "INR"


class FraudModelError(Exception):
    pass


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    await telemetry.start()
    yield
    await telemetry.shutdown(timeout=3.0)


app = FastAPI(title="fraud-service", lifespan=lifespan)
app.add_middleware(PulseTraceMiddleware, client=telemetry)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/score")
async def score(request: ScoreRequest) -> dict[str, object]:
    logger.info("Scoring transaction for %s", request.customer_id)
    async with telemetry.start_span("fraud.model.predict", op="ml.inference"):
        await asyncio.sleep(random.uniform(0.01, 0.04))
        if request.amount >= 50_000:
            raise FraudModelError("Fraud model unavailable for high-value transactions")
        risk = min(request.amount / 25_000, 0.99)
    decision = "decline" if risk > 0.8 else "approve"
    return {"decision": decision, "score": round(risk, 3)}

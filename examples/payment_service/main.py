"""Sample payment service instrumented with PulseTrace.

    uvicorn examples.payment_service.main:app --port 8000

Endpoints worth trying:

* ``POST /checkout {"customer_id": "c1", "amount": 250}`` - happy path: a trace
  spanning this service, the fraud service and the payments database.
* ``POST /checkout {"customer_id": "c1", "amount": -5}`` - unhandled ``ValueError``.
* ``POST /checkout {"customer_id": "c1", "amount": 60000}`` - the fraud service
  fails; this service raises ``PaymentProcessingError`` *from* the HTTP error.
* ``POST /refunds/<id>`` - a chained ``RefundError`` caused by a ``KeyError``.
* ``GET /slow`` - latency for the performance view.
* ``GET /debug/telemetry`` - the SDK's own counters (queued, retried, dropped).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import String, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from pulsetrace import PulseTrace, PulseTraceMiddleware
from pulsetrace.integrations.httpx import HttpxIntegration
from pulsetrace.integrations.logging import LoggingIntegration
from pulsetrace.integrations.sqlalchemy import SqlAlchemyIntegration

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
logger = logging.getLogger("payment_service")

FRAUD_SERVICE_URL = os.environ.get("FRAUD_SERVICE_URL", "http://localhost:8001")
engine = create_async_engine(
    os.environ.get("PAYMENTS_DATABASE_URL", "sqlite+aiosqlite:///./payments.db")
)
Session = async_sessionmaker(engine, expire_on_commit=False)

telemetry = PulseTrace(
    endpoint=os.environ.get("PULSETRACE_ENDPOINT", "http://localhost:9000/v1/events"),
    service_name="payment-service",
    environment=os.environ.get("PULSETRACE_ENVIRONMENT", "development"),
    release="1.2.0",
    traces_sample_rate=float(os.environ.get("PULSETRACE_TRACES_SAMPLE_RATE", "1.0")),
    queue_size=2048,
    # Knobs the end-to-end tests turn down so outages resolve in seconds.
    flush_interval=float(os.environ.get("PULSETRACE_FLUSH_INTERVAL", "1.0")),
    max_retries=int(os.environ.get("PULSETRACE_MAX_RETRIES", "5")),
    initial_backoff=float(os.environ.get("PULSETRACE_INITIAL_BACKOFF", "0.5")),
    max_backoff=float(os.environ.get("PULSETRACE_MAX_BACKOFF", "30")),
    integrations=[HttpxIntegration(), LoggingIntegration(), SqlAlchemyIntegration(engine)],
)


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(64))
    amount: Mapped[float]
    currency: Mapped[str] = mapped_column(String(3))
    status: Mapped[str] = mapped_column(String(16))


class CheckoutRequest(BaseModel):
    customer_id: str = Field(max_length=64)
    amount: float
    currency: str = Field(default="INR", min_length=3, max_length=3)


class PaymentProcessingError(Exception):
    pass


class RefundError(Exception):
    pass


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await telemetry.start()
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    app.state.fraud = httpx.AsyncClient(base_url=FRAUD_SERVICE_URL, timeout=2.0)
    yield
    await app.state.fraud.aclose()
    report = await telemetry.shutdown(timeout=3.0)
    logger.info("telemetry shut down: delivered=%d unsent=%d", report.delivered, report.unsent)
    await engine.dispose()


app = FastAPI(title="payment-service", lifespan=lifespan)
app.add_middleware(PulseTraceMiddleware, client=telemetry)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/checkout")
async def checkout(order: CheckoutRequest, request: Request) -> dict[str, Any]:
    telemetry.set_user({"id": order.customer_id})
    telemetry.set_tag("currency", order.currency)
    logger.info("Checkout initiated for %s", order.customer_id)

    if order.amount <= 0:
        raise ValueError(f"Invalid payment amount: {order.amount}")

    async with telemetry.start_span("payment.fraud_check"):
        try:
            response = await request.app.state.fraud.post("/score", json=order.model_dump())
            response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning("Fraud service unavailable: %s", type(exc).__name__)
            raise PaymentProcessingError("Payment processing failed") from exc
    verdict = response.json()
    if verdict["decision"] == "decline":
        logger.info("Payment declined with fraud score %.2f", verdict["score"])
        raise HTTPException(status_code=402, detail="Payment declined")

    payment = Payment(
        id=str(uuid.uuid4()),
        customer_id=order.customer_id,
        amount=order.amount,
        currency=order.currency,
        status="captured",
    )
    async with (
        telemetry.start_span("payment.persist", op="db"),
        Session() as session,
        session.begin(),
    ):
        session.add(payment)
    logger.info("Payment request created: %s", payment.id)
    return {"payment_id": payment.id, "status": payment.status, "fraud_score": verdict["score"]}


@app.get("/payments/{payment_id}")
async def get_payment(payment_id: str) -> dict[str, Any]:
    async with Session() as session:
        payment = (
            await session.execute(select(Payment).where(Payment.id == payment_id))
        ).scalar_one_or_none()
    if payment is None:
        raise HTTPException(status_code=404, detail="payment not found")
    return {"payment_id": payment.id, "amount": payment.amount, "status": payment.status}


REFUNDABLE = {"captured": "refund_pending"}


@app.post("/refunds/{payment_id}")
async def refund(payment_id: str) -> dict[str, Any]:
    logger.info("Refund requested for %s", payment_id)
    status = "settled"  # a status the refund table does not know about
    try:
        next_status = REFUNDABLE[status]
    except KeyError as exc:
        raise RefundError(f"Cannot refund payment {payment_id} in status {status!r}") from exc
    return {"payment_id": payment_id, "status": next_status}


@app.get("/slow")
async def slow() -> dict[str, float]:
    delay = random.uniform(0.2, 0.8)
    async with telemetry.start_span("report.generate"):
        await asyncio.sleep(delay)
    return {"seconds": round(delay, 3)}


@app.get("/debug/telemetry")
async def telemetry_stats() -> dict[str, Any]:
    return telemetry.stats()

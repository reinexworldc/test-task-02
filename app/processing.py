import asyncio
import logging
import random
from datetime import timedelta
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from app.config import get_settings
from app.db import sessions
from app.models import Outbox, Payment, utcnow
from app.schemas import PaymentDetail, payment_data

logger = logging.getLogger(__name__)
MAX_ATTEMPTS = 3


class PaymentEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payment_id: UUID
    attempt: int = Field(ge=1, le=MAX_ATTEMPTS)


async def emulate_gateway() -> str:
    await asyncio.sleep(random.uniform(2, 5))
    return "succeeded" if random.random() < 0.9 else "failed"


async def send_webhook(payment: Payment):
    payload = PaymentDetail(**payment_data(payment)).model_dump(mode="json")
    async with httpx.AsyncClient(
        timeout=get_settings().webhook_timeout_seconds, follow_redirects=False, trust_env=False
    ) as client:
        response = await client.post(
            payment.webhook_url,
            json={
                "event_id": str(payment.id),
                "event": "payment.processed",
                "payment": payload,
            },
            headers={"Idempotency-Key": str(payment.id)},
        )
        if not 200 <= response.status_code < 300:
            raise httpx.HTTPStatusError(
                "Webhook returned a non-2xx response", request=response.request, response=response
            )


async def process_payment(
    event: PaymentEvent, session_factory=sessions, gateway=emulate_gateway, webhook=send_webhook
):
    # Persist the gateway outcome before attempting a webhook.
    async with session_factory() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment is None:
            raise LookupError("Payment does not exist")
        if payment.webhook_delivered_at or event.attempt <= payment.attempts:
            return
        if payment.status == "pending":
            try:
                payment.status = await gateway()
                payment.processed_at = utcnow()
            except Exception as exc:  # noqa: BLE001
                schedule_failure(session, payment, event, exc)
                return

    async with session_factory() as session, session.begin():
        payment = await session.scalar(
            select(Payment).where(Payment.id == event.payment_id).with_for_update()
        )
        if payment.webhook_delivered_at or event.attempt <= payment.attempts:
            return
        payment.attempts = event.attempt
        try:
            await webhook(payment)
        except Exception as exc:  # noqa: BLE001
            schedule_failure(session, payment, event, exc)
        else:
            payment.webhook_delivered_at = utcnow()
            payment.last_error = None


def schedule_failure(session, payment: Payment, event: PaymentEvent, error: Exception):
    payment.attempts = event.attempt
    payment.last_error = type(error).__name__
    dead = event.attempt >= MAX_ATTEMPTS
    delay = 0 if dead else get_settings().retry_base_seconds * 2 ** (event.attempt - 1)
    payload = {
        "payment_id": str(payment.id),
        "attempt": event.attempt if dead else event.attempt + 1,
    }
    if dead:
        payload["error"] = payment.last_error
    session.add(
        Outbox(
            routing_key="payments.dead" if dead else "payments.new",
            payload=payload,
            available_at=utcnow() + timedelta(seconds=delay),
        )
    )
    logger.warning(
        "Payment %s attempt %s failed (%s), destination=%s",
        payment.id,
        event.attempt,
        payment.last_error,
        "DLQ" if dead else "retry",
    )

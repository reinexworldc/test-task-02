from datetime import UTC
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from sqlalchemy import select

from app.models import Outbox, Payment
from app.processing import PaymentEvent, emulate_gateway, process_payment, send_webhook
from tests.conftest import PAYMENT


async def create(client):
    response = await client.post(
        "/api/v1/payments", json=PAYMENT, headers={"Idempotency-Key": "process"}
    )
    return UUID(response.json()["payment_id"])


@pytest.mark.parametrize("outcome", ["succeeded", "failed"])
async def test_gateway_outcome_and_duplicate_delivery(client, database, outcome):
    payment_id = await create(client)
    gateway = AsyncMock(return_value=outcome)
    webhook = AsyncMock()
    event = PaymentEvent(payment_id=payment_id, attempt=1)
    await process_payment(event, database, gateway, webhook)
    await process_payment(event, database, gateway, webhook)
    gateway.assert_awaited_once()
    webhook.assert_awaited_once()
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == outcome
        assert payment.processed_at is not None
        assert payment.webhook_delivered_at is not None


async def test_webhook_three_attempts_backoff_and_dlq(client, database):
    payment_id = await create(client)
    gateway = AsyncMock(return_value="succeeded")
    webhook = AsyncMock(side_effect=httpx.ConnectError("offline"))
    for attempt in range(1, 4):
        event = PaymentEvent(payment_id=payment_id, attempt=attempt)
        await process_payment(event, database, gateway, webhook)
        await process_payment(event, database, gateway, webhook)
    gateway.assert_awaited_once()
    assert webhook.await_count == 3
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "succeeded"
        assert payment.attempts == 3
        assert payment.webhook_delivered_at is None
        events = (await session.scalars(select(Outbox).order_by(Outbox.created_at))).all()
        assert len(events) == 4
        assert [event.payload["attempt"] for event in events] == [1, 2, 3, 3]
        assert events[-1].routing_key == "payments.dead"
        for event, delay in zip(events[1:3], [2, 4], strict=True):
            actual = (
                event.available_at.replace(tzinfo=UTC) - event.created_at.replace(tzinfo=UTC)
            ).total_seconds()
            assert delay - 0.5 < actual <= delay


async def test_retry_then_webhook_success(client, database):
    payment_id = await create(client)
    gateway = AsyncMock(return_value="failed")
    webhook = AsyncMock(side_effect=[httpx.ConnectError("offline"), None])
    await process_payment(
        PaymentEvent(payment_id=payment_id, attempt=1), database, gateway, webhook
    )
    await process_payment(
        PaymentEvent(payment_id=payment_id, attempt=2), database, gateway, webhook
    )
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        assert payment.status == "failed"
        assert payment.webhook_delivered_at is not None
        assert payment.last_error is None
    gateway.assert_awaited_once()


async def test_gateway_failure_retries_are_deduplicated(client, database):
    payment_id = await create(client)
    gateway = AsyncMock(side_effect=RuntimeError("unavailable"))
    webhook = AsyncMock()
    for attempt in range(1, 4):
        event = PaymentEvent(payment_id=payment_id, attempt=attempt)
        await process_payment(event, database, gateway, webhook)
        await process_payment(event, database, gateway, webhook)
    assert gateway.await_count == 3
    webhook.assert_not_awaited()
    async with database() as session:
        events = (await session.scalars(select(Outbox))).all()
        assert len(events) == 4
        assert sum(event.routing_key == "payments.dead" for event in events) == 1


@pytest.mark.parametrize(("random_value", "expected"), [(0.89, "succeeded"), (0.9, "failed")])
async def test_gateway_delay_and_probability(monkeypatch, random_value, expected):
    sleep = AsyncMock()
    monkeypatch.setattr("app.processing.asyncio.sleep", sleep)
    monkeypatch.setattr("app.processing.random.random", lambda: random_value)
    assert await emulate_gateway() == expected
    assert 2 <= sleep.await_args.args[0] <= 5


@pytest.mark.parametrize("status", [200, 204, 302, 400, 500])
async def test_webhook_response_handling(client, database, monkeypatch, status):
    payment_id = await create(client)
    async with database() as session:
        payment = await session.get(Payment, payment_id)
        payment.status = "succeeded"
        real_client = httpx.AsyncClient

        def handler(request):
            assert request.headers["Idempotency-Key"] == str(payment_id)
            return httpx.Response(status)

        monkeypatch.setattr(
            "app.processing.httpx.AsyncClient",
            lambda **kwargs: real_client(transport=httpx.MockTransport(handler), **kwargs),
        )
        if status < 300:
            await send_webhook(payment)
        else:
            with pytest.raises(httpx.HTTPStatusError):
                await send_webhook(payment)

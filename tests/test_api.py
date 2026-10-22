import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.models import Outbox, Payment
from tests.conftest import PAYMENT


async def test_create_get_and_atomic_outbox(client, database):
    response = await client.post(
        "/api/v1/payments", json=PAYMENT, headers={"Idempotency-Key": "order-42"}
    )
    assert response.status_code == 202
    data = response.json()
    assert data["status"] == "pending"
    detail = await client.get(f"/api/v1/payments/{data['payment_id']}")
    assert detail.status_code == 200
    assert detail.json()["metadata"] == {"order_id": 42}
    assert detail.json()["amount"] == "100.00"
    async with database() as session:
        event = await session.scalar(select(Outbox))
        assert event.payload == {"payment_id": data["payment_id"], "attempt": 1}
        assert event.published_at is None


async def test_idempotency_and_conflicting_payload(client, database):
    first = await client.post("/api/v1/payments", json=PAYMENT, headers={"Idempotency-Key": "same"})
    again = await client.post(
        "/api/v1/payments", json={**PAYMENT, "amount": "100"}, headers={"Idempotency-Key": "same"}
    )
    assert again.status_code == 202
    assert first.json()["payment_id"] == again.json()["payment_id"]
    assert first.json()["status"] == again.json()["status"]
    assert datetime.fromisoformat(first.json()["created_at"]).replace(
        tzinfo=UTC
    ) == datetime.fromisoformat(again.json()["created_at"]).replace(tzinfo=UTC)
    conflict = await client.post(
        "/api/v1/payments", json={**PAYMENT, "amount": "101"}, headers={"Idempotency-Key": "same"}
    )
    assert conflict.status_code == 409
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1


@pytest.mark.parametrize(
    "path", ["/api/v1/payments", "/health", "/openapi.json", f"/api/v1/payments/{uuid4()}"]
)
async def test_authentication(client, path):
    method = client.post if path == "/api/v1/payments" else client.get
    assert (await method(path, headers={"X-API-Key": "wrong"})).status_code == 401
    client.headers.pop("X-API-Key")
    assert (await method(path)).status_code == 401


@pytest.mark.parametrize(
    "changes",
    [
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.001"},
        {"amount": "NaN"},
        {"amount": "10000000000000000"},
        {"currency": "GBP"},
        {"webhook_url": "ftp://test.com"},
        {"metadata": []},
        {"unknown": True},
    ],
)
async def test_validation(client, changes):
    response = await client.post(
        "/api/v1/payments", json={**PAYMENT, **changes}, headers={"Idempotency-Key": "invalid"}
    )
    assert response.status_code == 422


async def test_missing_idempotency_key_and_not_found(client):
    assert (await client.post("/api/v1/payments", json=PAYMENT)).status_code == 422
    assert (await client.get(f"/api/v1/payments/{uuid4()}")).status_code == 404


async def test_concurrent_idempotency_postgres(client, database):
    async with database() as session:
        if session.bind.dialect.name != "postgresql":
            pytest.skip("Requires PostgreSQL unique constraint concurrency")
    responses = await asyncio.gather(
        *[
            client.post("/api/v1/payments", json=PAYMENT, headers={"Idempotency-Key": "race"})
            for _ in range(8)
        ]
    )
    assert all(response.status_code == 202 for response in responses)
    assert len({response.json()["payment_id"] for response in responses}) == 1
    async with database() as session:
        assert await session.scalar(select(func.count()).select_from(Outbox)) == 1

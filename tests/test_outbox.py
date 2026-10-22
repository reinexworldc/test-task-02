from datetime import timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from app.models import Outbox, utcnow
from app.outbox import publish_one


async def test_publication_confirmation_and_rollback(database):
    async with database() as session, session.begin():
        event = Outbox(payload={"payment_id": "123", "attempt": 1})
        session.add(event)
    publisher = AsyncMock()
    publisher.publish.side_effect = ConnectionError("broker unavailable")
    with pytest.raises(ConnectionError):
        await publish_one(database, publisher)
    async with database() as session:
        assert (await session.get(Outbox, event.id)).published_at is None
    publisher.publish.side_effect = None
    assert await publish_one(database, publisher)
    assert publisher.publish.await_args.kwargs["persist"] is True
    assert publisher.publish.await_args.kwargs["mandatory"] is True
    async with database() as session:
        assert (await session.get(Outbox, event.id)).published_at is not None
    assert not await publish_one(database, publisher)


async def test_future_retries_are_not_published(database):
    async with database() as session, session.begin():
        session.add(Outbox(payload={}, available_at=utcnow() + timedelta(hours=1)))
    publisher = AsyncMock()
    assert not await publish_one(database, publisher)
    publisher.publish.assert_not_awaited()


async def test_dead_letter_routing(database):
    async with database() as session, session.begin():
        session.add(Outbox(payload={"error": "ConnectError"}, routing_key="payments.dead"))
    publisher = AsyncMock()
    assert await publish_one(database, publisher)
    assert publisher.publish.await_args.kwargs["exchange"].name == "payments.dlx"
    async with database() as session:
        assert (await session.scalar(select(Outbox))).published_at is not None

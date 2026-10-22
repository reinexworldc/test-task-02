import asyncio
import logging

from sqlalchemy import select

from app.config import get_settings
from app.db import sessions
from app.messaging import broker, dlx, exchange
from app.models import Outbox, utcnow

logger = logging.getLogger(__name__)


async def publish_one(session_factory=sessions, publisher=broker) -> bool:
    async with session_factory() as session, session.begin():
        event = await session.scalar(
            select(Outbox)
            .where(Outbox.published_at.is_(None), Outbox.available_at <= utcnow())
            .order_by(Outbox.available_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if event is None:
            return False
        await publisher.publish(
            event.payload,
            exchange=dlx if event.routing_key == "payments.dead" else exchange,
            routing_key=event.routing_key,
            persist=True,
            mandatory=True,
            timeout=10,
            message_id=str(event.id),
        )
        event.published_at = utcnow()
    return True


async def relay():
    while True:
        try:
            if await publish_one():
                continue
        except Exception:
            logger.exception("Outbox publication failed; transaction rolled back")
        await asyncio.sleep(get_settings().outbox_poll_seconds)

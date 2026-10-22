import asyncio
from contextlib import suppress
from typing import Any

from faststream import FastStream
from faststream.rabbit import RabbitMessage
from pydantic import ValidationError

from app.db import engine
from app.messaging import broker, declare_topology, exchange, queue
from app.outbox import relay
from app.processing import PaymentEvent, process_payment

app = FastStream(broker)
relay_task: asyncio.Task | None = None


@broker.subscriber(queue, exchange, retry=True)
async def consume(body: Any, message: RabbitMessage):
    try:
        event = PaymentEvent.model_validate(body)
    except ValidationError:
        await message.reject(requeue=False)
        return
    try:
        await process_payment(event)
    except LookupError:
        await message.reject(requeue=False)
    except Exception:
        # Infrastructure failures must not acknowledge uncommitted work.
        await asyncio.sleep(2)
        raise


@app.on_startup
async def prepare_topology():
    await broker.connect()
    await declare_topology()


@app.after_startup
async def start_relay():
    global relay_task
    relay_task = asyncio.create_task(relay())


@app.on_shutdown
async def stop_relay():
    if relay_task:
        relay_task.cancel()
        with suppress(asyncio.CancelledError):
            await relay_task
    await engine.dispose()

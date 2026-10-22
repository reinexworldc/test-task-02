import os

os.environ.setdefault("API_KEY", "test-api-key-123456789")

import httpx
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.api import app
from app.db import get_session
from app.models import Base


@pytest_asyncio.fixture
async def database():
    engine = create_async_engine(os.environ.get("TEST_DATABASE_URL", "sqlite+aiosqlite://"))
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


@pytest_asyncio.fixture
async def client(database):
    async def override():
        async with database() as session:
            yield session

    app.dependency_overrides[get_session] = override
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": os.environ["API_KEY"]},
    ) as client:
        yield client
    app.dependency_overrides.clear()


PAYMENT = {
    "amount": "100.00",
    "currency": "RUB",
    "description": "Order 42",
    "metadata": {"order_id": 42},
    "webhook_url": "https://merchant.example/webhook",
}

import hashlib
import hmac
import json
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID

from fastapi import Depends, FastAPI, Header, HTTPException
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.db import engine, get_session
from app.models import Outbox, Payment
from app.schemas import PaymentAccepted, PaymentCreate, PaymentDetail, payment_data


async def authenticate(x_api_key: Annotated[str | None, Header()] = None):
    expected = get_settings().api_key.get_secret_value()
    if x_api_key is None or not hmac.compare_digest(x_api_key.encode(), expected.encode()):
        raise HTTPException(401, "Invalid API key")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await engine.dispose()


app = FastAPI(
    title="Asynchronous Payments",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(authenticate)],
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)
Session = Annotated[AsyncSession, Depends(get_session)]


@app.post("/api/v1/payments", status_code=202, response_model=PaymentAccepted)
async def create_payment(
    body: PaymentCreate,
    session: Session,
    idempotency_key: Annotated[str, Header(min_length=1, max_length=255, pattern=r"^\S+$")],
):
    data = body.model_dump(mode="json")
    fingerprint = hashlib.sha256(
        json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    payment = Payment(
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        extra_data=body.metadata,
        webhook_url=str(body.webhook_url),
        idempotency_key=idempotency_key,
        request_hash=fingerprint,
    )
    try:
        async with session.begin():
            session.add(payment)
            await session.flush()
            session.add(Outbox(payload={"payment_id": str(payment.id), "attempt": 1}))
    except IntegrityError:
        payment = await session.scalar(
            select(Payment).where(Payment.idempotency_key == idempotency_key)
        )
        if payment is None:
            raise
        if payment.request_hash != fingerprint:
            raise HTTPException(
                409, "Idempotency key already used with a different request"
            ) from None
    return payment_data(payment)


@app.get("/api/v1/payments/{payment_id}", response_model=PaymentDetail)
async def get_payment(payment_id: UUID, session: Session):
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise HTTPException(404, "Payment not found")
    return payment_data(payment)


@app.get("/health")
async def health(session: Session):
    await session.execute(text("SELECT 1"))
    return {"status": "ok"}


@app.get("/openapi.json", include_in_schema=False)
async def openapi_schema():
    return app.openapi()

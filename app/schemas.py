from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    amount: Annotated[Decimal, Field(gt=0, max_digits=18, decimal_places=2, allow_inf_nan=False)]
    currency: Literal["RUB", "USD", "EUR"]
    description: str = Field(max_length=1000)
    metadata: dict = Field(default_factory=dict)
    webhook_url: HttpUrl = Field(max_length=2048)

    @field_validator("amount")
    @classmethod
    def normalize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: Literal["pending", "succeeded", "failed"]
    created_at: datetime


class PaymentDetail(PaymentAccepted):
    amount: Decimal
    currency: str
    description: str
    metadata: dict
    webhook_url: str
    processed_at: datetime | None
    webhook_delivered_at: datetime | None
    attempts: int


def payment_data(payment) -> dict:
    return {
        "payment_id": payment.id,
        "amount": payment.amount,
        "currency": payment.currency,
        "description": payment.description,
        "metadata": payment.extra_data,
        "status": payment.status,
        "webhook_url": payment.webhook_url,
        "created_at": payment.created_at,
        "processed_at": payment.processed_at,
        "webhook_delivered_at": payment.webhook_delivered_at,
        "attempts": payment.attempts,
    }

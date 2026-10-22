"""Opt-in end-to-end checks against an already running Docker Compose stack."""

import asyncio
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from uuid import uuid4

import httpx
import pytest

from tests.conftest import PAYMENT

pytestmark = pytest.mark.skipif(
    os.environ.get("LIVE_TESTS") != "1", reason="Set LIVE_TESTS=1 with Compose running"
)


@pytest.fixture
def receiver():
    deliveries = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            deliveries.append((body, self.headers["Idempotency-Key"], time.monotonic()))
            self.send_response(500 if self.path == "/failure" else 200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("0.0.0.0", 9001), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield deliveries
    server.shutdown()
    server.server_close()
    thread.join()


@pytest.mark.parametrize("path", ["success", "failure"])
async def test_real_payment_webhook_and_dlq(receiver, path):
    headers = {
        "X-API-Key": os.environ.get("LIVE_API_KEY", "local-development-key-change-me"),
        "Idempotency-Key": str(uuid4()),
    }
    body = {**PAYMENT, "webhook_url": f"http://host.docker.internal:9001/{path}"}
    async with httpx.AsyncClient(base_url="http://localhost:8000", headers=headers) as client:
        created = await client.post("/api/v1/payments", json=body)
        assert created.status_code == 202, created.text
        payment_id = created.json()["payment_id"]
        duplicate = await client.post("/api/v1/payments", json=body)
        assert duplicate.json()["payment_id"] == payment_id
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            detail = (await client.get(f"/api/v1/payments/{payment_id}")).json()
            if detail["webhook_delivered_at"] or detail["attempts"] == 3:
                break
            await asyncio.sleep(0.25)
        else:
            pytest.fail(f"Payment did not finish: {detail}")
        assert detail["status"] in ("succeeded", "failed")
        assert all(item[1] == payment_id for item in receiver)
        if path == "success":
            assert detail["attempts"] == 1
            assert detail["webhook_delivered_at"] is not None
            assert len(receiver) == 1
        else:
            assert detail["webhook_delivered_at"] is None
            assert len(receiver) == 3
            assert receiver[1][2] - receiver[0][2] >= 2
            assert receiver[2][2] - receiver[1][2] >= 4
            async with httpx.AsyncClient(auth=("payments", "payments")) as admin:
                while time.monotonic() < deadline:
                    response = await admin.post(
                        "http://localhost:15672/api/queues/%2F/payments.dlq/get",
                        json={
                            "count": 100,
                            "ackmode": "ack_requeue_true",
                            "encoding": "auto",
                            "truncate": 50000,
                        },
                    )
                    response.raise_for_status()
                    payloads = [json.loads(item["payload"]) for item in response.json()]
                    if any(item.get("payment_id") == payment_id for item in payloads):
                        break
                    await asyncio.sleep(0.25)
                else:
                    pytest.fail("Message was not delivered to the real DLQ")

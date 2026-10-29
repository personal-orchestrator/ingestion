import os
import sqlite3
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.config import settings
from app.main import app

BATCH = {
    "device_id": "pixel-1",
    "events": [
        {"timestamp": 1000, "package": "android", "event_type": "SCREEN_INTERACTIVE"},
        {"timestamp": 1001, "package": "android", "event_type": "KEYGUARD_HIDDEN"},
        {"timestamp": 1002, "package": "com.example.chat", "event_type": "ACTIVITY_RESUMED"},
    ],
}

@pytest.fixture(autouse=True)
def storage_dir(tmp_path):
    settings.storage_dir = str(tmp_path)
    yield

@pytest_asyncio.fixture
async def async_client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client

def stored_rows():
    with sqlite3.connect(os.path.join(settings.storage_dir, "usage_events.db")) as conn:
        return conn.execute("SELECT device_id, timestamp, package, event_type FROM usage_events ORDER BY timestamp").fetchall()

@pytest.mark.asyncio
async def test_usage_events_are_stored(async_client: AsyncClient):
    response = await async_client.post(
        "/usage-events",
        headers={"Authorization": f"Bearer {settings.usage_token}"},
        json=BATCH,
    )

    assert response.status_code == 200
    assert response.json() == {"received": 3, "inserted": 3}
    assert stored_rows() == [
        ("pixel-1", 1000, "android", "SCREEN_INTERACTIVE"),
        ("pixel-1", 1001, "android", "KEYGUARD_HIDDEN"),
        ("pixel-1", 1002, "com.example.chat", "ACTIVITY_RESUMED"),
    ]

@pytest.mark.asyncio
async def test_resending_overlapping_batch_stores_only_new_events(async_client: AsyncClient):
    headers = {"Authorization": f"Bearer {settings.usage_token}"}
    await async_client.post("/usage-events", headers=headers, json=BATCH)

    overlapping = {
        "device_id": "pixel-1",
        "events": BATCH["events"] + [
            {"timestamp": 1003, "package": "com.example.chat", "event_type": "ACTIVITY_PAUSED"},
        ],
    }
    response = await async_client.post("/usage-events", headers=headers, json=overlapping)

    assert response.status_code == 200
    assert response.json() == {"received": 4, "inserted": 1}
    assert len(stored_rows()) == 4

@pytest.mark.asyncio
async def test_usage_events_reject_wrong_token(async_client: AsyncClient):
    response = await async_client.post(
        "/usage-events",
        headers={"Authorization": "Bearer wrong"},
        json=BATCH,
    )
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_usage_events_reject_watch_credentials(async_client: AsyncClient):
    response = await async_client.post(
        "/usage-events",
        auth=(settings.auth_username, settings.auth_password),
        json=BATCH,
    )
    assert response.status_code == 401

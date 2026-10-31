import os
import sqlite3
from contextlib import closing
import pytest
from httpx import AsyncClient

from app.config import Settings, settings
from app.main import MAX_BODY_BYTES, MAX_USAGE_EVENTS, MAX_USAGE_TEXT_LENGTH

AUTH = {"Authorization": "Bearer test-token", "Content-Type": "application/json"}

BATCH = {
    "device_id": "pixel-1",
    "events": [
        {"timestamp": 1000, "package": "android", "event_type": "SCREEN_INTERACTIVE"},
        {"timestamp": 1001, "package": "android", "event_type": "KEYGUARD_HIDDEN"},
        {"timestamp": 1002, "package": "com.example.chat", "event_type": "ACTIVITY_RESUMED"},
    ],
}

@pytest.fixture(autouse=True)
def configure(monkeypatch):
    monkeypatch.setattr(settings, "usage_token", "test-token")

def stored_rows():
    with closing(sqlite3.connect(os.path.join(settings.usage_dir, "usage_events.db"))) as conn:
        return conn.execute("SELECT device_id, timestamp, package, event_type, class_name FROM usage_events ORDER BY timestamp").fetchall()

@pytest.mark.asyncio
async def test_usage_events_are_stored(async_client: AsyncClient):
    response = await async_client.post(
        "/usage-events",
        headers=AUTH,
        json=BATCH,
    )

    assert response.status_code == 200
    assert response.json() == {"received": 3, "inserted": 3}
    assert stored_rows() == [
        ("pixel-1", 1000, "android", "SCREEN_INTERACTIVE", ""),
        ("pixel-1", 1001, "android", "KEYGUARD_HIDDEN", ""),
        ("pixel-1", 1002, "com.example.chat", "ACTIVITY_RESUMED", ""),
    ]

@pytest.mark.asyncio
async def test_resending_overlapping_batch_stores_only_new_events(async_client: AsyncClient):
    await async_client.post("/usage-events", headers=AUTH, json=BATCH)

    overlapping = {
        "device_id": "pixel-1",
        "events": BATCH["events"] + [
            {"timestamp": 1003, "package": "com.example.chat", "event_type": "ACTIVITY_PAUSED"},
        ],
    }
    response = await async_client.post("/usage-events", headers=AUTH, json=overlapping)

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

@pytest.mark.asyncio
async def test_usage_events_reject_everything_when_token_unset(async_client: AsyncClient):
    settings.usage_token = Settings.model_fields["usage_token"].default

    for header in ("Bearer secret", "Bearer "):
        response = await async_client.post("/usage-events", headers={"Authorization": header}, json=BATCH)
        assert response.status_code == 401

@pytest.mark.asyncio
async def test_usage_events_reject_non_ascii_token(async_client: AsyncClient):
    response = await async_client.post(
        "/usage-events",
        headers={"Authorization": "Bearer café".encode("latin-1")},
        json=BATCH,
    )
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_usage_database_exists_after_startup(async_client: AsyncClient):
    assert stored_rows() == []

@pytest.mark.asyncio
async def test_distinct_activities_in_same_millisecond_are_both_stored(async_client: AsyncClient):
    event = {"timestamp": 1000, "package": "com.example.chat", "event_type": "ACTIVITY_PAUSED"}
    batch = {
        "device_id": "pixel-1",
        "events": [{**event, "class_name": "com.example.chat.Main"}, {**event, "class_name": "com.example.chat.Thread"}],
    }

    response = await async_client.post("/usage-events", headers=AUTH, json=batch)

    assert response.json() == {"received": 2, "inserted": 2}

@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [
    {"timestamp": 2**70},
    {"timestamp": -1},
    {"timestamp": "12"},
    {"timestamp": True},
    {"timestamp": 3.0},
    {"package": ""},
    {"package": "x" * (MAX_USAGE_TEXT_LENGTH + 1)},
    {"event_type": ""},
    {"class_name": "x" * (MAX_USAGE_TEXT_LENGTH + 1)},
])
async def test_usage_events_reject_out_of_range_values(async_client: AsyncClient, invalid):
    event = {**BATCH["events"][0], **invalid}

    response = await async_client.post("/usage-events", headers=AUTH, json={"device_id": "pixel-1", "events": [event]})

    assert response.status_code == 422
    assert stored_rows() == []

@pytest.mark.asyncio
async def test_usage_events_reject_empty_device_id(async_client: AsyncClient):
    response = await async_client.post("/usage-events", headers=AUTH, json={**BATCH, "device_id": ""})
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_usage_events_reject_too_many_events(async_client: AsyncClient):
    batch = {"device_id": "pixel-1", "events": [BATCH["events"][0]] * (MAX_USAGE_EVENTS + 1)}

    response = await async_client.post("/usage-events", headers=AUTH, json=batch)

    assert response.status_code == 422
    assert stored_rows() == []

@pytest.mark.asyncio
async def test_oversized_usage_body_is_rejected(async_client: AsyncClient):
    async def chunks():
        yield b" " * MAX_BODY_BYTES
        yield b" "

    response = await async_client.post("/usage-events", headers=AUTH, content=chunks())

    assert response.status_code == 413

@pytest.mark.asyncio
async def test_usage_body_is_not_parsed_before_auth(async_client: AsyncClient):
    response = await async_client.post("/usage-events", headers={**AUTH, "Authorization": "Bearer wrong"}, content=b"not json")
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_usage_events_reject_malformed_json(async_client: AsyncClient):
    response = await async_client.post("/usage-events", headers=AUTH, content=b"not json")
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_usage_events_reject_lone_surrogate(async_client: AsyncClient):
    body = b'{"device_id": "\\ud800", "events": []}'

    response = await async_client.post("/usage-events", headers=AUTH, content=body)

    assert response.status_code == 422

@pytest.mark.asyncio
async def test_device_id_cannot_forge_log_lines(async_client: AsyncClient, caplog):
    await async_client.post("/usage-events", headers=AUTH, json={**BATCH, "device_id": "pixel\nforged"})

    assert not any("\n" in r.getMessage() for r in caplog.records)

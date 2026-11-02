import glob
import hashlib
import os
from datetime import UTC, datetime

import pytest
from httpx import AsyncClient

from app.config import settings

AUTH = {"Authorization": "Bearer test-token"}

# Deliberately not canonical JSON: the spacing, key order and number spelling must survive storage
SYNC_BODY = b"""{ "source":"health_connect", "sequence":42, "timestamp":"2026-11-01T08:00:00Z",
  "heart_rate":[{"uuid":"a1","time":"2026-11-01T07:59:00Z","bpm":61.0}], "new_in_next_release":"\xc5\xbc" }"""

PING_BODY = b'{"test": true, "message": "Test ping from Life Dashboard Companion", "source": "health_connect"}'

@pytest.fixture(autouse=True)
def configure(monkeypatch):
    monkeypatch.setattr(settings, "life_dashboard_token", "test-token")
    monkeypatch.setattr(settings, "usage_token", "usage-token")

def stored_files():
    return glob.glob(os.path.join(settings.health_dir, "*", "*"))

def read_file(path):
    with open(path, "rb") as f:
        return f.read()

@pytest.mark.asyncio
async def test_payload_is_stored_as_received(async_client: AsyncClient):
    day_before = datetime.now(UTC).date().isoformat()
    response = await async_client.post("/life-dashboard", headers=AUTH, content=SYNC_BODY)
    day_after = datetime.now(UTC).date().isoformat()

    assert response.status_code == 204
    [path] = stored_files()
    assert read_file(path) == SYNC_BODY
    day, filename = os.path.split(os.path.relpath(path, settings.health_dir))
    assert day in (day_before, day_after)
    assert filename == f"{hashlib.sha256(SYNC_BODY).hexdigest()}.json"

@pytest.mark.asyncio
async def test_directory_exists_after_startup(async_client: AsyncClient):
    assert os.path.isdir(settings.health_dir)

@pytest.mark.asyncio
async def test_resent_payload_leaves_one_file(async_client: AsyncClient):
    await async_client.post("/life-dashboard", headers=AUTH, content=SYNC_BODY)
    await async_client.post("/life-dashboard", headers=AUTH, content=SYNC_BODY)

    assert len(stored_files()) == 1

@pytest.mark.asyncio
async def test_ping_stores_nothing(async_client: AsyncClient):
    response = await async_client.post("/life-dashboard", headers=AUTH, content=PING_BODY)

    assert response.status_code == 204
    assert stored_files() == []

@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b'{"test": false}', b'{"test": 1}', b'{"test": "false"}'],
                         ids=["false", "number", "string"])
async def test_small_body_with_other_test_value_is_stored(async_client: AsyncClient, body):
    response = await async_client.post("/life-dashboard", headers=AUTH, content=body)

    assert response.status_code == 204
    assert len(stored_files()) == 1

@pytest.mark.asyncio
@pytest.mark.parametrize("headers", [
    {"Authorization": "Bearer wrong-token"},
    {"Authorization": "Bearer usage-token"},
    {"Authorization": "Bearer café".encode("latin-1")},
    {},
], ids=["wrong", "usage-token", "non-ascii", "missing"])
async def test_bad_token_is_rejected(async_client: AsyncClient, headers):
    response = await async_client.post("/life-dashboard", headers=headers, content=SYNC_BODY)

    assert response.status_code == 401
    assert stored_files() == []

@pytest.mark.asyncio
async def test_body_is_not_read_before_auth(async_client: AsyncClient):
    chunks_read = []

    async def body():
        chunks_read.append(SYNC_BODY)
        yield SYNC_BODY

    # FastAPI only parses a body parameter as JSON, so without this header the test would pass on unsafe code too
    headers = {"Authorization": "Bearer wrong-token", "Content-Type": "application/json"}
    response = await async_client.post("/life-dashboard", headers=headers, content=body())

    assert response.status_code == 401
    assert chunks_read == []

@pytest.mark.asyncio
async def test_unset_token_rejects_everything(async_client: AsyncClient, monkeypatch):
    monkeypatch.setattr(settings, "life_dashboard_token", "")

    for header in ("Bearer test-token", "Bearer "):
        response = await async_client.post("/life-dashboard", headers={"Authorization": header}, content=SYNC_BODY)
        assert response.status_code == 401
    assert stored_files() == []

@pytest.mark.asyncio
async def test_life_dashboard_token_is_rejected_on_usage_events(async_client: AsyncClient):
    response = await async_client.post("/usage-events", headers=AUTH, json={"device_id": "pixel-1", "events": []})

    assert response.status_code == 401

@pytest.mark.asyncio
@pytest.mark.parametrize("body", [b"not json", b"[1, 2]", b'"text"', b"\xc3\x28", b""],
                         ids=["malformed", "array", "string", "invalid-utf8", "empty"])
async def test_body_that_is_not_a_payload_is_still_stored(async_client: AsyncClient, body):
    response = await async_client.post("/life-dashboard", headers=AUTH, content=body)

    assert response.status_code == 204
    [path] = stored_files()
    assert read_file(path) == body

@pytest.mark.asyncio
async def test_ten_mib_body_is_stored(async_client: AsyncClient):
    body = b"x" * (10 * 1024 * 1024)

    response = await async_client.post("/life-dashboard", headers=AUTH, content=body)

    assert response.status_code == 204
    [path] = stored_files()
    assert os.path.getsize(path) == len(body)

@pytest.mark.asyncio
async def test_failed_write_leaves_no_temp_file(async_client: AsyncClient, monkeypatch):
    def fail(*args):
        raise OSError("disk full")
    monkeypatch.setattr(os, "replace", fail)

    with pytest.raises(OSError):
        await async_client.post("/life-dashboard", headers=AUTH, content=SYNC_BODY)

    assert stored_files() == []

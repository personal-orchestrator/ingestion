import os
import json
import pytest
import nats
from httpx import AsyncClient

from app.config import settings
from app.main import MAX_BODY_BYTES

@pytest.mark.asyncio
async def test_upload_audio_success(async_client: AsyncClient):
    # Connect separate client to test container to verify publish
    test_nc = await nats.connect(settings.nats_url)
    sub = await test_nc.subscribe(settings.nats_subject)
    
    file_content = b"dummy content"
    filename = "test_audio.m4a"
    
    response = await async_client.post(
        "/upload",
        auth=(settings.auth_username, settings.auth_password),
        files={"file": (filename, file_content, "audio/mp4")}
    )
    
    assert response.status_code == 200
    assert response.json()["filename"] == filename
    
    # Check if saved locally
    saved_file_path = os.path.join(settings.storage_dir, filename)
    assert os.path.exists(saved_file_path)
    with open(saved_file_path, "rb") as f:
        assert f.read() == file_content
        
    # Check NATS message
    msg = await sub.next_msg(timeout=2.0)
    payload = json.loads(msg.data.decode("utf-8"))
    assert payload["filename"] == filename
    
    await test_nc.close()

@pytest.mark.asyncio
async def test_upload_audio_unauthorized(async_client: AsyncClient):
    response = await async_client.post(
        "/upload",
        auth=("wrong", "password"),
        files={"file": ("test.m4a", b"dummy", "audio/mp4")}
    )
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_upload_audio_invalid_format(async_client: AsyncClient):
    response = await async_client.post(
        "/upload",
        auth=(settings.auth_username, settings.auth_password),
        files={"file": ("test.txt", b"dummy", "text/plain")}
    )
    assert response.status_code == 400

@pytest.mark.asyncio
@pytest.mark.parametrize("setting", ["auth_username", "auth_password"])
async def test_upload_audio_rejects_non_ascii_configured_credential(async_client: AsyncClient, monkeypatch, setting):
    monkeypatch.setattr(settings, setting, "zażółć")

    response = await async_client.post(
        "/upload",
        auth=("wrong", "wrong"),
        files={"file": ("test.m4a", b"dummy", "audio/mp4")}
    )
    assert response.status_code == 401

@pytest.mark.asyncio
@pytest.mark.parametrize("filename", ["../evil.m4a", "nested/evil.m4a", "/evil.m4a", "..", "usage_events.db"])
async def test_upload_audio_rejects_unsafe_filename(async_client: AsyncClient, tmp_path, filename):
    response = await async_client.post(
        "/upload",
        auth=(settings.auth_username, settings.auth_password),
        files={"file": (filename, b"dummy", "audio/mp4")}
    )

    assert response.status_code == 400
    assert sorted(os.listdir(tmp_path)) == ["health", "recordings", "usage"]
    assert os.listdir(settings.storage_dir) == []

@pytest.mark.asyncio
async def test_upload_body_is_not_parsed_before_auth(async_client: AsyncClient):
    response = await async_client.post(
        "/upload",
        auth=("wrong", "password"),
        headers={"Content-Type": "multipart/form-data"},
        content=b"no boundary, so this cannot be parsed"
    )
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_upload_rejects_more_than_one_file(async_client: AsyncClient):
    part = ("test.m4a", b"dummy", "audio/mp4")

    response = await async_client.post(
        "/upload",
        auth=(settings.auth_username, settings.auth_password),
        files=[("file", part), ("file", part)]
    )

    assert response.status_code == 400
    assert os.listdir(settings.storage_dir) == []

@pytest.mark.asyncio
async def test_upload_rejects_text_fields(async_client: AsyncClient):
    response = await async_client.post(
        "/upload",
        auth=(settings.auth_username, settings.auth_password),
        data={"file": "not a file"},
        files={"other": ("test.m4a", b"dummy", "audio/mp4")}
    )
    assert response.status_code == 400

@pytest.mark.asyncio
async def test_upload_requires_file_field(async_client: AsyncClient):
    response = await async_client.post(
        "/upload",
        auth=(settings.auth_username, settings.auth_password),
        files={"other": ("test.m4a", b"dummy", "audio/mp4")}
    )
    assert response.status_code == 422

@pytest.mark.asyncio
async def test_oversized_upload_is_rejected(async_client: AsyncClient):
    response = await async_client.post(
        "/upload",
        auth=(settings.auth_username, settings.auth_password),
        files={"file": ("test.m4a", b"x" * (MAX_BODY_BYTES + 1), "audio/mp4")}
    )

    assert response.status_code == 413
    assert os.listdir(settings.storage_dir) == []

@pytest.mark.asyncio
async def test_failed_login_cannot_forge_log_lines(async_client: AsyncClient, caplog):
    await async_client.post(
        "/upload",
        auth=("evil\nforged", "wrong"),
        files={"file": ("test.m4a", b"dummy", "audio/mp4")}
    )

    assert not any("\n" in r.getMessage() for r in caplog.records)

import os
import json
import pytest
import pytest_asyncio
import nats
from httpx import AsyncClient, ASGITransport
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from app.config import settings
from app.main import app

class NatsContainer(DockerContainer):
    def __init__(self, image="nats:latest"):
        super(NatsContainer, self).__init__(image)
        self.with_exposed_ports(4222)
        
    def start(self):
        super().start()
        wait_for_logs(self, "Listening for client connections on 0.0.0.0:4222")
        return self

@pytest.fixture(scope="module")
def nats_server():
    with NatsContainer() as nats:
        yield nats

@pytest.fixture(autouse=True)
def setup_env(nats_server, tmp_path):
    host = nats_server.get_container_host_ip()
    port = nats_server.get_exposed_port(4222)
    settings.nats_url = f"nats://{host}:{port}"
    settings.storage_dir = str(tmp_path)
    yield

@pytest_asyncio.fixture
async def async_client():
    # ASGITransport triggers the lifespan of the app, connecting to our test container
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client

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

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs

from app.config import settings
from app.main import app

class NatsContainer(DockerContainer):
    def __init__(self, image="nats:latest"):
        super(NatsContainer, self).__init__(image)
        self.with_exposed_ports(4222)
        self.with_command("-js")

    def start(self):
        super().start()
        wait_for_logs(self, "Listening for client connections on 0.0.0.0:4222")
        return self

@pytest.fixture(scope="session")
def nats_server():
    with NatsContainer() as nats:
        yield nats

@pytest.fixture(autouse=True)
def setup_env(nats_server, tmp_path, monkeypatch):
    host = nats_server.get_container_host_ip()
    port = nats_server.get_exposed_port(4222)
    monkeypatch.setattr(settings, "nats_url", f"nats://{host}:{port}")
    monkeypatch.setattr(settings, "storage_dir", str(tmp_path / "recordings"))
    monkeypatch.setattr(settings, "usage_dir", str(tmp_path / "usage"))
    monkeypatch.setattr(settings, "health_dir", str(tmp_path / "health"))

@pytest_asyncio.fixture
async def async_client():
    # ASGITransport does not run the lifespan, so enter it here to connect to our test container
    async with app.router.lifespan_context(app):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            yield client

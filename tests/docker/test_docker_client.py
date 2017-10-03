import pytest
from docker.errors import DockerException

from entityd.docker.client import DockerClient


def test_docker_not_available(monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        pytest.MagicMock(side_effect=DockerException))
    assert DockerClient.client_available() == False


@pytest.mark.non_container
def test_docker_is_available():
    assert DockerClient.client_available()


@pytest.mark.non_container
def test_docker_client():
    client = DockerClient.get_client()
    info = client.info()
    assert info

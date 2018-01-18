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


def test_all_containers(monkeypatch):
    get_client = pytest.MagicMock()
    client_instance = get_client.return_value
    client_instance.containers.list.return_value = ["foo", "steve"]
    monkeypatch.setattr(DockerClient, "get_client", get_client)

    all_containers = DockerClient.all_containers()
    assert all_containers == ["foo", "steve"]


def test_all_containers_exception(monkeypatch):
    get_client = pytest.MagicMock()
    client_instance = get_client.return_value
    client_instance.containers.list.side_effect=DockerException
    monkeypatch.setattr(DockerClient, "get_client", get_client)

    all_containers = DockerClient.all_containers()
    assert all_containers == []
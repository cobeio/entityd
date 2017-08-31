import pytest
from docker.errors import DockerException
from mock import patch

from entityd.docker.docker import Client


def test_docker_not_available():
    with patch('entityd.docker.docker.DockerClient') as docker_client:
        docker_client.side_effect = DockerException

        assert Client.client_available() == False


def test_docker_is_available():
    assert Client.client_available()


def test_docker_client():
    client = Client.get_client()
    info = client.info()
    assert info
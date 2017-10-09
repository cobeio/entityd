import pytest

from entityd.docker.client import DockerClient


@pytest.fixture(autouse=True)
def clear_client():
    yield
    DockerClient._client = None
    DockerClient._client_info = None


@pytest.fixture
def running_container():
    network_id = 'aaaa'
    volume_name = 'volume1'
    attrs = {
        "State": {
            "ExitCode": 0,
            "StartedAt": "2017-08-30T10:52:25.439434269Z",
            "Error": "",
            "FinishedAt": "0001-01-01T00:00:00Z",
        },
        'NetworkSettings': {
            'Networks': {
                'non-swarm-net': {
                    'NetworkID': network_id
                }
            },
            'Ports': {'6379/tcp': None},
        },
        'Mounts': [{
            'Destination': '/data',
            'Driver': 'local',
            'Mode': '',
            'Name': volume_name,
            'Propagation': '',
            'RW': True,
            'Source': '/var/lib/docker/volumes/volume1/_data',
            'Type': 'volume'
        }],
    }
    image = pytest.MagicMock(id='image_id', tags=['debian:latest'])
    container = pytest.Mock(
        id="bar",
        name="running_container",
        status="running",
        labels=["label"],
        image=image,
        attrs=attrs,
        should_exist=True,
        network_id=network_id,        
        volume_name=volume_name)
    container.top.return_value = {
        "Titles": ["PID"],
        "Processes": [
            ['0'], ['1'], ['3']
        ]
    }

    return container


@pytest.fixture()
def finished_container():
    network_id = 'aaaa'
    volume_name = 'volume1'
    attrs = {
        "State": {
            "ExitCode": 21,
            "StartedAt": "2017-08-30T10:52:25.439434269Z",
            "Error": "",
            "FinishedAt": "2017-08-30T10:55:25.439434269Z",
        },
        'NetworkSettings': {
            'Networks': {
                'non-swarm-net': {
                    'NetworkID': network_id
                }
            },
            'Ports': {'6379/tcp': None},
        },
        'Mounts': [{
            'Destination': '/data',
            'Driver': 'local',
            'Mode': '',
            'Name': volume_name,
            'Propagation': '',
            'RW': True,
            'Source': '/var/lib/docker/volumes/volume1/_data',
            'Type': 'volume',
        }],
    }
    image = pytest.MagicMock(id='image_id', tags=['debian:latest'])
    container = pytest.Mock(
        id="bar",
        name="finished_container",
        status="exited",
        labels=["label"],
        image=image,
        attrs=attrs,
        network_id=network_id,
        volume_name=volume_name,
    )
    container.configure_mock(name="finished_container", should_exist=True)

    return container


@pytest.fixture
def docker_client(monkeypatch):

    def make_docker_client(client_info=None, containers=None, volumes=None):
        get_client = pytest.MagicMock()
        client_instance = get_client.return_value
        client_instance.info.return_value = client_info or {'ID': 'foo'}
        client_instance.containers.list.return_value = iter(containers or [])
        client_instance.volumes.list.return_value = iter(volumes or [])

        monkeypatch.setattr(DockerClient, "get_client", get_client)

    return make_docker_client

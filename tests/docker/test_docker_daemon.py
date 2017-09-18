import pytest
from docker.errors import DockerException
from mock import patch, MagicMock

from entityd.docker.docker import DockerDaemon, Client


@pytest.fixture
def docker_daemon(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    dd = DockerDaemon()
    pm.register(dd, 'entityd.docker.docker.DockerDaemon')
    return dd


def test_docker_not_available():
    with patch('entityd.docker.docker.DockerClient') as docker_client:
        docker_client.side_effect = DockerException
        docker_daemon = DockerDaemon()

        assert not list(docker_daemon.entityd_find_entity(docker_daemon.name))


def test_attrs_raises_exception():
    with pytest.raises(LookupError):
        docker_daemon = DockerDaemon()
        docker_daemon.entityd_find_entity(DockerDaemon.name, attrs="foo")


def test_not_provided():
    docker_daemon = DockerDaemon()
    assert docker_daemon.entityd_find_entity('foo') is None


def test_get_ueid():
    ueid = DockerDaemon.get_ueid("foo")
    assert ueid


def test_find_entities(monkeypatch, session, docker_daemon):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Containers': 6,
        'ContainersPaused': 1,
        'ContainersRunning': 3,
        'ContainersStopped': 2,
    }

    get_client = MagicMock()
    client_instance = get_client.return_value
    client_instance.info.return_value = client_info
    monkeypatch.setattr(Client, "get_client", get_client)

    docker_daemon.entityd_sessionstart(session)
    docker_daemon.entityd_configure(session.config)
    entities = docker_daemon.entityd_find_entity(DockerDaemon.name)
    entities = list(entities)
    assert len(entities) == 1

    entity = entities[0]
    assert entity.exists == True
    assert entity.attrs.get('id').value == client_info['ID']
    assert entity.attrs.get('id').traits == {"entity:id"}
    assert (entity.attrs.get('containers:total').value ==
            client_info['Containers'])
    assert (entity.attrs.get('containers:paused').value ==
            client_info['ContainersPaused'])
    assert (entity.attrs.get('containers:running').value ==
            client_info['ContainersRunning'])
    assert (entity.attrs.get('containers:stopped').value ==
            client_info['ContainersStopped'])

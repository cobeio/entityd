import pytest
from docker.errors import DockerException
from mock import patch, MagicMock

from entityd.docker.client import DockerClient
from entityd.docker.engine import DockerEngine


@pytest.fixture
def docker_engine(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    dd = DockerEngine()
    pm.register(dd, 'entityd.docker.daemon.DockerEngine')
    return dd


def test_docker_not_available():
    with patch('entityd.docker.client.docker.DockerClient') as docker_client:
        docker_client.side_effect = DockerException
        docker_engine = DockerEngine()

        assert not list(docker_engine.entityd_find_entity(docker_engine.name))


def test_attrs_raises_exception():
    with pytest.raises(LookupError):
        docker_engine = DockerEngine()
        docker_engine.entityd_find_entity(DockerEngine.name, attrs="foo")


def test_not_provided():
    docker_engine = DockerEngine()
    assert docker_engine.entityd_find_entity('foo') is None


def test_get_ueid():
    ueid = DockerEngine.get_ueid("foo")
    assert ueid


def test_find_entities(monkeypatch, session, docker_engine):
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
    monkeypatch.setattr(DockerClient, "get_client", get_client)

    docker_engine.entityd_sessionstart(session)
    docker_engine.entityd_configure(session.config)
    entities = docker_engine.entityd_find_entity(DockerEngine.name)
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

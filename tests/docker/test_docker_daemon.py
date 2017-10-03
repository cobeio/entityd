import pytest
from docker.errors import DockerException

from entityd.docker.client import DockerClient
from entityd.docker.daemon import DockerDaemon
from entityd.docker.swarm import DockerNode


@pytest.fixture
def docker_daemon(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    dd = DockerDaemon()
    pm.register(dd, 'entityd.docker.daemon.DockerDaemon')
    return dd


def test_docker_not_available(monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        pytest.MagicMock(side_effect=DockerException))
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


def test_find_entities_no_swarm(monkeypatch, session, docker_daemon):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Containers': 6,
        'ContainersPaused': 1,
        'ContainersRunning': 3,
        'ContainersStopped': 2,
        'Swarm': {
            'LocalNodeState': 'inactive',
        }
    }

    get_client = pytest.MagicMock()
    client_instance = get_client.return_value
    client_instance.info.return_value = client_info
    monkeypatch.setattr(DockerClient, "get_client", get_client)

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


def test_find_entities_with_swarm(monkeypatch, session, docker_daemon):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Containers': 6,
        'ContainersPaused': 1,
        'ContainersRunning': 3,
        'ContainersStopped': 2,
        'Swarm': {
            'LocalNodeState': 'active',
            'NodeID': 'aaaa',
        },
    }

    get_client = pytest.MagicMock()
    client_instance = get_client.return_value
    client_instance.info.return_value = client_info
    monkeypatch.setattr(DockerClient, "get_client", get_client)

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

    node_ueid = DockerNode.get_ueid(client_info['Swarm']['NodeID'])
    assert node_ueid in entity.parents



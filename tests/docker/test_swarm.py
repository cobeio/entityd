import pytest
from docker.errors import DockerException
from mock import patch, MagicMock

from entityd.docker.client import DockerClient
from entityd.docker.swarm import DockerSwarm


@pytest.fixture
def docker_swarm(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    swarm = DockerSwarm()
    pm.register(swarm, 'entityd.docker.swarm.DockerSwarm')
    return swarm


def test_docker_not_available():
    with patch('entityd.docker.client.docker.DockerClient') as docker_client:
        docker_client.side_effect = DockerException
        docker_swarm = DockerSwarm()

        assert not list(docker_swarm.entityd_find_entity(docker_swarm.name))


def test_attrs_raises_exception():
    with pytest.raises(LookupError):
        docker_swarm = DockerSwarm()
        docker_swarm.entityd_find_entity(DockerSwarm.name, attrs="foo")


def test_not_provided():
    docker_swarm = DockerSwarm()
    assert docker_swarm.entityd_find_entity('foo') is None


def test_get_ueid():
    ueid = DockerSwarm.get_ueid("foo")
    assert ueid


def test_non_swarm_docker(monkeypatch, session, docker_swarm):
    swarm = {
        'ControlAvailable': False,
        'Error': '',
        'LocalNodeState': 'inactive',
        'NodeAddr': '',
        'NodeID': '',
        'RemoteManagers': None,
    }

    client_info = {'Swarm': swarm}

    get_client = MagicMock()
    client_instance = get_client.return_value
    client_instance.info.return_value = client_info
    monkeypatch.setattr(DockerClient, "get_client", get_client)

    docker_swarm.entityd_configure(session.config)
    entities = docker_swarm.entityd_find_entity(docker_swarm.name)
    entities = list(entities)
    assert len(entities) == 0


def test_find_entities(monkeypatch, session, docker_swarm):
    raft = {
        'ElectionTick': 3,
        'HeartbeatTick': 1,
        'KeepOldSnapshots': 0,
        'LogEntriesForSlowFollowers': 500,
        'SnapshotInterval': 10000,
    }

    spec = {
        'EncryptionConfig': {'AutoLockManagers': False},
        'Labels': {},
        'Name': 'default',
        'Raft': raft,
    }

    cluster = {
        'CreatedAt': '2017-09-18T11:02:01.903734295Z',
        'ID': 'v1w5dux11fec5252r3hciqgzp',
        'Spec': spec,
    }

    swarm = {
        'ID': 'w5rbcl1ff2tx3exd6tbvq9em9',
        'Cluster': cluster,
        'ControlAvailable': True,
        'Error': '',
        'LocalNodeState': 'active',
        'Managers': 1,
        'Nodes': 1,
    }

    client_info = {'Swarm': swarm}

    get_client = MagicMock()
    client_instance = get_client.return_value
    client_instance.info.return_value = client_info
    monkeypatch.setattr(DockerClient, "get_client", get_client)

    docker_swarm.entityd_configure(session.config)
    entities = docker_swarm.entityd_find_entity(docker_swarm.name)
    entities = list(entities)
    assert len(entities) == 1

    entity = entities[0]
    assert entity.exists == True
    assert entity.label == "w5rbcl1ff2"
    assert entity.attrs.get('id').value == "w5rbcl1ff2tx3exd6tbvq9em9"
    assert entity.attrs.get('id').traits == {'entity:id'}

    assert (entity.attrs.get(
        'control-available').value == swarm['ControlAvailable'])

    assert entity.attrs.get('error').value == swarm['Error']

    assert entity.attrs.get('nodes:total').value == swarm['Nodes']
    assert entity.attrs.get('nodes:managers').value == swarm['Managers']
    assert entity.attrs.get('nodes:total').traits == {'metric:gauge'}
    assert entity.attrs.get('nodes:managers').traits == {'metric:gauge'}

    assert entity.attrs.get('name').value == spec['Name']
    assert (entity.attrs.get('auto-lock-managers').value ==
            spec['EncryptionConfig']['AutoLockManagers'])
    assert (entity.attrs.get(
        'raft:election-tick').value == raft['ElectionTick'])
    assert (entity.attrs.get(
        'raft:heartbeat-tick').value == raft['HeartbeatTick'])
    assert (entity.attrs.get(
        'raft:keep-old-snapshots').value == raft['KeepOldSnapshots'])
    assert (entity.attrs.get(
        'raft:log-entries-for-slow-followers').value ==
        raft['LogEntriesForSlowFollowers'])
    assert (entity.attrs.get(
        'raft:snapshot-interval').value == raft['SnapshotInterval'])

    assert entity.attrs.get('name').traits == set()
    assert entity.attrs.get('auto-lock-managers').traits == set()
    assert entity.attrs.get('raft:election-tick').traits == set()
    assert entity.attrs.get('raft:heartbeat-tick').traits == set()
    assert entity.attrs.get('raft:keep-old-snapshots').traits == set()
    assert (entity.attrs.get(
        'raft:log-entries-for-slow-followers').traits == set())
    assert entity.attrs.get('raft:snapshot-interval').traits == set()

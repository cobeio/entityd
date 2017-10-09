import pytest
from docker.errors import DockerException

import entityd
from entityd.docker.client import DockerClient
from entityd.docker.swarm import DockerNetwork


@pytest.fixture
def docker_network(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    network = DockerNetwork()
    pm.register(network, 'entityd.docker.swarm.DockerNetwork')
    return network


@pytest.fixture
def swarm_network():
    network = pytest.MagicMock()
    network.id = 'aaaa'
    network.attrs = {
        'Containers': None,
        'Driver': 'overlay',
        'EnableIPv6': False,
        'Id': network.id,
        'Ingress': False,
        'Internal': False,
        'Labels': {
            'foo': 'bar'},
        'Name': 'swarm-network',
        'Options': {
            'com.docker.network.driver.overlay.vxlanid_list': '4097'},
        'Scope': 'swarm'}

    return network


@pytest.fixture
def local_network():
    network = pytest.MagicMock()
    network.id = 'bbbb'
    network.attrs = {
        'Containers': {},
        'Driver': 'bridge',
        'EnableIPv6': False,
        'Id': network.id,
        'Ingress': False,
        'Internal': False,
        'Labels': {
            'monty': 'python'},
        'Name': 'local-network',
        'Options': {},
        'Scope': 'local'}

    return network


@pytest.fixture
def networks(monkeypatch):
    def make_client(client_info, networks):
        get_client = pytest.MagicMock()
        client_instance = get_client.return_value
        client_instance.info.return_value = client_info
        client_instance.networks.list.return_value = iter(networks)
        monkeypatch.setattr(DockerClient, "get_client", get_client)

    return make_client


def test_docker_not_available(monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        pytest.MagicMock(side_effect=DockerException))
    docker_network = DockerNetwork()

    assert not list(docker_network.entityd_find_entity(docker_network.name))


def test_attrs_raises_exception():
    with pytest.raises(LookupError):
        docker_network = DockerNetwork()
        docker_network.entityd_find_entity(DockerNetwork.name, attrs="foo")


def test_not_provided():
    docker_network = DockerNetwork()
    assert docker_network.entityd_find_entity('foo') is None


def test_get_ueid():
    ueid = DockerNetwork.get_ueid("foo")
    assert ueid


def test_find_entities_no_swarm(session, docker_network,
                                local_network, networks):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'inactive',
            'NodeID': '',
        },
    }

    testing_networks = [local_network]
    networks(client_info, testing_networks)

    daemon_ueid = entityd.docker.get_ueid(
        'DockerDaemon', client_info['ID'])

    docker_network.entityd_configure(session.config)
    entities = docker_network.entityd_find_entity(DockerNetwork.name)
    entities = list(entities)
    assert len(entities) == len(testing_networks)

    for entity, network in zip(entities, testing_networks):
        assert entity.label == network.attrs['Name']
        assert entity.attrs.get('id').value == network.attrs['Id']
        assert entity.attrs.get('id').traits == {'entity:id'}
        assert entity.attrs.get('labels').value == network.attrs['Labels']
        assert entity.attrs.get('options').value == network.attrs['Options']
        assert entity.attrs.get('driver').value == network.attrs['Driver']
        assert entity.attrs.get('ipv6-enabled').value == network.attrs['EnableIPv6']
        assert entity.attrs.get('ingress').value == network.attrs['Ingress']
        assert entity.attrs.get('internal').value == network.attrs['Internal']
        assert entity.attrs.get('scope').value == network.attrs['Scope']
        assert daemon_ueid in entity.parents


def test_find_entities_swarm_manager(session, docker_network, swarm_network,
                                     local_network, networks):
    cluster = {
        'ID': 'v1w5dux11fec5252r3hciqgzp',
    }

    swarm = {
        'Cluster': cluster,
        'ControlAvailable': True,
        'Error': '',
        'LocalNodeState': 'active',
        'Managers': 1,
        'Nodes': 1,
        'NodeID': 'aaaa',
        'RemoteManagers': [{'NodeID': 'aaaa'}],
    }

    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': swarm,
    }

    testing_networks = [swarm_network, local_network]
    networks(client_info, testing_networks)

    swarm_ueid = entityd.docker.get_ueid(
        'DockerSwarm', cluster['ID'])
    daemon_ueid = entityd.docker.get_ueid(
        'DockerDaemon', client_info['ID'])

    docker_network.entityd_configure(session.config)
    entities = docker_network.entityd_find_entity(DockerNetwork.name)
    entities = list(entities)
    assert len(entities) == 2

    for entity, network in zip(entities, testing_networks) :
        assert entity.label == network.attrs['Name']
        assert entity.attrs.get('id').value == network.attrs['Id']
        assert entity.attrs.get('id').traits == {'entity:id'}
        assert entity.attrs.get('labels').value == network.attrs['Labels']
        assert entity.attrs.get('options').value == network.attrs['Options']
        assert entity.attrs.get('driver').value == network.attrs['Driver']
        assert entity.attrs.get('ipv6-enabled').value == network.attrs['EnableIPv6']
        assert entity.attrs.get('ingress').value == network.attrs['Ingress']
        assert entity.attrs.get('internal').value == network.attrs['Internal']
        scope = network.attrs['Scope']
        assert entity.attrs.get('scope').value == scope

        if scope == "local":
            assert daemon_ueid in entity.parents
        elif scope == "swarm":
            assert swarm_ueid in entity.parents


def test_find_entities_swarm_worker(session, docker_network, swarm_network,
                                    local_network, networks):
    cluster = {
        'ID': 'v1w5dux11fec5252r3hciqgzp',
    }

    swarm = {
        'Cluster': cluster,
        'ControlAvailable': False,
        'Error': '',
        'LocalNodeState': 'active',
        'NodeID': 'aaaa',
        'RemoteManagers': [{'NodeID': 'aaaa'}],
    }

    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': swarm,
    }

    testing_networks = [swarm_network, local_network]
    networks(client_info, testing_networks)

    swarm_ueid = entityd.docker.get_ueid(
        'DockerSwarm', cluster['ID'])
    daemon_ueid = entityd.docker.get_ueid(
        'DockerDaemon', client_info['ID'])

    docker_network.entityd_configure(session.config)
    entities = docker_network.entityd_find_entity(DockerNetwork.name)
    entities = list(entities)
    assert len(entities) == 2

    for entity, network in zip(entities, testing_networks) :
        assert entity.label == network.attrs['Name']
        assert entity.attrs.get('id').value == network.attrs['Id']
        assert entity.attrs.get('id').traits == {'entity:id'}
        assert entity.attrs.get('labels').value == network.attrs['Labels']
        assert entity.attrs.get('options').value == network.attrs['Options']
        assert entity.attrs.get('driver').value == network.attrs['Driver']
        assert entity.attrs.get('ipv6-enabled').value == network.attrs['EnableIPv6']
        assert entity.attrs.get('ingress').value == network.attrs['Ingress']
        assert entity.attrs.get('internal').value == network.attrs['Internal']
        scope = network.attrs['Scope']
        assert entity.attrs.get('scope').value == scope

        assert entity.attrs.get('labels').traits == set()
        assert entity.attrs.get('options').traits == set()
        assert entity.attrs.get('driver').traits == set()
        assert entity.attrs.get('ipv6-enabled').traits == set()
        assert entity.attrs.get('ingress').traits == set()
        assert entity.attrs.get('internal').traits == set()
        assert entity.attrs.get('scope').traits == set()

        if scope == "local":
            assert daemon_ueid in entity.parents
        elif scope == "swarm":
            assert swarm_ueid in entity.parents

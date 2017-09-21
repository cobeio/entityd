import pytest
from docker.errors import DockerException
from mock import MagicMock

from entityd.docker.client import DockerClient
from entityd.docker.swarm import DockerNode


@pytest.fixture
def docker_node(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    node = DockerNode()
    pm.register(node, 'entityd.docker.swarm.DockerNode')
    return node


@pytest.fixture
def manager_node():
    node = MagicMock()
    node.ip_address = '192.168.2.23'
    node.attrs = {
        'ID': 'aaaa',
        'Description': {'Hostname': 'bbbb'},
        'ManagerStatus': {
            'Addr': '192.168.2.23:2377',
            'Leader': True,
            'Reachability': 'reachable'},
        'Spec': {
            'Availability': 'active',
            'Labels': {'foo': 'bar'},
            'Role': 'manager'},
        'Status': {
            'Addr': node.ip_address,
            'State': 'ready'},
        'Version': {
            'Index': 9}
    }
    return node


@pytest.fixture
def worker_node():
    node = MagicMock()
    node.ip_address = '192.168.2.24'
    node.attrs = {
        'ID': 'cccc',
        'Description': {'Hostname': 'dddd'},
        'ManagerStatus': {
            'Addr': '192.168.2.23:2377',
            'Leader': True,
            'Reachability': 'reachable'},
        'Spec': {
            'Availability': 'active',
            'Labels': {'foo': 'bar'},
            'Role': 'worker'},
        'Status': {
            'Addr': node.ip_address,
            'State': 'ready'},
        'Version': {
            'Index': 9}
    }
    return node


@pytest.fixture
def inactive_node():
    node = MagicMock()
    node.attrs = {
        'ID': 'eeee',
        'Description': {'Hostname': 'ffff'},
        'ManagerStatus': {
            'Addr': '192.168.2.23:2377',
            'Leader': True,
            'Reachability': 'reachable'},
        'Spec': {
            'Availability': 'inactive',
            'Labels': {'foo': 'bar'},
            'Role': 'worker'},
        'Status': {
            'Addr': '192.168.2.25',
            'State': 'ready'},
        'Version': {
            'Index': 9}
    }
    return node


@pytest.fixture
def nodes(monkeypatch):
    def make_client(client_info, nodes):
        get_client = MagicMock()
        client_instance = get_client.return_value
        client_instance.info.return_value = client_info
        client_instance.nodes.list.return_value = iter(nodes)
        monkeypatch.setattr(DockerClient, "get_client", get_client)

    return make_client


def test_docker_not_available(monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        MagicMock(side_effect=DockerException))
    docker_node = DockerNode()

    assert not list(docker_node.entityd_find_entity(docker_node.name))


def test_attrs_raises_exception():
    with pytest.raises(LookupError):
        docker_daemon = DockerNode()
        docker_daemon.entityd_find_entity(DockerNode.name, attrs="foo")


def test_not_provided():
    docker_daemon = DockerNode()
    assert docker_daemon.entityd_find_entity('foo') is None


def test_get_ueid():
    ueid = DockerNode.get_ueid("foo")
    assert ueid


def test_find_entities_no_swarm(session, docker_node, nodes):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'inactive',
            'NodeID': '',
        },
    }

    testing_nodes = []
    nodes(client_info, testing_nodes)

    docker_node.entityd_configure(session.config)
    entities = docker_node.entityd_find_entity(DockerNode.name)
    entities = list(entities)
    assert len(entities) == 0


def test_find_entities_with_swarm(session, docker_node, manager_node, nodes,
                                  worker_node, inactive_node):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'active',
            'NodeID': 'aaaa',
        },
    }

    testing_nodes = [manager_node, worker_node, inactive_node]
    nodes(client_info, testing_nodes)

    docker_node.entityd_configure(session.config)
    entities = docker_node.entityd_find_entity(DockerNode.name)
    entities = list(entities)
    assert len(entities) == 3

    for entity, node in zip(entities, testing_nodes) :
        manager_attrs = node.attrs['ManagerStatus']
        assert (entity.attrs.get(
            'node:id').value == node.attrs['ID'])
        assert (entity.attrs.get(
            'node:role').value == node.attrs['Spec']['Role'])
        assert (entity.attrs.get('node:availability').value ==
                node.attrs['Spec']['Availability'])
        assert (entity.attrs.get(
            'node:labels').value == node.attrs['Spec']['Labels'])
        assert (entity.attrs.get(
            'node:state').value == node.attrs['Status']['State'])
        assert (entity.attrs.get(
            'node:address').value == node.attrs['Status']['Addr'])
        assert (entity.attrs.get(
            'node:version').value == node.attrs['Version']['Index'])
        assert (entity.attrs.get(
            'node:manager:reachability').value == manager_attrs['Reachability'])
        assert (entity.attrs.get(
            'node:manager:leader').value == manager_attrs['Leader'])
        assert (entity.attrs.get(
            'node:manager:addr').value == manager_attrs['Addr'])
        assert entity.attrs.get('node:id').traits == set()
        assert entity.attrs.get('node:role').traits == set()
        assert entity.attrs.get('node:availability').traits == set()
        assert entity.attrs.get('node:labels').traits == set()
        assert entity.attrs.get('node:state').traits == set()
        assert entity.attrs.get('node:address').traits == set()
        assert entity.attrs.get('node:version').traits == set()
        assert entity.attrs.get('node:manager:reachability').traits == set()
        assert entity.attrs.get('node:manager:leader').traits == set()
        assert entity.attrs.get('node:manager:addr').traits == set()

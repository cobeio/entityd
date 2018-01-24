import pytest
from docker.errors import DockerException

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
    node = pytest.MagicMock()
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
    node = pytest.MagicMock()
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
    node = pytest.MagicMock()
    node.attrs = {
        'ID': 'eeee',
        'Description': {'Hostname': 'ffff'},
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
def manager_node_no_labels():
    node = pytest.MagicMock()
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
            'Role': 'manager'},
        'Status': {
            'Addr': node.ip_address,
            'State': 'ready'},
        'Version': {
            'Index': 9}
    }
    return node


@pytest.fixture
def nodes(monkeypatch):
    def make_client(client_info, nodes):
        get_client = pytest.MagicMock()
        client_instance = get_client.return_value
        client_instance.info.return_value = client_info
        client_instance.nodes.list.return_value = iter(nodes)
        monkeypatch.setattr(DockerClient, "get_client", get_client)

    return make_client


def test_docker_not_available(monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        pytest.MagicMock(side_effect=DockerException))
    docker_node = DockerNode()

    assert not list(docker_node.entityd_emit_entities())


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

    entities = list(docker_node.entityd_emit_entities())
    assert len(entities) == 0


def test_find_entities_with_swarm(session, docker_node, manager_node, nodes,
                                  worker_node, inactive_node):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'active',
            'NodeID': 'aaaa',
            'RemoteManagers': [{'NodeID': 'aaaa'}],
        },
    }

    testing_nodes = [manager_node, worker_node, inactive_node]
    nodes(client_info, testing_nodes)

    entities = list(docker_node.entityd_emit_entities())
    node_entities = [x for x in entities if x.metype == DockerNode.name]
    assert len(node_entities) == 3

    for entity, node in zip(node_entities, testing_nodes) :
        assert (entity.attrs.get('id').value == node.attrs['ID'])
        assert (entity.attrs.get('role').value == node.attrs['Spec']['Role'])
        assert (entity.attrs.get(
            'availability').value == node.attrs['Spec']['Availability'])
        assert (entity.attrs.get(
            'state').value == node.attrs['Status']['State'])
        assert (entity.attrs.get(
            'address').value == node.attrs['Status']['Addr'])
        assert (entity.attrs.get(
            'version').value == node.attrs['Version']['Index'])

        if 'ManagerStatus' in node.attrs:
            manager_attrs = node.attrs['ManagerStatus']
            assert (entity.attrs.get(
                'manager:reachability').value == manager_attrs['Reachability'])
            assert (entity.attrs.get(
                'manager:leader').value == manager_attrs['Leader'])
            assert (entity.attrs.get(
                'manager:address').value == manager_attrs['Addr'])
        else:
            assert entity.attrs.get('manager:reachability').value is None
            assert entity.attrs.get('manager:leader').value is None
            assert entity.attrs.get('manager:address').value is None

        assert entity.attrs.get('id').traits == {'entity:id'}
        assert entity.attrs.get('role').traits == set()
        assert entity.attrs.get('availability').traits == set()
        assert entity.attrs.get('state').traits == set()
        assert entity.attrs.get('address').traits == set()
        assert entity.attrs.get('version').traits == set()
        assert entity.attrs.get('manager:reachability').traits == set()
        assert entity.attrs.get('manager:leader').traits == set()
        assert entity.attrs.get('manager:address').traits == set()

    group_entities = [x for x in entities if x.metype == 'Group']
    assert len(group_entities) == 3

    for entity in group_entities:
        assert entity.attrs.get('kind').value == 'label:foo'
        assert entity.attrs.get('id').value == 'bar'
        assert entity.attrs.get('kind').traits == {'entity:id'}
        assert entity.attrs.get('id').traits == {'entity:id'}


def test_find_entities_missing_labels(session,
                                      docker_node,
                                      manager_node_no_labels,
                                      nodes,
                                      worker_node,
                                      inactive_node):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'active',
            'NodeID': 'aaaa',
            'RemoteManagers': [{'NodeID': 'aaaa'}],
        },
    }

    testing_nodes = [manager_node_no_labels, worker_node, inactive_node]
    nodes(client_info, testing_nodes)
    assert list(docker_node.entityd_emit_entities())
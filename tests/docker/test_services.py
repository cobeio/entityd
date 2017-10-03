import pytest
from docker.errors import DockerException
from mock import MagicMock

import entityd
from entityd.docker.client import DockerClient
from entityd.docker.swarm import DockerService


@pytest.fixture
def docker_service(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    service = DockerService()
    pm.register(service, 'entityd.docker.swarm.DockerService')
    return service


@pytest.fixture
def replicated_service():
    service = MagicMock()
    service.container_id1 = 'container_id1'
    service.container_id2 = 'container_id2'
    service.container_ueid1 = entityd.docker.get_ueid(
        'DockerContainer', service.container_id1)
    service.container_ueid2 = entityd.docker.get_ueid(
        'DockerContainer', service.container_id2)
    service.children = [service.container_ueid1, service.container_ueid2]

    service.attrs = {
        'ID': 'aaaaaa',
        'Spec': {
            'EndpointSpec': {'Mode': 'vip'},
            'Labels': {'label1': 'value1'},
            'Mode': {'Replicated': {'Replicas': 3}},
            'Name': 'replicated-service',
            'TaskTemplate': {
                'Networks': [
                    {'Aliases': ['node'], 'Target': 'bbbbbb'}],
            }
        },
        'Version': {'Index': 56}
    }
    service.tasks.return_value = [{
        'DesiredState': 'running',
        'ID': 'task1',
        'NodeID': 'node1',
        'ServiceID': 'service2',
        'Status': {
            'ContainerStatus': {
                'ContainerID': service.container_id1},
            'Message': 'started',
            'State': 'running'},
        'Version': {'Index': 86}
    },{
        'DesiredState': 'running',
        'ID': 'task2',
        'NodeID': 'node1',
        'ServiceID': 'service2',
        'Status': {
            'ContainerStatus': {
                'ContainerID': service.container_id2},
            'Message': 'started',
            'State': 'running'},
        'Version': {'Index': 86}
    },{
        'DesiredState': 'running',
        'ID': 'task3',
        'NodeID': 'node2',
        'ServiceID': 'service2',
        'Status': {
            'ContainerStatus': {},
            'Message': 'preparing',
            'State': 'preparing'},
        'Version': {'Index': 86}
    }]
    service.running_containers = 2

    return service


@pytest.fixture
def global_service():
    service = MagicMock()
    service.container_id1 = 'container_id1'
    service.container_id2 = 'container_id2'
    service.container_ueid1 = entityd.docker.get_ueid(
        'DockerContainer', service.container_id1)
    service.container_ueid2 = entityd.docker.get_ueid(
        'DockerContainer', service.container_id2)
    service.children = [service.container_ueid1, service.container_ueid2]

    service.attrs = {
        'ID': 'service2',
        'Spec': {
            'EndpointSpec': {'Mode': 'vip'},
            'Labels': {'label1': 'value1'},
            'Mode': {'Global': {}},
            'Name': 'global-service',
            'TaskTemplate': {
                'Networks': [
                    {'Aliases': ['node'], 'Target': 'bbbbbb'}],
            }
        },
        'Version': {'Index': 56}
    }
    service.tasks.return_value = [{
        'DesiredState': 'running',
        'ID': 'task1',
        'NodeID': 'node1',
        'ServiceID': 'service2',
        'Status': {
            'ContainerStatus': {
                'ContainerID': service.container_id1},
            'Message': 'started',
            'State': 'running'},
        'Version': {'Index': 86}
    },{
        'DesiredState': 'running',
        'ID': 'task2',
        'NodeID': 'node2',
        'ServiceID': 'service2',
        'Status': {
            'ContainerStatus': {
                'ContainerID': service.container_id2},
            'Message': 'started',
            'State': 'running'},
        'Version': {'Index': 86}
    }]
    service.running_containers = 2

    return service


@pytest.fixture
def services(monkeypatch):
    def make_client(client_info, services):
        get_client = MagicMock()
        client_instance = get_client.return_value
        client_instance.info.return_value = client_info
        client_instance.services.list.return_value = iter(services)
        monkeypatch.setattr(DockerClient, "get_client", get_client)

    return make_client


def test_docker_not_available(monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        MagicMock(side_effect=DockerException))
    docker_service = DockerService()

    assert not list(docker_service.entityd_find_entity(docker_service.name))


def test_attrs_raises_exception():
    with pytest.raises(LookupError):
        docker_service = DockerService()
        docker_service.entityd_find_entity(DockerService.name, attrs="foo")


def test_not_provided():
    docker_service = DockerService()
    assert docker_service.entityd_find_entity('foo') is None


def test_get_ueid():
    ueid = DockerService.get_ueid("foo")
    assert ueid


def test_find_entities_no_swarm(session, docker_service, services):
    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'inactive',
            'NodeID': '',
        },
    }

    testing_services = []
    services(client_info, testing_services)

    docker_service.entityd_configure(session.config)
    entities = docker_service.entityd_find_entity(DockerService.name)
    entities = list(entities)
    assert len(entities) == 0


def test_find_entities_with_swarm(session, docker_service, services,
                                  global_service, replicated_service):
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
    }

    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': swarm,
    }

    testing_services = [global_service, replicated_service]
    services(client_info, testing_services)

    swarm_ueid = entityd.docker.get_ueid(
        'DockerSwarm', cluster['ID'])

    network_ueid = entityd.docker.get_ueid(
        'DockerNetwork', replicated_service.attrs['Spec']['TaskTemplate']['Networks'][0]['Target'])

    docker_service.entityd_configure(session.config)
    entities = docker_service.entityd_find_entity(DockerService.name)
    entities = list(entities)
    assert len(entities) == 2

    for entity, service in zip(entities, testing_services) :
        assert entity.attrs.get('id').value == service.attrs['ID']
        assert entity.attrs.get('labels').value == service.attrs['Spec']['Labels']

        mode_attrs = service['Spec']['Mode']
        if "Replicated" in mode_attrs:
            assert entity.attrs.get('mode').value == 'replicated'
            assert entity.attrs.get('desired-replicas').value == mode_attrs['Replicated']['Replicas']

            assert entity.attrs.get('mode').traits == set()
            assert entity.attrs.get('desired-replicas').traits == set()
        elif "Global" in mode_attrs:
            assert entity.attrs.get('mode').value == 'global'
            assert entity.attrs.get('mode').traits == set()

        assert entity.attrs.get('running-containers').value == service.running_containers

        assert entity.attrs.get('id').traits == {'entity:id'}
        assert entity.attrs.get('labels').traits == set()
        assert entity.attrs.get('running-containers').traits == set()

        assert len(entity.children) == service.running_containers
        for child in service.children:
            assert child in entity.children

        assert len(entity.parents) == 2
        assert swarm_ueid in entity.parents
        assert network_ueid in entity.parents

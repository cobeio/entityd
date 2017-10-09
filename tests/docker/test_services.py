import pytest
from docker.errors import DockerException

import entityd
from entityd.docker.client import DockerClient
from entityd.docker.swarm import DockerService


@pytest.fixture
def docker_service(pm, session, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    service = DockerService()
    pm.register(service, 'entityd.docker.swarm.DockerService')
    return service


@pytest.fixture
def replicated_service():
    service = pytest.MagicMock()
    service.daemon_id = 'foo'
    service.container_id1 = 'container_id1'
    service.container_id2 = 'container_id2'
    service.container_ueid1 = entityd.docker.get_ueid(
        'DockerContainer', service.container_id1)
    service.container_ueid2 = entityd.docker.get_ueid(
        'DockerContainer', service.container_id2)

    service.network_id = 'bbbbbb'
    service.network_ueid = entityd.docker.get_ueid(
        'DockerNetwork', service.network_id)

    service.volume_name = 'bill'
    service.volume_ueid = entityd.docker.get_ueid(
        'DockerVolume', service.daemon_id, service.volume_name)

    service.mount_target = "/" + service.volume_name
    service.mount_ueid1 = entityd.docker.get_ueid(
        'DockerVolumeMount',
        service.mount_target,
        service.container_id1)
    service.mount_ueid2 = entityd.docker.get_ueid(
        'DockerVolumeMount',
        service.mount_target,
        service.container_id2)

    service.children = [service.container_ueid1,
                        service.container_ueid2,
                        service.volume_ueid,
                        service.mount_ueid1,
                        service.mount_ueid2]

    service.attrs = {
        'ID': 'service1',
        'Spec': {
            'EndpointSpec': {'Mode': 'vip'},
            'Labels': {'label1': 'value1'},
            'Mode': {'Replicated': {'Replicas': 3}},
            'Name': 'replicated-service',
            'TaskTemplate': {
                'Networks': [
                    {'Aliases': ['node'], 'Target': service.network_id}],
                'ContainerSpec': {
                    'Mounts': [
                        {
                            'Source': service.volume_name,
                            'Target': service.mount_target,
                            'Type': 'volume'}
                    ]
                }
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
    },{
        'DesiredState': 'running',
        'ID': 'task4',
        'NodeID': 'node2',
        'ServiceID': 'service2',
        'Status': {
            'ContainerStatus': {},
            'Message': 'starting',
            'State': 'starting'},
        'Version': {'Index': 86}
    }]
    service.states = {
        "pending": 0,
        "assigned": 0,
        "accepted": 0,
        "preparing": 1,
        "ready": 0,
        "starting": 1,
        "running": 2,
        "complete": 0,
        "shutdown": 0,
        "failed": 0,
        "rejected": 0,
    }

    return service


@pytest.fixture
def global_service():
    service = pytest.MagicMock()
    service.daemon_id = 'foo'
    service.container_id1 = 'container_id1'
    service.container_id2 = 'container_id2'
    service.container_ueid1 = entityd.docker.get_ueid(
        'DockerContainer', service.container_id1)
    service.container_ueid2 = entityd.docker.get_ueid(
        'DockerContainer', service.container_id2)
    service.network_id = 'bbbbbb'
    service.network_ueid = entityd.docker.get_ueid(
        'DockerNetwork', service.network_id)

    service.volume_name = 'bill'
    service.volume_ueid = entityd.docker.get_ueid(
        'DockerVolume', service.daemon_id, service.volume_name)

    service.mount_target = "/" + service.volume_name
    service.mount_ueid1 = entityd.docker.get_ueid(
        'DockerVolumeMount',
        service.mount_target,
        service.container_id1)
    service.mount_ueid2 = entityd.docker.get_ueid(
        'DockerVolumeMount',
        service.mount_target,
        service.container_id2)

    service.children = [service.container_ueid1,
                        service.container_ueid2,
                        service.volume_ueid,
                        service.mount_ueid1,
                        service.mount_ueid2]

    service.attrs = {
        'ID': 'service2',
        'Spec': {
            'EndpointSpec': {'Mode': 'vip'},
            'Labels': {'label1': 'value1'},
            'Mode': {'Global': {}},
            'Name': 'global-service',
            'TaskTemplate': {
                'Networks': [
                    {'Aliases': ['node'], 'Target': service.network_id}],
                'ContainerSpec': {
                    'Mounts': [
                        {
                            'Source': service.volume_name,
                            'Target': service.mount_target,
                            'Type': 'volume'}
                    ]
                }
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
    service.states = {
        "pending": 0,
        "assigned": 0,
        "accepted": 0,
        "preparing": 0,
        "ready": 0,
        "starting": 0,
        "running": 2,
        "complete": 0,
        "shutdown": 0,
        "failed": 0,
        "rejected": 0,
    }
    return service


@pytest.fixture
def services(monkeypatch):
    def make_client(client_info, services):
        get_client = pytest.MagicMock()
        client_instance = get_client.return_value
        client_instance.info.return_value = client_info
        client_instance.services.list.return_value = iter(services)
        monkeypatch.setattr(DockerClient, "get_client", get_client)

    return make_client


def test_docker_not_available(monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        pytest.MagicMock(side_effect=DockerException))
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
    docker_service.entityd_collection_before(session)
    entities = docker_service.entityd_find_entity(DockerService.name)
    entities = list(entities)
    docker_service.entityd_collection_after(session, None)
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
        'RemoteManagers': [{'NodeID': 'aaaa'}],
    }

    client_info = {
        'ID': 'foo',
        'Name': 'bar',
        'Swarm': swarm,
    }

    testing_services = [replicated_service, global_service]
    services(client_info, testing_services)

    swarm_ueid = entityd.docker.get_ueid(
        'DockerSwarm', cluster['ID'])

    network_ueid = entityd.docker.get_ueid(
        'DockerNetwork', replicated_service.attrs['Spec']['TaskTemplate']['Networks'][0]['Target'])
    docker_service.entityd_configure(session.config)
    docker_service.entityd_collection_before(session)
    entities = docker_service.entityd_find_entity(DockerService.name)
    entities = list(entities)
    docker_service.entityd_collection_after(session, None)
    assert len(entities) == 2

    entities = sorted(entities, key=lambda entity: entity.attrs.get('id').value)

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

        assert entity.attrs.get('id').traits == {'entity:id'}
        assert entity.attrs.get('labels').traits == set()

        for key, value in service.states.items():
            assert entity.attrs.get('replicas:' + key).value == value
            assert entity.attrs.get('replicas:' + key).traits == set()

        assert len(entity.children) == len(service.children)
        for child in service.children:
            assert child in entity.children

        assert len(entity.parents) == 2
        assert swarm_ueid in entity.parents
        assert network_ueid in entity.parents

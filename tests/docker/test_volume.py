import pytest
from docker.errors import DockerException, NotFound

import entityd
from entityd.docker.client import DockerClient
from entityd.docker.volume import DockerVolume, DockerVolumeMount


@pytest.fixture
def docker_volume(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerVolume instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    volume = DockerVolume()
    pm.register(volume, 'entityd.docker.volume.DockerVolume')
    return volume


@pytest.fixture
def docker_volume_mount(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerVolumeMount instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    volume_mount = DockerVolumeMount()
    pm.register(volume_mount, 'entityd.docker.volume.DockerVolumeMount')
    return volume_mount


@pytest.fixture
def volume():
    volume = pytest.MagicMock()
    volume.name = 'volume1'
    volume.attrs = {
        'Driver': 'local',
        'Labels': {'label':'test_label'},
        'Mountpoint': '/var/lib/docker/volumes/aaaa/_data',
        'Name': volume.name,
        'Options': {},
        'Scope': 'local',
    }

    return volume

@pytest.fixture
def volume_no_labels():
    volume = pytest.MagicMock()
    volume.name = 'volume1'
    volume.attrs = {
        'Driver': 'local',
        'Labels': None,
        'Mountpoint': '/var/lib/docker/volumes/aaaa/_data',
        'Name': volume.name,
        'Options': {},
        'Scope': 'local',
    }

    return volume


@pytest.fixture(params=[DockerVolume, DockerVolumeMount])
def entity_class(request):
    return request.param


def test_docker_not_available(entity_class, monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        pytest.MagicMock(side_effect=DockerException))
    instance = entity_class()

    assert not list(instance.entityd_emit_entities())


@pytest.fixture
def docker_client_with_side_effect(monkeypatch):

    def make_docker_client(client_info=None, containers=None, volumes=None):
        get_client = pytest.MagicMock()
        client_instance = get_client.return_value
        client_instance.info.return_value = client_info or {'ID': 'foo'}
        client_instance.containers.list.side_effect =\
            NotFound('Mocked exception')
        client_instance.volumes.list.return_value = iter(volumes or [])

        monkeypatch.setattr(DockerClient, 'get_client', get_client)

    return make_docker_client


def test_container_not_found(running_container,
                             docker_client_with_side_effect):
    containers = [running_container]
    docker_client_with_side_effect(
        client_info={'ID': 'foo'},
        containers=containers,
    )
    dockerVolumeMount = DockerVolumeMount()
    assert len(list(dockerVolumeMount.generate_updates())) == 0


def test_get_ueid(entity_class):
    ueid = entity_class.get_ueid("foo", "bar")
    assert ueid


def test_find_volumes_no_swarm(session, docker_client, docker_volume, volume):
    daemon_id = 'foo'
    client_info = {
        'ID': daemon_id,
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'inactive',
            'NodeID': '',
        },
    }

    daemon_ueid = entityd.docker.get_ueid('DockerDaemon', daemon_id)

    testing_volumes = [volume]
    docker_client(client_info=client_info, volumes=testing_volumes)

    entities = list(docker_volume.entityd_emit_entities())
    volume_entities = [x for x in entities if x.metype == DockerVolume.name]
    assert len(volume_entities) == len(testing_volumes)

    for entity, volume in zip(volume_entities, testing_volumes):
        assert entity.label == volume.attrs['Name']
        assert entity.attrs.get('daemon-id').value == daemon_id
        assert entity.attrs.get('name').value == volume.attrs['Name']
        assert entity.attrs.get('options').value == volume.attrs['Options']
        assert entity.attrs.get('driver').value == volume.attrs['Driver']
        assert entity.attrs.get('mount-point').value == volume.attrs['Mountpoint']
        assert entity.attrs.get('scope').value == volume.attrs['Scope']

        assert entity.attrs.get('daemon-id').traits == {'entity:id'}
        assert entity.attrs.get('name').traits == {'entity:id'}
        assert entity.attrs.get('options').traits == set()
        assert entity.attrs.get('driver').traits == set()
        assert entity.attrs.get('mount-point').traits == set()
        assert entity.attrs.get('scope').traits == set()

        assert len(entity.parents) == 1
        assert daemon_ueid in entity.parents

    group_entities = [x for x in entities if x.metype == 'Group']
    assert len(group_entities) == len(testing_volumes)

    for entity in group_entities:
        assert entity.attrs.get('kind').value == 'label:label'
        assert entity.attrs.get('id').value == 'test_label'


def test_find_volumes_with_swarm(session, docker_client, docker_volume, volume):
    daemon_id = 'foo'
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
        'ID': daemon_id,
        'Name': 'bar',
        'Swarm': swarm,
    }

    daemon_ueid = entityd.docker.get_ueid('DockerDaemon', daemon_id)

    testing_volumes = [volume]
    docker_client(client_info=client_info, volumes=testing_volumes)

    entities = list(docker_volume.entityd_emit_entities())
    volume_entities = [x for x in entities if x.metype == DockerVolume.name]
    assert len(volume_entities) == len(testing_volumes)

    for entity, volume in zip(volume_entities, testing_volumes):
        assert entity.label == volume.attrs['Name']
        assert entity.attrs.get('daemon-id').value == daemon_id
        assert entity.attrs.get('name').value == volume.attrs['Name']
        assert entity.attrs.get('options').value == volume.attrs['Options']
        assert entity.attrs.get('driver').value == volume.attrs['Driver']
        assert entity.attrs.get('mount-point').value == volume.attrs['Mountpoint']
        assert entity.attrs.get('scope').value == volume.attrs['Scope']

        assert entity.attrs.get('daemon-id').traits == {'entity:id'}
        assert entity.attrs.get('name').traits == {'entity:id'}
        assert entity.attrs.get('options').traits == set()
        assert entity.attrs.get('driver').traits == set()
        assert entity.attrs.get('mount-point').traits == set()
        assert entity.attrs.get('scope').traits == set()

        assert len(entity.parents) == 1
        assert daemon_ueid in entity.parents

    group_entities = [x for x in entities if x.metype == 'Group']
    assert len(group_entities) == len(testing_volumes)

    for entity in group_entities:
        assert entity.attrs.get('kind').value == 'label:label'
        assert entity.attrs.get('id').value == 'test_label'
        assert entity.attrs.get('kind').traits == {'entity:id'}
        assert entity.attrs.get('id').traits == {'entity:id'}

def test_find_volumes_no_label(session, docker_client, docker_volume,
                               volume_no_labels):
    daemon_id = 'foo'
    client_info = {
        'ID': daemon_id,
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'inactive',
            'NodeID': '',
        },
    }
    testing_volumes = [volume_no_labels]
    docker_client(client_info=client_info, volumes=testing_volumes)
    assert list(docker_volume.entityd_emit_entities())


def test_find_mounts_no_swarm(session, docker_client, docker_volume_mount,
                              running_container, volume):
    daemon_id = 'foo'
    client_info = {
        'ID': daemon_id,
        'Name': 'bar',
        'Swarm': {
            'LocalNodeState': 'inactive',
            'NodeID': '',
        },
    }

    testing_containers = [running_container]
    testing_volumes = [volume]
    docker_client(client_info=client_info,
                  containers=testing_containers, volumes=testing_volumes)

    entities = list(docker_volume_mount.entityd_emit_entities())
    assert len(entities) == len(testing_containers)

    mounts_and_containers = []
    for container in testing_containers:
        for mount in container.attrs['Mounts']:
            mounts_and_containers.append((mount, container))
    volume_lookup = {vol.name: vol for vol in testing_volumes}

    for entity, m_and_c in zip(entities, mounts_and_containers):
        mount = m_and_c[0]
        container = m_and_c[1]
        vol = volume_lookup[mount['Name']]

        assert entity.label == vol.attrs['Name']
        assert entity.attrs.get('target').value == mount['Destination']
        assert entity.attrs.get('container_id').value == container.id

        assert entity.attrs.get('target').traits == {'entity:id'}
        assert entity.attrs.get('container_id').traits == {'entity:id'}

        assert entity.attrs.get('name').value == volume.attrs['Name']
        assert entity.attrs.get('volume:options').value == volume.attrs['Options']
        assert entity.attrs.get('volume:driver').value == volume.attrs['Driver']
        assert entity.attrs.get('volume:mount-point').value == volume.attrs['Mountpoint']
        assert entity.attrs.get('volume:scope').value == volume.attrs['Scope']
        assert entity.attrs.get('volume:mode').value == mount['Mode']
        assert entity.attrs.get('volume:read-write').value == mount['RW']
        assert entity.attrs.get('volume:source').value == mount['Source']

        assert entity.attrs.get('name').traits == set()
        assert entity.attrs.get('volume:options').traits == set()
        assert entity.attrs.get('volume:driver').traits == set()
        assert entity.attrs.get('volume:mount-point').traits == set()
        assert entity.attrs.get('volume:scope').traits == set()
        assert entity.attrs.get('volume:mode').traits == set()
        assert entity.attrs.get('volume:read-write').traits == set()
        assert entity.attrs.get('volume:source').traits == set()

        assert len(entity.parents) == 2

        container_ueid = entityd.docker.get_ueid('DockerContainer',
                                                 container.id)
        assert container_ueid in entity.parents

        volume_ueid = entityd.docker.get_ueid('DockerVolume',
                                              daemon_id,
                                              volume.attrs['Name'])
        assert volume_ueid in entity.parents



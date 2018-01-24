import pytest
from docker.errors import DockerException

import entityd
from entityd.docker.client import DockerClient
from entityd.docker.container import DockerContainer
from entityd.docker.daemon import DockerDaemon


@pytest.fixture
def docker_container(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainer instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    container = DockerContainer()
    pm.register(container, 'entityd.docker.container.DockerContainer')
    return container


def test_docker_not_available(monkeypatch, docker_container):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        pytest.MagicMock(side_effect=DockerException))
    assert len(list(docker_container.entityd_emit_entities())) == 0


def test_emit_entities(monkeypatch, docker_container,
                       running_container, finished_container):

    containers = [running_container, finished_container]
    daemon_id = 'foo'

    get_client = pytest.MagicMock()
    client_instance = get_client.return_value
    client_instance.info.return_value = {'ID': daemon_id}
    client_instance.containers.list.return_value = iter(containers)
    monkeypatch.setattr(DockerClient, "get_client", get_client)

    daemon_ueid = DockerDaemon.get_ueid(daemon_id)

    entities = list(docker_container.entityd_emit_entities())
    entities_containers = [entity for entity
                           in entities if entity.metype == "Docker:Container"]
    entities_labels = [entity for entity
                       in entities if entity.metype == "Group"]
    assert len(entities) == 4
    assert len(entities_containers) == 2
    assert len(entities_labels) == 2

    for entity, container in zip(entities_containers, containers):
        assert daemon_ueid in entity.parents
        assert entity.exists == container.should_exist
        assert entity.attrs.get('name').value == container.name
        assert entity.attrs.get('id').value == container.id
        assert entity.attrs.get('id').traits == {"entity:id"}
        assert entity.attrs.get('state:status').value == container.status
        assert entity.attrs.get('image:id').value == container.image.id
        assert entity.attrs.get('image:name').value == container.image.tags
        assert entity.attrs.get('state:started-at').value == \
            container.attrs['State']['StartedAt']

        if container.status not in ["exited", "dead"]:
            assert entity.attrs.get('state:exit-code').value is None
            assert entity.attrs.get('state:error').value is None
            assert entity.attrs.get('state:finished-at').value is None
        else:
            assert entity.attrs.get('state:exit-code').value == \
                container.attrs['State']['ExitCode']
            assert entity.attrs.get('state:error').value == \
                container.attrs['State']['Error']
            assert entity.attrs.get('state:finished-at').value == \
                container.attrs['State']['FinishedAt']

        network_ueid = entityd.docker.get_ueid('DockerNetwork',
                                               container.network_id)
        assert network_ueid in entity.parents

    for label, container in zip(entities_labels, containers):
        assert label.metype == 'Group'
        assert label.attrs.get('kind').value == 'label:label'
        assert label.attrs.get('kind').traits == {'entity:id'}
        assert label.attrs.get('id').value == container.labels['label']
        assert label.attrs.get('id').traits == {'entity:id'}
        assert len(label.children) == 1
        assert docker_container.get_ueid(container.id) in label.children


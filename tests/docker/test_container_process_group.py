import os
from datetime import datetime

import docker
import pytest
import syskit
from docker.errors import DockerException

from entityd.docker.client import DockerClient
from entityd.docker.container import DockerContainer
from entityd.docker.container_group import DockerContainerGroup


@pytest.fixture
def container_group(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainerGroup instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    dcpg = DockerContainerGroup()
    pm.register(dcpg, 'entityd.docker.container_group.DockerContainerGroup')
    return dcpg


def test_docker_not_available(monkeypatch):
    monkeypatch.setattr('entityd.docker.client.docker.DockerClient',
                        pytest.Mock(side_effect=DockerException))

    group = DockerContainerGroup()
    assert not list(group.entityd_find_entity(group.name))


def test_attrs_raises_exception():
    group = DockerContainerGroup()
    with pytest.raises(LookupError):
        group.entityd_find_entity(
            DockerContainerGroup.name,
            attrs="foo")


def test_not_provided():
    group = DockerContainerGroup()
    assert group.entityd_find_entity('foo') is None


def test_get_process_ueid(session, container_group):
    container_group.entityd_sessionstart(session)
    container_group.entityd_configure(session.config)

    pid = os.getpid()
    ueid = container_group.get_process_ueid(pid)
    assert ueid


def test_non_runnning_containers(session, container_group,
                                 docker_client, running_container,
                                 finished_container):
    containers = [running_container, finished_container]

    docker_client(client_info={'ID':'foo'}, containers=containers)

    container_group.entityd_sessionstart(session)
    container_group.entityd_configure(session.config)

    entities = container_group.entityd_find_entity(DockerContainerGroup.name)
    entities_list = list(entities)

    assert len(entities_list) == 1

    for entity in entities_list:
        assert entity.label == running_container.name
        assert entity.attrs.get('kind').value == DockerContainer.name
        assert entity.attrs.get('kind').traits == {'entity:id'}
        container_ueid = DockerContainer.get_ueid(running_container.id)
        assert entity.attrs.get('id').value == str(container_ueid)
        assert entity.attrs.get('id').traits == {'entity:id'}


def test_empty_processes_from_top(session, container_group,
                                  docker_client, running_container):

    running_container.top.return_value = {"Processes": None}
    containers = [running_container]

    docker_client(client_info={'ID':'foo'}, containers=containers)

    container_group.entityd_sessionstart(session)
    container_group.entityd_configure(session.config)

    entities = container_group.entityd_find_entity(
        DockerContainerGroup.name)
    entities = list(entities)

    assert len(entities) == 0


def test_generate_updates(monkeypatch, session, running_container,
                          container_group):

    containers = [running_container]

    get_client = pytest.MagicMock()
    client_instance = get_client.return_value
    client_instance.info.return_value = {'ID': 'foo'}
    client_instance.containers.list.return_value = iter(containers)
    monkeypatch.setattr(DockerClient, "get_client", get_client)

    procs = {}
    for x in range(5):
        name = "proc" + str(x)
        proc = pytest.MagicMock(name=name, pid=x)
        proc.start_time.timestamp.return_value = datetime.utcnow().timestamp()
        procs[x] = proc

    # 0
    # ├── 1
    # │   ├── 2
    # │   └── 3
    # └── 4

    procs[0].children.return_value = iter([procs[1], procs[4]])
    procs[1].children.return_value = iter([procs[2], procs[3]])

    # We monkey patch over the syskit get process to return a
    # process from our dict and an exception if it's not present
    def get_proc(pid):
        if pid in procs:
            return procs[pid]
        else:
            raise syskit.NoSuchProcessError

    del procs[3]

    monkeypatch.setattr(syskit, "Process", get_proc)

    container_group.entityd_sessionstart(session)
    container_group.entityd_configure(session.config)

    # Add the ueid to the mocked processes to help with the loop below
    for proc in procs.values():
        proc.ueid = container_group.get_process_ueid(proc.pid)

    entities = container_group.entityd_find_entity(DockerContainerGroup.name)
    entities_list = list(entities)
    assert len(entities_list) == 1

    entity = entities_list[0]
    container_ueid = DockerContainer.get_ueid(running_container.id)

    assert entity.label == running_container.name
    assert entity.attrs.get('kind').value == DockerContainer.name
    assert entity.attrs.get('kind').traits == {'entity:id'}
    assert entity.attrs.get('id').value == str(container_ueid)
    assert entity.attrs.get('id').traits == {'entity:id'}
    assert container_ueid in entity.children
    for proc in procs.values():
        assert proc.ueid in entity.children

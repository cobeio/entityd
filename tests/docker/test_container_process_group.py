import os
from datetime import datetime

import docker
import pytest
import syskit
from docker.errors import DockerException
from mock import Mock, patch, MagicMock

from entityd.docker.docker import (
    DockerContainerProcessGroup,
    DockerContainer, Client)


@pytest.fixture
def container_process_group(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainerProcessGroup instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    dcpg = DockerContainerProcessGroup()
    pm.register(dcpg, 'entityd.docker.docker.DockerContainerProcessGroup')
    return dcpg


def test_docker_not_available( monkeypatch):
    monkeypatch.setattr(docker, 'DockerClient',
                        Mock(side_effect=DockerException))

    group = DockerContainerProcessGroup()
    assert not list(group.entityd_find_entity(group.name))


def test_attrs_raises_exception():
    group = DockerContainerProcessGroup()
    with pytest.raises(LookupError):
        group.entityd_find_entity(
            DockerContainerProcessGroup.name,
            attrs="foo")


def test_not_provided():
    group = DockerContainerProcessGroup()
    assert group.entityd_find_entity('foo') is None


def test_get_process_ueid(session, container_process_group):
    container_process_group.entityd_sessionstart(session)
    container_process_group.entityd_configure(session.config)

    pid = os.getpid()
    ueid = container_process_group.get_process_ueid(pid)
    assert ueid


def test_get_missed_process():
    with patch('entityd.docker.docker.Process') as mock:
        # Create some mock process objects
        procs = list()
        for x in range(2,7):
            mock_proc = Mock()
            mock_proc.pid = x
            procs.append(mock_proc)

        # Set the side effects of syskit.Process to
        # return iterators of our processes
        instance = mock.return_value
        instance.children.side_effect = [
            iter([procs[0], procs[1]]),
            iter([procs[2], procs[3], procs[4]]),
            iter([])]

        # These are the procs we say have already been added
        already_added = {procs[0].pid, procs[3].pid, procs[4].pid}

        temp = DockerContainerProcessGroup()
        results = temp.get_missed_process_children(1, already_added)
        results = set(results)
        # Check we got the remaining processes from the function
        assert results == {procs[1].pid, procs[2].pid}


def test_generate_updates(monkeypatch, session, running_container, container_process_group):

    containers = [running_container]

    get_client = MagicMock()
    client_instance = get_client.return_value
    client_instance.info.return_value = {'ID': 'foo'}
    client_instance.containers.list.return_value = iter(containers)
    monkeypatch.setattr(Client, "get_client", get_client)

    procs = {}
    for x in range(5):
        name = "proc" + str(x)
        proc = MagicMock(name=name, pid=x)
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
    # process from our dict
    def get_proc(pid):
        return procs[pid]

    monkeypatch.setattr(syskit, "Process", get_proc)

    container_process_group.entityd_sessionstart(session)
    container_process_group.entityd_configure(session.config)

    # Add the ueid to the mocked processes to help with the loop below
    for proc in procs.values():
        proc.ueid = container_process_group.get_process_ueid(proc.pid)

    entities = container_process_group.entityd_find_entity(DockerContainerProcessGroup.name)
    entities = list(entities)
    assert len(entities) == 1

    for entity in entities:
        assert entity.label == running_container.name
        assert entity.attrs.get('kind').value == DockerContainer.name
        container_ueid = DockerContainer.get_ueid(running_container.id)

        assert entity.attrs.get('id').value == str(container_ueid)
        assert container_ueid in entity.children
        for proc in procs.values():
            assert proc.ueid in entity.children



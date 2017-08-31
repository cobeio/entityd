import os
from datetime import datetime

import pytest
from docker.errors import DockerException
from mock import Mock, patch, MagicMock

from entityd.docker.docker import DockerContainerProcessGroup, DockerDaemon, \
    DockerContainer


@pytest.fixture
def container_process_group(pm, host_entity_plugin):  # pylint: disable=unused-argument
    """A DockerContainerProcessGroup instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    """
    dcpg = DockerContainerProcessGroup()
    pm.register(dcpg, 'entityd.docker.docker.DockerContainerProcessGroup')
    return dcpg


def test_docker_not_available():
    with patch('entityd.docker.docker.DockerClient') as docker_client:
        docker_client.side_effect = DockerException
        group = DockerContainerProcessGroup()

        assert not list(group.entityd_find_entity(group.name))


def test_attrs_raises_exception():
    with pytest.raises(LookupError):
        group = DockerContainerProcessGroup()
        group.entityd_find_entity(DockerContainerProcessGroup.name, attrs="foo")


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



@patch('entityd.docker.docker.DockerClient')
@patch('entityd.docker.docker.Process', name="help")
def test_generate_updates(syskit_process, client, session, running_container, container_process_group):
    client_instance = client.return_value
    client_instance.info.return_value = {'ID': 'foo'}

    proc = MagicMock(name="bob", pid=123)
    proc.start_time.timestamp.return_value = datetime.utcnow().timestamp()

    proc2 = MagicMock(name="steve", pid=456)
    proc2.start_time.timestamp.return_value = datetime.utcnow().timestamp()

    proc3 = MagicMock(name="jon", pid=789)
    proc3.start_time.timestamp.return_value = datetime.utcnow().timestamp()

    proc.children.return_value = iter([proc2])
    proc2.children.return_value = iter([proc3])

    # We add the three procs to the side_effect twice because we want to
    # generate the ueid here to make sure they get into the
    # children of the group
    syskit_process.side_effect = [proc, proc2, proc3, proc, proc2, proc, proc2, proc3]

    containers = [running_container]

    client_instance.containers.list.return_value = iter(containers)

    container_process_group.entityd_sessionstart(session)
    container_process_group.entityd_configure(session.config)

    proc.ueid = container_process_group.get_process_ueid(proc.pid)
    proc2.ueid = container_process_group.get_process_ueid(proc2.pid)
    proc3.ueid = container_process_group.get_process_ueid(proc3.pid)

    entities = container_process_group.entityd_find_entity(DockerContainerProcessGroup.name)

    for entity in entities:
        assert entity.label == running_container.name
        assert entity.attrs.get('kind').value == DockerContainer.name
        container_ueid = DockerContainer.get_ueid(running_container.id)

        assert entity.attrs.get('ownerUEID').value == container_ueid
        assert container_ueid in entity.children
        assert proc.ueid in entity.children
        assert proc2.ueid in entity.children
        assert proc3.ueid in entity.children



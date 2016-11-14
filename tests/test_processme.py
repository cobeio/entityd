import collections
import functools
import os
import subprocess
import time

import act
import cobe
import docker
import requests
import pytest
import syskit
import zmq

import entityd.hookspec
import entityd.hostme
import entityd.processme


import entityd.core
import entityd.kvstore


container_ents = collections.namedtuple(
    'container_ents', ['entities',
                       'container_ueid',
                       'container_id',
                       'container_top_pid'])


@pytest.fixture(autouse=True)
def revert_mocking_of_cpuusage(mock_cpuusage):
    """Revert the mocking out of the CpuUsage calculation thread."""
    mock_cpuusage.revert()


@pytest.fixture
def module_mock_cpuusage(monkeypatch):
    """Mock out CpuUsage calculation thread.

    Whilst reverting mocking out of CpuUsage for this module in above
    autouse fixture `revert_mocking_of_cpuusage`, a couple of tests do
    actually need it mocking out, which this performs.
    """
    cpuusage = pytest.Mock()
    cpuusage.listen_endpoint = 'inproc://cpuusage'
    monkeypatch.setattr(entityd.processme, 'CpuUsage',
                        pytest.Mock(return_value=cpuusage))


@pytest.fixture
def procent(request, pm, host_entity_plugin, session):  # pylint: disable=unused-argument
    """A entityd.processme.ProcessEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    """
    procent = entityd.processme.ProcessEntity()
    pm.register(procent, 'entityd.processme.ProcessEntity')
    request.addfinalizer(procent.entityd_sessionfinish)
    return procent


@pytest.fixture(scope='module')
def debian_image():
    """The debian docker image.

    The image name, consisting of both the repository and tag joined
    by ``:``, is returned as a string.
    """
    REPO = 'eu.gcr.io/cobesaas/debian'
    TAG = '8.5'
    IMAGE = REPO + ':' + TAG
    out = subprocess.check_output(['docker', 'images', REPO])
    out = out.decode('utf-8')
    image = collections.namedtuple('image', ['repo', 'tag', 'id'])
    images = [image(*l.split()[:3]) for l in out.splitlines()[1:]]
    deb_image = {i for i in images if i.tag == TAG}
    if not deb_image:
        subprocess.check_call(['gcloud', 'docker', 'pull', IMAGE])
    return IMAGE


@pytest.yield_fixture(scope='module')
def container(debian_image):
    """Run a container."""
    docker_client = docker.Client(base_url='unix://var/run/docker.sock')
    try:
        container = docker_client.create_container(
            image=debian_image,
            command='/bin/bash -c "while true; do sleep 1; done"',
            name='sleeper',
        )
    except requests.ConnectionError:
        pytest.skip('Test not possible due to docker daemon not running.')
    docker_client.start(container=container.get('Id'))
    container_top_pid = int(docker_client.top(container)['Processes'][0][1])
    yield container_top_pid, container['Id']
    docker_client.stop(container['Id'])
    docker_client.remove_container(container['Id'])


@pytest.fixture
def container_entities(procent, session, container):
    """Give all process entities and container id/pid if container running."""
    container_top_pid, container_id = container
    update = entityd.EntityUpdate('Container')
    update.attrs.set('id', 'docker://' + container_id, traits={'entity:id'})
    container_ueid = update.ueid
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', None)
    return container_ents(entities=entities,
                          container_ueid=container_ueid,
                          container_id=container_id,
                          container_top_pid=container_top_pid)


@pytest.fixture(params=[{'pid': os.getpid()}, None])
def process_entity(procent, request, session, kvstore):  # pylint: disable=unused-argument
    """Provide entity for the current process.

    This fixture operates to test both ``processme`` paths of finding
    entities - via finding all process entities and also via finding an
    entity with particular attributes.
    """
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', request.param)
    for entity in entities:
        if entity.attrs.get('pid').value == os.getpid():
            return entity


@pytest.fixture
def syskit_user_error(monkeypatch):
    """Mock syskit to generate an error if user attribute is requested."""
    @property
    def syskit_error(self):  # pylint: disable=unused-argument
        raise syskit.AttrNotAvailableError
    monkeypatch.setattr(syskit.Process, 'user', syskit_error)


@pytest.fixture
def no_docker_client(monkeypatch):
    """Mock out docker Client to test no docker client being available."""
    monkeypatch.setattr(docker, 'Client',
                        pytest.Mock(side_effect=docker.errors.DockerException))


@pytest.fixture
def cpuusage_interval(monkeypatch):
    monkeypatch.setattr(entityd.processme, "CpuUsage",
                        functools.partial(entityd.processme.CpuUsage,
                                          interval=0.1))


def test_no_docker_client(no_docker_client, procent):   # pylint: disable=unused-argument
    assert procent._docker_client is None


def test_configure(procent, config):
    procent.entityd_configure(config)
    assert config.entities['Process'].obj is procent


def test_find_entity(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Process'
        assert entity.attrs.get('starttime').value
        count += 1
    assert count


def test_entities_have_core_attributes(procent, session, kvstore): # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Process'
        if not entity.exists:
            continue
        for attr in 'binary pid starttime ppid host cputime utime stime vsz ' \
                    'rss uid euid suid username gid sgid egid sessionid ' \
                    'command'.split():
            assert entity.attrs.get(attr)
        count += 1
    assert count


def test_container_entity_has_containerid_attribute(container,
                                                    procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    container_top_pid, containerid = container
    entities = procent.entityd_find_entity(
        'Process', {'pid': container_top_pid})
    entity = next(entities)
    assert entity.attrs.get('containerid').value == containerid
    with pytest.raises(StopIteration):
        next(entities)


def test_get_process_containers_handles_missing_process(container,
                                                        session, procent):
    procent.entityd_sessionstart(session)
    container_top_pid, containerid = container
    pids = ['non existent pid', container_top_pid, 'another non existent pid']
    assert procent.get_process_containers(
        pids) == {container_top_pid: containerid}


def test_get_container_data_when_no_docker_client(
        container, no_docker_client, procent):   # pylint: disable=unused-argument
    assert procent.get_process_containers({'pid': 1}) == {}


def test_non_container_entity_has_no_containerid_attr(process_entity):
    with pytest.raises(KeyError):
        process_entity.attrs.get('containerid')


@pytest.mark.parametrize(
    ('attr', 'traits'),
    [('pid', {'entity:id'}),
     ('starttime', {'entity:id', 'time:posix', 'unit:seconds'}),
     ('host', {'entity:id', 'entity:ueid'}),
     ('cputime', {'metric:counter', 'time:duration', 'unit:seconds'}),
     ('utime', {'metric:counter', 'time:duration', 'unit:seconds'}),
     ('stime', {'metric:counter', 'time:duration', 'unit:seconds'}),
     ('vsz', {'metric:gauge', 'unit:bytes'}),
     ('rss', {'metric:gauge', 'unit:bytes'}),
     ('binary', set()),
     ('ppid', set()),
     ('uid', set()),
     ('euid', set()),
     ('suid', set()),
     ('username', set()),
     ('gid', set()),
     ('sgid', set()),
     ('egid', set()),
     ('sessionid', set()),
     ('command', set()),
    ])
def test_pid_traits(process_entity, attr, traits):
    assert process_entity.attrs.get(attr).traits == traits


def test_find_current_process_entity(process_entity):
    assert process_entity.metype == 'Process'
    assert process_entity.attrs.get('pid').value == os.getpid()
    assert process_entity.attrs.get('starttime').value
    assert process_entity.attrs.get('ppid').value == os.getppid()


def test_find_entity_with_unknown_attrs(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', {'unknown': 1})
    with pytest.raises(StopIteration):
        next(entities)


def test_find_entity_with_binary(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', {'binary': 'py.test'})
    proc = next(entities)
    assert proc.metype == 'Process'
    assert proc.attrs.get('binary').value == 'py.test'


def test_get_ueid_new(kvstore, session, procent):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    ueid = procent.get_ueid(proc)
    assert ueid
    assert isinstance(ueid, cobe.UEID)


def test_get_ueid_reuse(kvstore, session, procent):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc0 = syskit.Process(os.getpid())
    ueid0 = procent.get_ueid(proc0)
    proc1 = syskit.Process(os.getpid())
    ueid1 = procent.get_ueid(proc1)
    assert ueid0 == ueid1


def test_get_ueid(session, host_entity_plugin):  # pylint: disable=unused-argument
    procent = entityd.processme.ProcessEntity()
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    ueid = procent.get_ueid(proc)
    ueid2 = next(procent.filtered_processes({'pid': os.getpid()})).ueid
    assert ueid == ueid2
    procent.entityd_sessionfinish()


def test_get_parents_nohost_noparent(session, kvstore, procent):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    rels = procent.get_parents(proc, {proc.pid: proc})
    assert not rels


def test_get_parents_parent(procent, session, kvstore, host_entity_plugin):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    host = list(host_entity_plugin.entityd_find_entity('Host', None))[0]
    proc = syskit.Process(os.getpid())
    pproc = syskit.Process(os.getppid())
    rels = procent.get_parents(proc, {proc.pid: proc, pproc.pid: pproc})
    assert len(rels) == 1
    assert rels[0] != host.ueid
    assert isinstance(rels[0], cobe.UEID)


def test_root_process_has_host_parent(procent, session, kvstore, monkeypatch):  #pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    hostupdate = entityd.EntityUpdate('Host')
    monkeypatch.setattr(session.pluginmanager.hooks,
                        'entityd_find_entity',
                        pytest.Mock(return_value=[[hostupdate]]))
    proc = syskit.Process(1)
    assert proc.ppid == 0
    hostueid, = procent.get_parents(proc, {proc.pid: proc})
    assert hostueid == hostupdate.ueid


def test_find_single_container_parent(container_entities):
    count = 0
    for entity in container_entities.entities:
        pid = entity.attrs.get('pid').value
        parents = [ueid_obj for ueid_obj in list(entity.parents)]
        if container_entities.container_ueid in parents:
            assert pid == container_entities.container_top_pid
            count += 1
    assert count == 1


def test_no_possible_username_possible(syskit_user_error, process_entity):  # pylint: disable=unused-argument
    with pytest.raises(KeyError):
        process_entity.attrs.get('username')


@pytest.fixture
def proctable():
    proc = syskit.Process(os.getpid())
    pproc = syskit.Process(os.getppid())
    return {proc.pid: proc, pproc.pid: pproc}


def test_processes(procent, proctable, monkeypatch, session, kvstore):  # pylint: disable=unused-argument
    # Wire up a fake process table
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(procent, 'update_process_table',
                        pytest.Mock(return_value=proctable))

    # Check we get MEs for the processes.
    gen = procent.processes()
    pids = []
    for me in gen:
        assert me.metype == 'Process'
        assert me.ueid
        if not me.exists:
            continue
        pids.append(me.attrs.get('pid').value)
    assert pids
    assert sorted(proctable.keys()) == sorted(pids)


def test_processes_deleted(procent, proctable, monkeypatch, session, kvstore):  # pylint: disable=unused-argument
    # Wire up a fake process table
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(procent, 'update_process_table',
                        pytest.Mock(return_value=proctable))

    # Extract the ME for the py.test process
    gen = procent.processes()
    for me in gen:
        if me.attrs.get('pid').value == os.getppid():
            pprocme = me

    # Delete py.test process from process table and check deleted ME
    del proctable[os.getpid()]
    monkeypatch.setattr(
        procent,
        'update_process_table',
        pytest.Mock(return_value=proctable))
    gen = procent.processes()
    pprocme2, = gen
    assert pprocme2.ueid == pprocme.ueid


def test_update_process_table():
    active = entityd.processme.ProcessEntity.update_process_table({})
    assert isinstance(active[os.getpid()], syskit.Process)
    pt2 = entityd.processme.ProcessEntity.update_process_table(active)
    assert pt2[os.getpid()] is active[os.getpid()]


def test_process_table_vanished(monkeypatch):
    # A process vanishes during creation
    monkeypatch.setattr(syskit, 'Process',
                        pytest.Mock(side_effect=syskit.NoSuchProcessError))
    monkeypatch.setattr(syskit.Process, 'enumerate',
                        pytest.Mock(return_value=[42]))
    pt = entityd.processme.ProcessEntity.update_process_table({})
    assert not pt


def test_process_table_vanished_refresh(monkeypatch):
    # A process vanishes during creation
    proc = syskit.Process(os.getpid())
    proc.refresh = pytest.Mock(side_effect=syskit.NoSuchProcessError)
    monkeypatch.setattr(syskit.Process, 'enumerate',
                        pytest.Mock(return_value=[os.getpid()]))
    pt = entityd.processme.ProcessEntity.update_process_table(
        {os.getpid(): proc})
    assert not pt


def test_specific_process_deleted(procent, session, kvstore, monkeypatch):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    pid = os.getpid()

    monkeypatch.setattr(syskit, 'Process',
                        pytest.Mock(side_effect=syskit.NoSuchProcessError))
    monkeypatch.setattr(syskit.Process, 'enumerate',
                        pytest.Mock(return_value=[42]))
    entities = procent.entityd_find_entity('Process', {'pid': pid})
    with pytest.raises(StopIteration):
        next(entities)


def test_specific_parent_deleted(procent, session, kvstore, monkeypatch):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())

    def patch_syskit():
        monkeypatch.setattr(syskit, 'Process',
                            pytest.Mock(side_effect=syskit.NoSuchProcessError))
        return proc

    monkeypatch.setattr(syskit, 'Process', pytest.Mock(
        return_value=patch_syskit()))
    monkeypatch.setattr(syskit.Process, 'enumerate',
                        pytest.Mock(return_value=[os.getpid()]))

    entities = procent.entityd_find_entity('Process', {'pid': os.getpid()})
    proc = next(entities)
    assert proc.attrs.get('pid').value == os.getpid()
    # If the parent isn't present, we continue anyway, but it doesn't appear
    #  in relations.
    assert not proc.parents._relations
    with pytest.raises(StopIteration):
        proc = next(entities)


@pytest.fixture
def zombie_process(request):
    popen = subprocess.Popen(['true'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    request.addfinalizer(popen.kill)
    t = time.time()
    while (time.time() - t < 5 and
           syskit.Process(popen.pid).status != syskit.ProcessStatus.zombie):
        time.sleep(0.1)
    if syskit.Process(popen.pid).status != syskit.ProcessStatus.zombie:
        pytest.fail('Failed to create a zombie process for testing.')

    return popen


def test_zombie_process(procent, session, kvstore, monkeypatch,  # pylint: disable=unused-argument
                        zombie_process):
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process',
                                           {'pid': zombie_process.pid})
    entity = next(entities)
    for attr in ['executable', 'args', 'argcount']:
        with pytest.raises(KeyError):
            assert entity.attrs.get(attr)


def test_cpu_usage_sock_not_present_one(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.session = session
    entities = procent.entityd_find_entity('Process', {'pid': os.getpid()})
    entity = next(entities)
    # CPU usage only available after background thread has updated
    with pytest.raises(KeyError):
        _ = entity.attrs.get('cpu')


def test_cpu_usage_sock_not_present_all(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.session = session
    entities = procent.entityd_find_entity('Process', None)
    for entity in entities:
        with pytest.raises(KeyError):
            _ = entity.attrs.get('cpu')


def test_cpu_usage_attr_is_present(
        cpuusage_interval, procent, session, kvstore): # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    assert procent.cpu_usage_thread.is_alive()
    while True:
        entities = procent.entityd_find_entity('Process', {'pid': os.getpid()})
        entity = next(entities)
        try:
            cpu_usage = entity.attrs.get('cpu')
        except KeyError:
            time.sleep(1)
            continue
        else:
            assert isinstance(cpu_usage.value, float)
            assert cpu_usage.traits == {'metric:gauge', 'unit:percent'}
            break


def test_cpu_usage_not_running_one(
        module_mock_cpuusage, procent, session, kvstore):   # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', {'pid': os.getpid()})
    entity = next(entities)
    with pytest.raises(KeyError):
        _ = entity.attrs.get('cpu')


def test_cpu_usage_not_running_all(
        module_mock_cpuusage, procent, session, kvstore):   # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', None)
    for entity in entities:
        with pytest.raises(KeyError):
            _ = entity.attrs.get('cpu')


def test_entity_has_label(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Process'
        if not entity.exists:
            continue
        assert entity.label == entity.attrs.get('binary').value
        count += 1
    assert count


def test_host_ueid(procent, session, host_entity_plugin):
    procent.entityd_sessionstart(session)
    host = list(host_entity_plugin.entityd_find_entity('Host', None))[0]
    assert procent.host_ueid == host.ueid


def test_host_ueid_cached(procent, session, host_entity_plugin):
    procent.entityd_sessionstart(session)
    host = list(host_entity_plugin.entityd_find_entity('Host', None))[0]
    first_ueid = procent.host_ueid
    assert first_ueid == host.ueid
    assert procent.host_ueid is first_ueid


def test_host_ueid_no_host_plugin(monkeypatch, procent, session):
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(
        session.pluginmanager.hooks,
        'entityd_find_entity',
        pytest.Mock(return_value=[]),
    )
    with pytest.raises(LookupError):
        assert procent.host_ueid


def test_host_ueid_no_host_entity(monkeypatch, procent, session):
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(
        session.pluginmanager.hooks,
        'entityd_find_entity',
        pytest.Mock(return_value=[iter([])]),
    )
    with pytest.raises(LookupError):
        assert procent.host_ueid


class TestCpuUsage:

    @pytest.fixture
    def context(self):
        return act.zkit.new_context()

    @pytest.fixture
    def cpuusage(self, context):
        return entityd.processme.CpuUsage(context, interval=0.1)

    def test_timer(self, monkeypatch, cpuusage):
        """Test the timer is firing, and triggers an update."""
        monkeypatch.setattr(cpuusage, 'update',
                            pytest.Mock(side_effect=cpuusage.stop))
        cpuusage.start()
        cpuusage.join()
        assert cpuusage.update.called

    def test_exception_logged(self, monkeypatch, cpuusage):
        monkeypatch.setattr(cpuusage, '_run',
                            pytest.Mock(side_effect=ZeroDivisionError))
        monkeypatch.setattr(cpuusage, '_log', pytest.Mock())
        stop = lambda: monkeypatch.setattr(cpuusage, '_run',
                                           pytest.Mock())
        cpuusage._log.exception.side_effect = stop
        cpuusage.start()
        cpuusage.join(timeout=2)
        assert cpuusage._log.exception.called

    def test_update(self, cpuusage):
        """Test the update functionality."""
        cpuusage.update()
        first_run = cpuusage.last_run_processes.items()
        for key, proc in first_run:
            assert isinstance(proc, syskit.Process)
        assert not cpuusage.last_run_percentages
        cpuusage.update()
        for key, proc in cpuusage.last_run_processes.items():
            if key in [k for k, p in first_run]:
                assert isinstance(proc, syskit.Process)
                assert cpuusage.last_run_percentages.get(key) is not None

    def test_one_proc_cpu_calculation(self, cpuusage):
        proc = pytest.Mock()
        proc.pid = 1
        proc.cputime = 0.0
        now = time.time()
        proc.start_time.timestamp.return_value = now
        proc.refreshed.timestamp.return_value = now
        assert cpuusage.percent_cpu_usage(proc, proc) == 0.0

        proc1 = pytest.Mock()
        proc1.start_time.timestamp.return_value = proc.start_time.timestamp()
        proc1.cputime = 1.0
        now += 1
        proc1.refreshed.timestamp.return_value = now
        assert cpuusage.percent_cpu_usage(proc, proc1) >= 99.0

        proc2 = pytest.Mock()
        proc2.cputime = 2.0
        proc2.start_time.timestamp.return_value = proc.start_time.timestamp()
        now += 2
        proc2.refreshed.timestamp.return_value = now
        assert cpuusage.percent_cpu_usage(proc1, proc2) == 50

    def test_get_one(self, request, context, cpuusage):
        cpuusage.start()
        request.addfinalizer(cpuusage.join)
        request.addfinalizer(cpuusage.stop)
        req = context.socket(zmq.PAIR)
        req.connect('inproc://cpuusage')
        pid = os.getpid()
        while True:
            req.send_pyobj(pid)
            pc = req.recv_pyobj()
            if pc is None:
                continue
            else:
                assert isinstance(pc, float)
                break

    def test_get_all(self, request, context, cpuusage):
        cpuusage.start()
        request.addfinalizer(cpuusage.join)
        request.addfinalizer(cpuusage.stop)
        req = context.socket(zmq.PAIR)
        req.connect('inproc://cpuusage')
        pid = os.getpid()
        while True:
            req.send_pyobj(None)
            pc = req.recv_pyobj()
            if not pc:
                continue
            else:
                assert isinstance(pc, dict)
                assert isinstance(pc[pid], float)
                break

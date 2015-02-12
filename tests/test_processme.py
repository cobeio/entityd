import base64
import os
import subprocess
import time

import syskit
import pytest

import entityd.hookspec
import entityd.processme


import entityd.core
import entityd.kvstore


@pytest.fixture
def procent(pm):
    """A entityd.processme.ProcessEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    """
    procent = entityd.processme.ProcessEntity()
    pm.register(procent, 'entityd.processme.ProcessEntity')
    return procent


def test_plugin_registered(pm):
    name = 'entityd.processme'
    entityd.processme.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.processme.ProcessEntity')


def test_session_hooks_reload_proc(procent, session, kvstore):  # pylint: disable=unused-argument
    # Create an entry in known_ueids
    proc = syskit.Process(os.getpid())
    procent.entityd_sessionstart(session)
    ueid = procent.get_ueid(proc)
    assert ueid in procent.known_ueids

    # Persist that entry to the kvstore
    procent.entityd_sessionfinish()

    # Reload this entry from the kvstore
    procent.known_ueids.clear()
    procent.entityd_sessionstart(session)
    assert ueid in procent.known_ueids


def test_sessionfinish_delete_ueids(procent, session, kvstore):
    # Create an entry in known_ueids
    proc = syskit.Process(os.getpid())
    procent.entityd_sessionstart(session)
    ueid = procent.get_ueid(proc)
    assert ueid in procent.known_ueids

    # Persist that entry to the kvstore
    procent.entityd_sessionfinish()
    assert kvstore.get(entityd.processme.ProcessEntity.prefix +
                       base64.b64encode(ueid).decode('ascii')) == ueid

    # Check that entry is deleted from the kvstore
    procent.known_ueids.clear()
    procent.entityd_sessionfinish()
    with pytest.raises(KeyError):
        kvstore.get(
            entityd.processme.ProcessEntity.prefix +
            base64.b64encode(ueid).decode('ascii'))


def test_configure(procent, config):
    procent.entityd_configure(config)
    assert config.entities['Process'].obj is procent


def test_find_entity(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Process'
        if entity.deleted:
            continue
        assert entity.attrs.get('starttime').value
        count += 1
    assert count


def test_entity_attrs(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Process'
        if entity.deleted:
            continue
        for attr in 'binary pid starttime ppid host cputime utime stime vsz ' \
                    'rss uid suid euid username command gid sgid egid ' \
                    'sessionid'.split():
            assert entity.attrs.get(attr)
            if attr in ['cputime']:
                assert entity.attrs.get(attr).type == 'perf:counter'
            if attr in ['vsz', 'rss']:
                assert entity.attrs.get(attr).type == 'perf:gauge'

        count += 1
    assert count


def test_find_entity_with_pid(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    pid = os.getpid()
    entities = procent.entityd_find_entity('Process', {'pid': pid})
    proc = next(entities)
    assert proc.metype == 'Process'
    assert proc.attrs.get('pid').value == pid
    assert proc.attrs.get('starttime').value
    assert proc.attrs.get('ppid').value == os.getppid()

    with pytest.raises(StopIteration):
        next(entities)


def test_find_entity_with_unknown_attrs(procent):
    with pytest.raises(LookupError):
        procent.entityd_find_entity('Process', {'unknown': 1})


def test_find_entity_with_binary(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', {'binary': 'py.test'})
    proc = next(entities)
    assert proc.metype == 'Process'
    assert proc.attrs.get('binary').value == 'py.test'


def test_deleted_filtered(procent, session, kvstore, monkeypatch):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    update = entityd.entityupdate.EntityUpdate('Process', ueid=b'fedcba')
    update.attrs.set('binary', 'mybinary')
    deleted = entityd.entityupdate.EntityUpdate('Process', ueid=b'abcdef')
    deleted.delete()
    gen = (update for update in [update, deleted])
    monkeypatch.setattr(procent, 'processes', pytest.Mock(return_value=gen))
    assert list(procent.entityd_find_entity(
        'Process', {'binary': 'mybinary'})) == [update]


def test_get_ueid_new(kvstore, session, procent):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    ueid = procent.get_ueid(proc)
    assert ueid


def test_get_ueid_reuse(kvstore, session, procent):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc0 = syskit.Process(os.getpid())
    ueid0 = procent.get_ueid(proc0)
    proc1 = syskit.Process(os.getpid())
    ueid1 = procent.get_ueid(proc1)
    assert ueid0 == ueid1


def test_forget_entity(kvstore, session, procent):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    # Insert a process into known_ueids
    proc = syskit.Process(os.getpid())
    ueid = procent.get_ueid(proc)
    assert ueid in procent.known_ueids

    # Check it is removed
    entity = entityd.EntityUpdate('Process')
    entity.attrs.set('pid', proc.pid, attrtype='id')
    entity.attrs.set('starttime', proc.start_time.timestamp(), attrtype='id')
    entity.attrs.set('host', procent.host_ueid, attrtype='id')
    procent.forget_entity(entity)
    assert ueid not in procent.known_ueids


def test_forget_non_existent_entity(procent):
    # Should not raise an exception if a process is no longer there.
    assert not procent.known_ueids
    update = entityd.EntityUpdate('Process', ueid='non-existent-ueid')
    procent.forget_entity(update)
    assert not procent.known_ueids


def test_get_ueid(session):
    procent = entityd.processme.ProcessEntity()
    procent.session = session
    proc = syskit.Process(os.getpid())
    assert not procent.known_ueids
    ueid = procent.get_ueid(proc)
    assert ueid in procent.known_ueids
    ueid2 = next(procent.process(os.getpid())).ueid
    assert ueid == ueid2


def test_get_parents_nohost_noparent(session, kvstore, procent):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    rels = procent.get_parents(proc.pid, {proc.pid: proc})
    assert not rels


def test_get_parents_nohost_parent(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    pproc = syskit.Process(os.getppid())
    rels = procent.get_parents(proc.pid, {proc.pid: proc, pproc.pid: pproc})
    parent, = rels
    assert type(parent) == bytes


def test_get_parents_host_noparent(procent, session, kvstore, monkeypatch):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    hostupdate = entityd.EntityUpdate('Host')
    monkeypatch.setattr(session.pluginmanager.hooks,
                        'entityd_find_entity',
                        pytest.Mock(return_value=[[hostupdate]]))
    proc = syskit.Process(os.getpid())
    rels = procent.get_parents(proc.pid, {proc.pid: proc})
    host, = rels
    assert host == hostupdate.ueid


@pytest.fixture
def proctable():
    proc = syskit.Process(os.getpid())
    pproc = syskit.Process(os.getppid())
    return {proc.pid: proc, pproc.pid: pproc}


def test_processes(procent, proctable, monkeypatch, session, kvstore):  # pylint: disable=unused-argument
    # Wire up a fake process table
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(procent, 'update_process_table',
                        pytest.Mock(return_value=(proctable, {})))

    # Check we get MEs for the processes.
    gen = procent.processes()
    pids = []
    for me in gen:
        assert me.metype == 'Process'
        assert me.ueid
        if me.deleted:
            continue
        pids.append(me.attrs.get('pid').value)
    assert pids
    assert sorted(proctable.keys()) == sorted(pids)


def test_processes_deleted(procent, proctable, monkeypatch, session, kvstore):  # pylint: disable=unused-argument
    # Wire up a fake process table
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(procent, 'update_process_table',
                        pytest.Mock(return_value=(proctable, {})))
    this_process = proctable[os.getpid()]

    # Extract the ME for the py.test process
    gen = procent.processes()
    for me in gen:
        if me.deleted:
            continue
        if me.attrs.get('pid').value == os.getpid():
            procme = me

    # Delete py.test process from process table and check deleted ME
    del proctable[os.getpid()]
    monkeypatch.setattr(procent,
                        'update_process_table',
                        pytest.Mock(return_value=(proctable,
                                                  {os.getpid(): this_process})))
    gen = procent.processes()
    pprocme, delme = sorted(list(gen), key=lambda me: me.ueid == procme.ueid)
    assert pprocme.metype == 'Process'
    assert pprocme.attrs.get('pid').value == os.getppid()
    assert delme.metype == 'Process'
    assert delme.deleted is True
    assert delme.ueid == procme.ueid

    # Assert ueid is forgotten
    assert pprocme.ueid in procent.known_ueids
    assert delme.ueid not in procent.known_ueids


def test_update_process_table():
    active, deleted = entityd.processme.ProcessEntity.update_process_table({})
    assert active
    assert isinstance(deleted, dict)
    assert isinstance(active[os.getpid()], syskit.Process)
    pt2 = entityd.processme.ProcessEntity.update_process_table(active)[0]
    assert pt2[os.getpid()] is active[os.getpid()]


def test_process_table_vanished(monkeypatch):
    # A process vanishes during creation
    monkeypatch.setattr(syskit, 'Process',
                        pytest.Mock(side_effect=syskit.NoSuchProcessError))
    monkeypatch.setattr(syskit.Process, 'enumerate',
                        pytest.Mock(return_value=[42]))
    pt = entityd.processme.ProcessEntity.update_process_table({})[0]
    assert not pt


def test_process_table_vanished_refresh(monkeypatch):
    # A process vanishes during creation
    proc = syskit.Process(os.getpid())
    proc.refresh = pytest.Mock(side_effect=syskit.NoSuchProcessError)
    monkeypatch.setattr(syskit.Process, 'enumerate',
                        pytest.Mock(return_value=[os.getpid()]))
    pt = entityd.processme.ProcessEntity.update_process_table(
        {os.getpid(): proc})[0]
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

    def patch_syskit(arg):  # pylint: disable=unused-argument
        monkeypatch.setattr(syskit, 'Process',
                            pytest.Mock(side_effect=syskit.NoSuchProcessError))
        return proc

    monkeypatch.setattr(syskit, 'Process', pytest.Mock(
        side_effect=patch_syskit))

    entities = procent.entityd_find_entity('Process', {'pid': os.getpid()})
    proc = next(entities)
    assert proc.attrs.get('pid').value == os.getpid()
    # If the parent isn't present, we continue anyway, but it doesn't appear
    #  in relations.
    assert not proc.parents._relations
    with pytest.raises(StopIteration):
        proc = next(entities)


def test_previously_known_ueids_are_deleted_if_not_present(session, procent):
    kvstore = pytest.Mock()
    kvstore.getmany.return_value = {
        entityd.processme.ProcessEntity.prefix + 'made up ueid': 'made up ueid'
    }
    session.addservice('kvstore', kvstore)
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity(name='Process', attrs=None)
    count = 0
    for process in entities:
        if process.deleted and process.ueid == 'made up ueid':
            count += 1
    assert count == 1


def test_previously_known_ueids_are_not_deleted_if_present(session, procent):
    kvstore = pytest.Mock()
    kvstore_return = {}
    kvstore.getmany.return_value = kvstore_return
    session.addservice('kvstore', kvstore)
    procent.entityd_sessionstart(session)
    assert not procent.loaded_ueids
    proc_ueid = procent.get_ueid(syskit.Process(os.getpid()))

    kvstore_return[entityd.processme.ProcessEntity.prefix +
                   base64.b64encode(proc_ueid).decode('ascii')] = proc_ueid
    procent.entityd_sessionstart(session)
    assert procent.loaded_ueids

    entities = procent.entityd_find_entity(name='Process', attrs=None)
    process_found = False
    for process in entities:
        if process.ueid == proc_ueid:
            if process.deleted:
                pytest.fail('Should not be deleted.')
            else:
                process_found = True
                assert process.attrs.get('pid').value == os.getpid()
    assert process_found
    assert not procent.loaded_ueids


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
    entities = procent.entityd_find_entity('Process', {'pid':
                                                           zombie_process.pid})
    entity = next(entities)
    for attr in ['executable', 'args', 'argcount']:
        with pytest.raises(KeyError):
            assert entity.attrs.get(attr)


def test_cpu_usage_attr(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', {'pid': os.getpid()})
    entity = next(entities)
    cpu_usage = entity.attrs.get('cpu%')
    assert isinstance(cpu_usage.value, float)
    assert cpu_usage.type == 'perf:gauge'


def test_cpu_usage_calculation(procent):
    proc = pytest.Mock()
    proc.pid = 1
    proc.cputime = 0.0
    proc.start_time.timestamp.return_value = time.time()
    assert procent.get_cpu_usage(proc) == 0.0

    proc = pytest.Mock()
    proc.pid = 2
    proc.cputime = 1.0
    proc.start_time.timestamp.return_value = time.time() - 1
    assert procent.get_cpu_usage(proc) >= 99.0

    proc = pytest.Mock()
    proc.pid = 3
    proc.cputime = 1.0
    proc.start_time.timestamp.return_value = time.time() - 2
    assert 49.0 <= procent.get_cpu_usage(proc) <= 51.0

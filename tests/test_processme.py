import base64
import os

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
    assert kvstore.get(entityd.processme.ProcessEntity.prefix.encode('ascii') +
                       base64.b64encode(ueid)) == ueid

    # Check that entry is deleted from the kvstore
    procent.known_ueids.clear()
    procent.entityd_sessionfinish()
    with pytest.raises(KeyError):
        kvstore.get(
            entityd.processme.ProcessEntity.prefix.encode('ascii') +
            base64.b64encode(ueid))


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
    entity.attrs.set('start_time', proc.start_time, attrtype='id')
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
    monkeypatch.setattr(procent, 'process_table',
                        pytest.Mock(return_value=proctable))

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
    monkeypatch.setattr(procent, 'process_table',
                        pytest.Mock(return_value=proctable))

    # Extract the ME for the py.test process
    gen = procent.processes()
    for me in gen:
        if me.deleted:
            continue
        if me.attrs.get('pid').value == os.getpid():
            procme = me

    # Delete py.test process from process table and check deleted ME
    del proctable[os.getpid()]
    gen = procent.processes()
    pprocme, delme = list(gen)
    assert pprocme.metype == 'Process'
    assert pprocme.attrs.get('pid').value == os.getppid()
    assert delme.metype == 'Process'
    assert delme.deleted is True
    assert delme.ueid == procme.ueid

    # Assert ueid is forgotten
    assert pprocme.ueid in procent.known_ueids
    assert delme.ueid not in procent.known_ueids


def test_process_table():
    pt = entityd.processme.ProcessEntity.process_table()
    assert pt
    assert isinstance(pt[os.getpid()], syskit.Process)


def test_process_table_vanished(monkeypatch):
    # A process vanishes during creation
    monkeypatch.setattr(syskit, 'Process',
                        pytest.Mock(side_effect=syskit.NoSuchProcessError))
    monkeypatch.setattr(syskit.Process, 'enumerate',
                        pytest.Mock(return_value=[42]))
    pt = entityd.processme.ProcessEntity.process_table()
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

    def patch_syskit(pid):
        monkeypatch.setattr(syskit, 'Process',
                            pytest.Mock(side_effect=syskit.NoSuchProcessError))
        proc = pytest.Mock()
        proc.pid = pid
        proc.ppid = os.getppid()
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
    kvstore.getmany.return_value = {entityd.processme.ProcessEntity.prefix +
                                    'made up ueid':
                                        'made up ueid'}
    session.addservice('kvstore', kvstore)
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity(name='Process', attrs=None)
    for process in entities:
        if process.deleted and process.ueid == 'made up ueid':
            return
    pytest.fail('deleted ueid not found')

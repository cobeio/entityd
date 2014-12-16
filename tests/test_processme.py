import os

import syskit
import pytest

import entityd.hookspec
import entityd.processme


import entityd.core
import entityd.kvstore


@pytest.fixture
def config(pm):
    """An entityd.core.Config instance."""
    return entityd.core.Config(pm, [])


@pytest.fixture
def session(pm, config):
    """An entityd.core.Session instance."""
    return entityd.core.Session(pm, config)


@pytest.fixture
def kvstore(session):
    """Return a kvstore instance registered to the session fixture.

    This creates a KVStore and registers it to the ``session`` fixture.

    """
    kvstore = entityd.kvstore.KVStore(':memory:')
    session.addservice('kvstore', kvstore)
    return kvstore


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


def test_session_hooks_reload_proc(procent, session, kvstore):
    # Create an entry in known_ueids
    proc = syskit.Process(os.getpid())
    procent.entityd_sessionstart(session)
    key = procent._cache_key(proc.pid, proc.start_time.timestamp())
    ueid = procent.get_ueid(proc)
    assert procent.known_ueids[key] == ueid

    # Persist that entry to the kvstore
    procent.entityd_sessionfinish()
    assert kvstore.get(key) == ueid

    # Reload this entry from the kvstore
    procent.known_ueids.clear()
    procent.entityd_sessionstart(session)
    assert procent.known_ueids[key] == ueid


def test_sessionfinish_delete_ueids(procent, session, kvstore):
    # Create an entry in known_ueids
    proc = syskit.Process(os.getpid())
    procent.entityd_sessionstart(session)
    key = procent._cache_key(proc.pid, proc.start_time.timestamp())
    ueid = procent.get_ueid(proc)
    assert procent.known_ueids[key] == ueid

    # Persist that entry to the kvstore
    procent.entityd_sessionfinish()
    assert kvstore.get(key) == ueid

    # Check that entry is deleted from the kvstore
    procent.known_ueids.clear()
    procent.entityd_sessionfinish()
    with pytest.raises(KeyError):
        kvstore.get(key)


def test_configure(procent, config):
    procent.entityd_configure(config)
    assert config.entities['Process'].obj is procent


def test_find_entity(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    entities = procent.entityd_find_entity('Process', None)
    count = 0
    for entity in entities:
        assert entity.metype == 'Process'
        assert entity.attrs.getvalue('starttime')
        count += 1
    assert count


def test_find_entity_with_pid(procent, session, kvstore):
    procent.entityd_sessionstart(session)
    pid = os.getpid()
    entities = procent.entityd_find_entity('Process', {'pid': pid})
    proc = next(entities)
    assert proc.metype == 'Process'
    assert proc.attrs.getvalue('pid') == pid
    assert proc.attrs.getvalue('starttime')
    assert proc.attrs.getvalue('ppid') == os.getppid()

    with pytest.raises(StopIteration):
        next(entities)


def test_find_entity_with_unknown_attrs(procent):
    with pytest.raises(LookupError):
        procent.entityd_find_entity('Process', {'unknown': 1})


def test_cache_key():
    key = entityd.processme.ProcessEntity._cache_key(123, 456.7)
    assert key.startswith('entityd.processme:')


def test_cache_key_diff():
    key0 = entityd.processme.ProcessEntity._cache_key(1, 456.7)
    key1 = entityd.processme.ProcessEntity._cache_key(2, 456.7)
    assert key0 != key1


def test_get_ueid_new(kvstore, session, procent):
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    ueid = procent.get_ueid(proc)
    assert ueid


def test_get_ueid_reuse(kvstore, session, procent):
    procent.entityd_sessionstart(session)
    proc0 = syskit.Process(os.getpid())
    ueid0 = procent.get_ueid(proc0)
    proc1 = syskit.Process(os.getpid())
    ueid1 = procent.get_ueid(proc1)
    assert ueid0 == ueid1


def test_forget_entity(kvstore, session, procent):
    procent.entityd_sessionstart(session)
    # Insert a process into known_ueids
    proc = syskit.Process(os.getpid())
    key = procent._cache_key(proc.pid, proc.start_time.timestamp())
    procent.get_ueid(proc)
    assert key in procent.known_ueids

    # Check it is removed
    procent.forget_entity(proc.pid, proc.start_time.timestamp())
    assert key not in procent.known_ueids


def test_forget_non_existent_entity(procent):
    # Should not raise an exception if a process is no longer there.
    assert not procent.known_ueids
    procent.forget_entity(123, 123.123)
    assert not procent.known_ueids


def test_get_ueid(session):
    procent = entityd.processme.ProcessEntity()
    procent.session = session
    proc = syskit.Process(os.getpid())
    assert not procent.known_ueids
    ueid = procent.get_ueid(proc)
    proc_key = next(iter(procent.known_ueids.keys()))
    assert ueid == procent.known_ueids[proc_key]


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
        pids.append(me.attrs.getvalue('pid'))
    assert sorted(proctable.keys()) == sorted(pids)


def test_processes_deleted(procent, proctable, monkeypatch, session, kvstore):  # pylint: disable=unused-argument
    # Wire up a fake process table
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(procent, 'process_table',
                        pytest.Mock(return_value=proctable))

    # Extract the ME for the py.test process
    gen = procent.processes()
    for me in gen:
        if me.attrs.getvalue('pid') == os.getpid():
            procme = me

    # Delete py.test process from process table and check deleted ME
    del proctable[os.getpid()]
    gen = procent.processes()
    pprocme, delme = list(gen)
    assert pprocme.metype == 'Process'
    assert pprocme.attrs.getvalue('pid') == os.getppid()
    assert delme.metype == 'Process'
    assert delme.deleted is True
    assert delme.ueid == procme.ueid

    # Assert ueid is forgotten
    assert pprocme.ueid in procent.known_ueids.values()
    assert delme.ueid not in procent.known_ueids.values()


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

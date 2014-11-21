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
    # Create an entry in known_uuids
    proc = syskit.Process(os.getpid())
    procent.entityd_sessionstart(session)
    key = procent._cache_key(proc.pid, proc.start_time.timestamp())
    uuid = procent.get_uuid(proc)
    assert procent.known_uuids[key] == uuid

    # Persist that entry to the kvstore
    procent.entityd_sessionfinish()
    assert kvstore.get(key) == uuid

    # Reload this entry from the kvstore
    procent.known_uuids.clear()
    procent.entityd_sessionstart(session)
    assert procent.known_uuids[key] == uuid


def test_sessionfinish_delete_uuids(procent, session, kvstore):
    # Create an entry in known_uuids
    proc = syskit.Process(os.getpid())
    procent.entityd_sessionstart(session)
    key = procent._cache_key(proc.pid, proc.start_time.timestamp())
    uuid = procent.get_uuid(proc)
    assert procent.known_uuids[key] == uuid

    # Persist that entry to the kvstore
    procent.entityd_sessionfinish()
    assert kvstore.get(key) == uuid

    # Check that entry is deleted from the kvstore
    procent.known_uuids.clear()
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
        assert entity['type'] == 'Process'
        assert entity['attrs']['starttime']
        count += 1
    assert count


def test_find_entity_with_attrs(procent):
    with pytest.raises(LookupError):
        procent.entityd_find_entity('Process', {})


def test_cache_key():
    key = entityd.processme.ProcessEntity._cache_key(123, 456.7)
    assert key.startswith('entityd.processme:')


def test_cache_key_diff():
    key0 = entityd.processme.ProcessEntity._cache_key(1, 456.7)
    key1 = entityd.processme.ProcessEntity._cache_key(2, 456.7)
    assert key0 != key1


def test_get_uuid_new(procent):
    proc = syskit.Process(os.getpid())
    uuid = procent.get_uuid(proc)
    assert uuid


def test_get_uuid_reuse(procent):
    proc0 = syskit.Process(os.getpid())
    uuid0 = procent.get_uuid(proc0)
    proc1 = syskit.Process(os.getpid())
    uuid1 = procent.get_uuid(proc1)
    assert uuid0 == uuid1


def test_forget_entity(procent):
    # Insert a process into known_uuids
    proc = syskit.Process(os.getpid())
    key = procent._cache_key(proc.pid, proc.start_time.timestamp())
    procent.get_uuid(proc)
    assert key in procent.known_uuids

    # Check it is removed
    procent.forget_entity(proc.pid, proc.start_time.timestamp())
    assert key not in procent.known_uuids


def test_forget_non_existent_entity(procent):
    # Should not raise an exception if a process is no longer there.
    assert not procent.known_uuids
    procent.forget_entity(123, 123.123)
    assert not procent.known_uuids


def test_get_uuid():
    procent = entityd.processme.ProcessEntity()
    procent.session = session
    proc = syskit.Process(os.getpid())
    assert not procent.known_uuids
    uuid = procent.get_uuid(proc)
    proc_key = next(iter(procent.known_uuids.keys()))
    assert uuid == procent.known_uuids[proc_key]


def test_get_relations_nohost_noparent(session, kvstore, procent):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    rels = procent.get_relations(proc.pid, {proc.pid: proc})
    assert not rels


def test_get_relations_nohost_parent(procent, session, kvstore):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    proc = syskit.Process(os.getpid())
    pproc = syskit.Process(os.getppid())
    rels = procent.get_relations(proc.pid, {proc.pid: proc, pproc.pid: pproc})
    parent, = rels
    assert parent['uuid']
    assert parent['type'] == 'me:Process'
    assert parent['rel'] == 'parent'


def test_get_relations_host_noparent(procent, session, kvstore, monkeypatch):  # pylint: disable=unused-argument
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(session.pluginmanager.hooks,
                        'entityd_find_entity',
                        pytest.Mock(return_value=[[{'uuid': 'abcd'}]]))
    proc = syskit.Process(os.getpid())
    rels = procent.get_relations(proc.pid, {proc.pid: proc})
    host, = rels
    assert host['uuid'] == procent.host_uuid
    assert host['type'] == 'me:Host'
    assert host['rel'] == 'parent'


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
        assert me['type'] == 'Process'
        assert me['uuid']
        pids.append(me['attrs']['pid'])
    assert sorted(proctable.keys()) == sorted(pids)


def test_processes_deleted(procent, proctable, monkeypatch, session, kvstore):  # pylint: disable=unused-argument
    # Wire up a fake process table
    procent.entityd_sessionstart(session)
    monkeypatch.setattr(procent, 'process_table',
                        pytest.Mock(return_value=proctable))

    # Extract the ME for the py.test process
    gen = procent.processes()
    for me in gen:
        if me['attrs']['pid'] == os.getpid():
            procme = me

    # Delete py.test process from process table and check deleted ME
    del proctable[os.getpid()]
    gen = procent.processes()
    pprocme, delme = list(gen)
    assert pprocme['type'] == 'Process'
    assert pprocme['attrs']['pid'] == os.getppid()
    assert delme['type'] == 'Process'
    assert delme['delete'] is True
    assert delme['uuid'] == procme['uuid']

    # Assert uuid is forgotten
    assert pprocme['uuid'] in procent.known_uuids.values()
    assert delme['uuid'] not in procent.known_uuids.values()


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

import os
import socket
import uuid

import pytest
import syskit

import entityd.core
import entityd.endpointme
import entityd.kvstore
import entityd.processme


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
def endpoint_gen(pm):
    """A entityd.entityd.EndpointEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.

    """
    endpoint_gen = entityd.endpointme.EndpointEntity()
    pm.register(endpoint_gen, 'entityd.endpointme.EndpointEntity')
    return endpoint_gen


@pytest.fixture
def local_socket(request):
    # Connect a socket to look for
    local_ip = '127.0.0.1'
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    request.addfinalizer(s.close)
    s.bind((local_ip, 0))
    s.listen(1)
    return s


@pytest.fixture
def conn(local_socket):
    return list(syskit.Process(os.getpid()).connections)[0]


def test_plugin_registered(pm):
    name = 'entityd.endpointme'
    entityd.endpointme.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.endpointme.EndpointEntity')


def test_session_hooks_reload_proc(endpoint_gen, session, kvstore, conn):
    # Create an entry in known_uuids
    endpoint_gen.entityd_sessionstart(session)
    key = endpoint_gen._cache_key(os.getpid(), conn.fd)
    uuid = endpoint_gen.get_uuid(conn)
    assert endpoint_gen.known_uuids[key] == uuid

    # Persist that entry to the kvstore
    endpoint_gen.entityd_sessionfinish()
    assert kvstore.get(key) == uuid

    # Reload this entry from the kvstore
    endpoint_gen.known_uuids.clear()
    endpoint_gen.entityd_sessionstart(session)
    assert endpoint_gen.known_uuids[key] == uuid


def test_sessionfinish_delete_uuids(endpoint_gen, session, kvstore, conn):
    # Create an entry in known_uuids
    endpoint_gen.entityd_sessionstart(session)
    key = endpoint_gen._cache_key(conn.bound_pid, conn.fd)
    uuid = endpoint_gen.get_uuid(conn)
    assert endpoint_gen.known_uuids[key] == uuid

    # Persist that entry to the kvstore
    endpoint_gen.entityd_sessionfinish()
    assert kvstore.get(key) == uuid

    # Check that entry is deleted from the kvstore
    endpoint_gen.known_uuids.clear()
    endpoint_gen.entityd_sessionfinish()
    with pytest.raises(KeyError):
        kvstore.get(key)


def test_configure(endpoint_gen, config):
    endpoint_gen.entityd_configure(config)
    assert config.entities['Endpoint'].obj is endpoint_gen


def test_find_entity_with_attrs(endpoint_gen):
    with pytest.raises(LookupError):
        endpoint_gen.entityd_find_entity('Endpoint', {})


def test_cache_key():
    key = entityd.endpointme.EndpointEntity._cache_key(123, 456.7)
    assert key.startswith('entityd.endpointme:')


def test_cache_key_diff():
    key0 = entityd.endpointme.EndpointEntity._cache_key(1, 456.7)
    key1 = entityd.endpointme.EndpointEntity._cache_key(2, 456.7)
    assert key0 != key1


def test_forget_entity(endpoint_gen, conn):
    # Insert an Endpoint into known_uuids
    key = endpoint_gen._cache_key(conn.bound_pid, conn.fd)
    endpoint_gen.get_uuid(conn)
    assert key in endpoint_gen.known_uuids

    # Check it is removed
    endpoint_gen.forget_entity(conn.bound_pid, conn.fd)
    assert key not in endpoint_gen.known_uuids


def test_forget_non_existent_entity(endpoint_gen):
    # Should not raise an exception if an endpoint is no longer there.
    assert not endpoint_gen.known_uuids
    endpoint_gen.forget_entity(123, 123.123)
    assert not endpoint_gen.known_uuids


def test_endpoints_for_process(pm, session, kvstore, local_socket):
    _, socket_port = local_socket.getsockname()

    pm.register(entityd.processme.ProcessEntity(), name='entityd.processme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.processme')
    pm.hooks.entityd_sessionstart(session=session)

    endpoint_plugin = entityd.endpointme.EndpointEntity()
    endpoint_plugin.entityd_sessionstart(session)
    entities = endpoint_plugin.endpoints_for_process(os.getpid())

    endpoint = None
    for endpoint in entities:
        assert endpoint['type'] == 'Endpoint'
        assert 'uuid' in endpoint
        assert uuid.UUID(hex=endpoint['uuid']).hex == endpoint['uuid']
        assert 'attrs' in endpoint
        assert 'local_addr' in endpoint['attrs']
        ip, port = endpoint['attrs']['local_addr']
        assert ip == '127.0.0.1'
        assert port == socket_port

    if not endpoint:
        pytest.fail("No endpoints found")


def test_get_entities(request, pm, session, kvstore):
    endpoint_gen = entityd.endpointme.EndpointEntity()
    endpoint_gen.session = session

    pm.register(entityd.processme.ProcessEntity(), name='entityd.processme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.processme')
    pm.hooks.entityd_sessionstart(session=endpoint_gen.session)

    entities = endpoint_gen.entityd_find_entity(name='Endpoint', attrs=None)

    endpoint = None
    for endpoint in entities:
        assert endpoint['type'] == 'Endpoint'
        assert 'uuid' in endpoint
        assert uuid.UUID(hex=endpoint['uuid']).hex == endpoint['uuid']
        assert 'attrs' in endpoint
        assert 'local_addr' in endpoint['attrs']
        assert 'relations' in endpoint

    if endpoint is None:
        pytest.fail('No endpoints found')


def test_get_uuid_new(endpoint_gen, conn):
    uuid = endpoint_gen.get_uuid(conn)
    assert uuid


def test_get_uuid_reuse(endpoint_gen, local_socket):
    conn0 = list(syskit.Process(os.getpid()).connections)[0]
    uuid0 = endpoint_gen.get_uuid(conn0)
    conn1 = list(syskit.Process(os.getpid()).connections)[0]
    uuid1 = endpoint_gen.get_uuid(conn1)
    assert uuid0 == uuid1

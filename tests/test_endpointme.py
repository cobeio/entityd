import base64
import os
import socket

import pytest

import entityd.connections
import entityd.core
import entityd.endpointme
import entityd.kvstore
import entityd.processme


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
    """Create a connected socket which we can look for in the Endpoints"""
    local_ip = '127.0.0.1'
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    request.addfinalizer(s.close)
    s.bind((local_ip, 0))
    s.listen(1)
    return s


@pytest.fixture
def remote_socket(request, local_socket):
    """A socket connecting to local_socket"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    request.addfinalizer(s.close)
    s.connect(local_socket.getsockname())
    return s


@pytest.fixture
def unix_socket(request):
    """A unix socket"""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    request.addfinalizer(s.close)
    return s


@pytest.fixture
def conn(local_socket):  # pylint: disable=unused-argument
    """Get the Connection object corresponding to local_socket"""
    conns = entityd.connections.Connections()
    conn0 = conns.retrieve('all', os.getpid())[0]
    return conn0


def test_plugin_registered(pm):
    name = 'entityd.endpointme'
    entityd.endpointme.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.endpointme.EndpointEntity')


def test_session_hooks_reload_proc(endpoint_gen, session, kvstore, conn):  # pylint: disable=unused-argument
    # Create an entry in known_ueids
    endpoint_gen.entityd_sessionstart(session)
    ueid = endpoint_gen.get_ueid(conn)

    # Persist that entry to the kvstore
    endpoint_gen.entityd_sessionfinish()

    # Reload this entry from the kvstore
    endpoint_gen.known_ueids.clear()
    endpoint_gen.entityd_sessionstart(session)
    assert ueid in endpoint_gen.known_ueids


def test_sessionfinish_delete_ueids(endpoint_gen, session, kvstore, conn):
    # Create an entry in known_ueids
    endpoint_gen.entityd_sessionstart(session)
    ueid = endpoint_gen.get_ueid(conn)
    assert ueid in endpoint_gen.known_ueids

    # Persist that entry to the kvstore
    endpoint_gen.entityd_sessionfinish()
    assert kvstore.get(entityd.endpointme.EndpointEntity.prefix.encode(
        'ascii') + base64.b64encode(ueid)) == ueid

    # Check that entry is deleted from the kvstore
    endpoint_gen.known_ueids.clear()
    endpoint_gen.entityd_sessionfinish()
    with pytest.raises(KeyError):
        kvstore.get(entityd.endpointme.EndpointEntity.prefix.encode(
            'ascii') + ueid)


def test_configure(endpoint_gen, config):
    endpoint_gen.entityd_configure(config)
    assert config.entities['Endpoint'].obj is endpoint_gen


def test_find_entity_with_attrs(endpoint_gen):
    with pytest.raises(LookupError):
        endpoint_gen.entityd_find_entity('Endpoint', {})


def test_forget_entity(endpoint_gen, conn):
    # Insert an Endpoint into known_ueids
    ueid = endpoint_gen.get_ueid(conn)
    assert ueid in endpoint_gen.known_ueids

    # Check it is removed
    update = endpoint_gen.create_local_update(conn)
    endpoint_gen.forget_entity(update)
    assert ueid not in endpoint_gen.known_ueids


def test_forget_non_existent_entity(endpoint_gen):
    # Should not raise an exception if an endpoint is no longer there.
    assert not endpoint_gen.known_ueids
    update = entityd.EntityUpdate('Endpoint')
    update.attrs.set('addr', '0.0.0.0', 'id')
    update.attrs.set('port', 80, 'id')
    update.attrs.set('family', 2, 'id')
    update.attrs.set('protocol', 2, 'id')
    endpoint_gen.forget_entity(update)
    assert not endpoint_gen.known_ueids


def test_endpoints_for_process(pm, session, kvstore, local_socket,  # pylint: disable=unused-argument
                               remote_socket):
    # conn is required to keep the connection from being GC'd
    conn = local_socket.accept()  # pylint: disable=unused-variable

    pm.register(entityd.processme.ProcessEntity(),
                name='entityd.processme')
    endpoint_plugin = entityd.endpointme.EndpointEntity()
    pm.register(endpoint_plugin, name='entityd.endpointme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.processme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.endpointme')
    pm.hooks.entityd_sessionstart(session=session)

    entities = endpoint_plugin.endpoints_for_process(os.getpid())
    count = 0
    for count, endpoint in enumerate(entities, start=1):
        assert endpoint.metype == 'Endpoint'
        assert endpoint.ueid
        assert endpoint.attrs
        assert endpoint.attrs.get('family').value == 'INET'
        assert endpoint.attrs.get('protocol').value == 'TCP'
        addr = endpoint.attrs.get('addr').value
        port = endpoint.attrs.get('port').value
        if (addr, port) == local_socket.getsockname():
            # If it's the listening socket, we won't have a
            # remote address here. If it's accepted socket, then we do.
            if len(list(endpoint.children)):
                assert endpoint.attrs.get('listening').value is False
                update = entityd.EntityUpdate('Endpoint')
                update.attrs.set('addr', remote_socket.getsockname()[0],
                                 attrtype='id')
                update.attrs.set('port', remote_socket.getsockname()[1],
                                 attrtype='id')
                update.attrs.set('family', 'INET', attrtype='id')
                update.attrs.set('protocol',
                                 endpoint.attrs.get('protocol').value,
                                 attrtype='id')
                assert update.ueid in endpoint.children
                assert update.ueid in endpoint.parents
            else:
                assert endpoint.attrs.get('listening').value is True
        elif (addr, port) == remote_socket.getsockname():
            # This is the remote, connected socket
            assert endpoint.attrs.get('listening').value is False
            update = entityd.EntityUpdate('Endpoint')
            update.attrs.set('addr', local_socket.getsockname()[0],
                             attrtype='id')
            update.attrs.set('port', local_socket.getsockname()[1],
                             attrtype='id')
            update.attrs.set('family', 'INET', attrtype='id')
            update.attrs.set('protocol', endpoint.attrs.get('protocol').value,
                             attrtype='id')
            assert update.ueid in endpoint.children
            assert update.ueid in endpoint.parents
    assert count == 3


def test_unix_socket(pm, session, kvstore, unix_socket):  # pylint: disable=unused-argument
    """Unix sockets should not be returned"""
    pm.register(entityd.processme.ProcessEntity(),
                name='entityd.processme')
    endpoint_plugin = entityd.endpointme.EndpointEntity()
    pm.register(endpoint_plugin, name='entityd.endpointme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.processme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.endpointme')
    pm.hooks.entityd_sessionstart(session=session)

    entities = endpoint_plugin.endpoints_for_process(os.getpid())
    count = 0
    for endpoint in entities:
        count += 1
        assert endpoint.metype == 'Endpoint'
        assert endpoint.ueid
        print(endpoint.attrs.items())
    assert count == 0


def test_get_entities(pm, session, kvstore):  # pylint: disable=unused-argument
    endpoint_gen = entityd.endpointme.EndpointEntity()
    endpoint_gen.session = session

    pm.register(entityd.processme.ProcessEntity(), name='entityd.processme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.processme')
    pm.hooks.entityd_sessionstart(session=endpoint_gen.session)

    entities = endpoint_gen.entityd_find_entity(name='Endpoint', attrs=None)

    endpoint = None
    for endpoint in entities:
        assert endpoint.metype == 'Endpoint'
        assert endpoint.ueid
        for id_key in ['addr', 'port', 'family', 'protocol']:
            assert endpoint.attrs.get(id_key).type == 'id'

    if endpoint is None:
        pytest.fail('No endpoints found')


def test_get_deleted_entity(pm, session, kvstore, local_socket, conn):  # pylint: disable=unused-argument
    endpoint_gen = entityd.endpointme.EndpointEntity()
    endpoint_gen.session = session

    pm.register(entityd.processme.ProcessEntity(), name='entityd.processme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.processme')
    pm.hooks.entityd_sessionstart(session=endpoint_gen.session)

    # Get the current mes, so we have one to delete.
    list(endpoint_gen.entityd_find_entity(name='Endpoint', attrs=None))

    local_socket.close()
    # Now the socket is closed, we should get a 'delete' message
    deleted_ueids = [me.ueid for me in
                     endpoint_gen.entityd_find_entity(
                         name='Endpoint', attrs=None) if me.deleted]
    assert len(deleted_ueids)


def test_endpoint_for_deleted_process(pm, session,
                                      kvstore, local_socket, conn):  # pylint: disable=unused-argument
    endpoint_gen = entityd.endpointme.EndpointEntity()
    endpoint_gen.session = session

    pm.register(entityd.processme.ProcessEntity(), name='entityd.processme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.processme')
    pm.hooks.entityd_sessionstart(session=endpoint_gen.session)

    proc = entityd.EntityUpdate('Process')
    proc.delete()
    session.pluginmanager.hooks.entityd_find_entity = pytest.Mock()
    session.pluginmanager.hooks.entityd_find_entity.return_value = [[proc]]

    # If the process has been deleted, then the endpoint shouldn't be returned
    entities = endpoint_gen.endpoints_for_process(os.getpid())
    assert not list(entities)


def test_get_ueid_new(endpoint_gen, conn):
    ueid = endpoint_gen.get_ueid(conn)
    assert ueid


def test_get_ueid_reuse(endpoint_gen, local_socket):  # pylint: disable=unused-argument
    conns = entityd.connections.Connections()
    conn0 = conns.retrieve('all', os.getpid())[0]
    ueid0 = endpoint_gen.get_ueid(conn0)
    conns = entityd.connections.Connections()
    conn1 = conns.retrieve('all', os.getpid())[0]
    ueid1 = endpoint_gen.get_ueid(conn1)
    assert ueid0 == ueid1


def test_previously_known_ueids_are_deleted_if_not_present(session,
                                                           endpoint_gen):
    kvstore = pytest.Mock()
    kvstore.getmany.return_value = {'cache_key': 'made up ueid'}
    session.addservice('kvstore', kvstore)
    endpoint_gen.entityd_sessionstart(session)
    entities = endpoint_gen.entityd_find_entity(name='Endpoint', attrs=None)
    for endpoint in entities:
        if endpoint.deleted:
            assert endpoint.ueid == 'made up ueid'
            return
    pytest.fail('deleted ueid not found')

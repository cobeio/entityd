import os
import re
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


def test_configure(endpoint_gen, config):
    endpoint_gen.entityd_configure(config)
    assert config.entities['Endpoint'].obj is endpoint_gen


def test_find_entity_with_attrs(endpoint_gen):
    with pytest.raises(LookupError):
        endpoint_gen.entityd_find_entity('Endpoint', {})


def test_endpoints_for_process(pm, session, host_entity_plugin,  # pylint: disable=unused-argument
                               kvstore, local_socket, remote_socket):  # pylint: disable=unused-argument
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
                                 traits={'entity:id'})
                update.attrs.set('port', remote_socket.getsockname()[1],
                                 traits={'entity:id'})
                update.attrs.set('family', 'INET', traits={'entity:id'})
                update.attrs.set('protocol',
                                 endpoint.attrs.get('protocol').value,
                                 traits={'entity:id'})
                assert update.ueid in endpoint.children
                assert update.ueid in endpoint.parents
            else:
                assert endpoint.attrs.get('listening').value is True
        elif (addr, port) == remote_socket.getsockname():
            # This is the remote, connected socket
            assert endpoint.attrs.get('listening').value is False
            update = entityd.EntityUpdate('Endpoint')
            update.attrs.set('addr', local_socket.getsockname()[0],
                             traits={'entity:id'})
            update.attrs.set('port', local_socket.getsockname()[1],
                             traits={'entity:id'})
            update.attrs.set('family', 'INET', traits={'entity:id'})
            update.attrs.set('protocol', endpoint.attrs.get('protocol').value,
                             traits={'entity:id'})
            assert update.ueid in endpoint.children
            assert update.ueid in endpoint.parents
    assert count == 3
    pm.hooks.entityd_sessionfinish()


def test_unix_socket(pm, session, host_entity_plugin, kvstore, unix_socket):  # pylint: disable=unused-argument
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
        if endpoint.attrs.get('family').value == 'UNIX':
            count += 1
    assert count == 0
    pm.hooks.entityd_sessionfinish()


def test_get_entities(pm, session, host_entity_plugin, kvstore):  # pylint: disable=unused-argument
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
            assert endpoint.attrs.get(id_key).traits == {'entity:id'}

    if endpoint is None:
        pytest.fail('No endpoints found')
    pm.hooks.entityd_sessionfinish()


def test_endpoint_for_deleted_process(pm, session, host_entity_plugin,  # pylint: disable=unused-argument
                                      kvstore, local_socket, conn):  # pylint: disable=unused-argument
    endpoint_gen = entityd.endpointme.EndpointEntity()
    endpoint_gen.session = session

    pm.register(entityd.processme.ProcessEntity(), name='entityd.processme')
    pm.hooks.entityd_plugin_registered(pluginmanager=pm,
                                       name='entityd.processme')
    pm.hooks.entityd_sessionstart(session=endpoint_gen.session)

    proc = entityd.EntityUpdate('Process')
    proc.set_not_exists()
    session.pluginmanager.hooks.entityd_find_entity = pytest.Mock()
    session.pluginmanager.hooks.entityd_find_entity.return_value = [[proc]]

    # If the process no longer exists, then the endpoint shouldn't be returned
    entities = endpoint_gen.endpoints_for_process(os.getpid())
    assert not list(entities)
    pm.hooks.entityd_sessionfinish()


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


def test_entity_has_label(session, kvstore, endpoint_gen, local_socket):  # pylint: disable=unused-argument
    endpoint_gen.entityd_sessionstart(session)
    entities = endpoint_gen.entityd_find_entity(name='Endpoint', attrs=None)

    for entity in entities:
        label = entity.label
        match = re.match(r'([0-9a-fA-F:\.]*):\d+', label)
        if match.group(1).count(':') > 1:
            assert socket.inet_pton(socket.AF_INET6, match.group(1))
        else:
            assert socket.inet_pton(socket.AF_INET, match.group(1))


def test_multiple_fds():
    count = len(entityd.connections.Connections().retrieve('all', os.getpid()))
    s1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _ = socket.fromfd(s1.fileno(), socket.AF_INET, socket.SOCK_DGRAM)
    s1.bind(('127.0.0.1', 12345))
    conns = entityd.connections.Connections().retrieve('all', os.getpid())
    assert len(conns) == count + 1

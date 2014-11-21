import socket
import uuid
import os

import pytest
import entityd.core

import entityd.endpointme
import entityd.hostme
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


def test_endpoints_for_process(request):
    # Connect a socket to look for
    local_ip = '127.0.0.1'
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    request.addfinalizer(s.close)
    s.bind((local_ip, 0))
    _, socket_port = s.getsockname()
    s.listen(1)

    endpoint_plugin = entityd.endpointme.EndpointEntity()
    entities = endpoint_plugin.endpoints_for_process({
        'uuid': 123,
        'attrs': {
            'pid': os.getpid()
        }
    })

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
        assert 'relations' in endpoint and endpoint['relations']

    if endpoint is None:
        pytest.fail('No endpoints found')

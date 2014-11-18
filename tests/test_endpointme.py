import socket
import uuid
import os

import pytest
import entityd.core

import entityd.endpointme
import entityd.hostme
import entityd.processme


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


def test_get_entities(request, pm):
    endpoint_gen = entityd.endpointme.EndpointEntity()
    config = pm.hooks.entityd_cmdline_parse(
        pluginmanager=pm, argv=[])
    endpoint_gen.session = entityd.core.Session(pm, config)

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

import socket
import uuid
import os

import pytest
import entityd.core

import entityd.endpointme


def test_get_entity(request, pm):
    endpoint_gen = entityd.endpointme.EndpointEntity()
    config = pm.hooks.entityd_cmdline_parse(
        pluginmanager=pm, argv=[])
    endpoint_gen.session = entityd.core.Session(pm, config)

    # Connect a socket to look for
    local_ip = '127.0.0.1'
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    request.addfinalizer(s.close)
    s.bind((local_ip, 8899))
    s.listen(1)

    this_process_entity = pm.hooks.entityd_find_entity(
        name='Process', attrs={'pid': os.getpid()})
    print("Process ME: {}".format(this_process_entity))
    entities = pm.hooks.entityd_find_entity(name='Endpoint', attrs=None)

    for endpoint in entities:
        if (endpoint['relations'][0]['uuid'] !=
            this_process_entity['attrs']['uuid']):
            continue
        endpoint = entities[0]
        assert endpoint['type'] == 'Endpoint'
        assert 'uuid' in endpoint
        assert uuid.UUID(hex=endpoint['uuid']).hex == endpoint['uuid']
        assert 'attrs' in endpoint
        assert 'local_addr' in endpoint['attrs']
        ip, port = endpoint['attrs']['local_addr']
        assert ip == '127.0.0.1'
        assert port == 8899
    else:
        pytest.fail('No endpoints generated')

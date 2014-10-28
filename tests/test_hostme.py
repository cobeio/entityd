import sys

import entityd.core
import entityd.__main__
import entityd.pm


def test_generate_host_me():
    entityd.core.entityd_mainloop = entityd_mainloop
    entityd.__main__.main(sys.argv[1:])


@entityd.pm.hookimpl
def entityd_mainloop(session):
    (entity,), = session.pluginmanager.hooks.entityd_find_entity(
        name='Host', attrs=None)

    assert entity['type'] == 'Host'
    assert 'uuid' in entity
    assert 'timestamp' in entity

    assert 'uptime' in entity['attrs']
    assert 'fqdn' in entity['attrs']

    # test uuids remains the same
    (entity2,), = session.pluginmanager.hooks.entityd_find_entity(
        name='Host', attrs=None)

    assert entity['uuid'] == entity2['uuid']

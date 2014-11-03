import unittest

import entityd.hostme


def test_get_uuid():
    host_gen = entityd.hostme.HostEntity()
    host_gen.session = unittest.mock.Mock()
    # Disable actual sqlite database persistence
    host_gen.session.pluginmanager.hooks.entityd_kvstore_get.return_value \
        = None
    uuid = host_gen.get_uuid()

    host_gen.session.pluginmanager.hooks.entityd_kvstore_get \
        .assert_called_once()
    host_gen.session.pluginmanager.hooks.entityd_kvstore_put \
        .assert_called_once()

    assert uuid == host_gen.get_uuid()
    assert host_gen.host_uuid is not None


def test_get_entity():
    host_gen = entityd.hostme.HostEntity()
    host_gen.session = unittest.mock.Mock()
    # Disable actual sqlite database persistence
    host_gen.session.pluginmanager.hooks.entityd_kvstore_get.return_value \
        = None
    host_gen.session.pluginmanager.hooks.entityd_find_entity.return_value \
        = ({'uuid': unittest.mock.sentinel.uuid},),

    entities = list(host_gen.entityd_find_entity(name='Host', attrs=None))
    assert len(entities) == 1
    host = entities[0]
    assert host['type'] == 'Host'
    assert 'uuid' in host
    assert 'timestamp' in host
    assert 'attrs' in host

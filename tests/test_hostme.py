import pytest

import entityd.hookspec
import entityd.hostme


def test_plugin_registered(pm):
    name = 'entityd.hostme'
    entityd.hostme.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.hostme.HostEntity')


def test_configure():
    config = pytest.Mock()
    entityd.hostme.HostEntity().entityd_configure(config)
    assert config.addentity.called_once_with('Host',
                                             'entityd.hostme.HostEntity')


def test_session_stored_on_start():
    session = pytest.Mock()
    he = entityd.hostme.HostEntity()
    he.entityd_sessionstart(session)
    assert he.session is session


def test_find_entity_with_attrs():
    with pytest.raises(LookupError):
        entityd.hostme.HostEntity().entityd_find_entity('Host', {})


def test_get_uuid():
    host_gen = entityd.hostme.HostEntity()
    host_gen.session = pytest.Mock()
    # Disable actual sqlite database persistence
    host_gen.session.svc.kvstore.get.side_effect = KeyError
    uuid = host_gen.get_uuid()

    host_gen.session.pluginmanager.hooks.entityd_kvstore_get \
        .assert_called_once()
    host_gen.session.pluginmanager.hooks.entityd_kvstore_put \
        .assert_called_once()

    assert uuid == host_gen.get_uuid()
    assert host_gen.host_uuid is not None


def test_get_entity():
    host_gen = entityd.hostme.HostEntity()
    host_gen.session = pytest.Mock()
    # Disable actual sqlite database persistence
    host_gen.session.pluginmanager.hooks.entityd_kvstore_get.return_value \
        = None

    entities = list(host_gen.entityd_find_entity(name='Host', attrs=None))
    assert len(entities) == 1
    host = entities[0]
    assert host.metype == 'Host'
    assert host.ueid
    assert hasattr(host, 'timestamp')
    assert host.attrs

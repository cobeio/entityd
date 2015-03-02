import socket
import time

import pytest
import syskit

import entityd.hookspec
import entityd.hostme


@pytest.fixture
def host_gen():
    host_gen = entityd.hostme.HostEntity()
    host_gen.session = pytest.Mock()
    # Disable actual sqlite database persistence
    host_gen.session.svc.kvstore.get.side_effect = KeyError
    return host_gen


@pytest.fixture
def host(host_gen):
    entities = list(host_gen.entityd_find_entity(name='Host', attrs=None))
    return entities[0]


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


def test_get_uuid(host_gen):
    uuid = host_gen.get_uuid()

    host_gen.session.pluginmanager.hooks.entityd_kvstore_get \
        .assert_called_once()
    host_gen.session.pluginmanager.hooks.entityd_kvstore_put \
        .assert_called_once()

    assert uuid == host_gen.get_uuid()
    assert host_gen.host_uuid is not None


def test_metype(host):
    assert host.metype == 'Host'


def test_free(host):
    free = syskit.free()
    assert abs(host.attrs.get('free').value - free) < 2 ** 10


def test_cpu_usage(host_gen):
    entities = list(host_gen.entityd_find_entity(name='Host', attrs=None))
    host = entities[0]

    assert isinstance(host.attrs.get('usr').value, float)
    assert 99 < sum([host.attrs.get(key).value
                     for key in ['usr%', 'sys%', 'nice%', 'idle%', 'iowait%',
                                 'irq%', 'softirq%', 'steal%']]) <= 100.1
    time.sleep(.1)
    entities = list(host_gen.entityd_find_entity(name='Host', attrs=None))
    host = entities[0]
    assert 99 < sum([host.attrs.get(key).value
                     for key in ['usr%', 'sys%', 'nice%', 'idle%', 'iowait%',
                                 'irq%', 'softirq%', 'steal%']]) <= 100.1


def test_loadavg(host_gen):
    entities = list(host_gen.hosts())
    host = entities[0]
    loadavg = (host.attrs.get('loadavg_1').value,
               host.attrs.get('loadavg_5').value,
               host.attrs.get('loadavg_15').value)
    for value in loadavg:
        assert isinstance(value, float)
        assert 0 < value < 16


def test_entity_has_label():
    hostgen = entityd.hostme.HostEntity()
    hostgen.session = pytest.Mock()
    hostgen.session.pluginmanager.hooks.entityd_kvstore_get.return_value \
        = None

    entity = next(hostgen.entityd_find_entity(name='Host', attrs=None))
    assert entity.label == socket.getfqdn()


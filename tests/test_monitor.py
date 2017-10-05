import base64

import cobe
import pytest

import entityd.monitor


# pylint: disable=unused-argument


def test_sessionstart_entities_loaded(session, kvstore):
    kvstore.add('metypes', ['foo', 'bar', 'foo:bar'])
    kvstore.add('ueids:foo', 'a' * 32)
    kvstore.add('ueids:bar', 'b' * 32)
    kvstore.add('ueids:foo:bar', 'c' * 32)
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(session)
    assert monitor.last_batch == {
        'foo': {cobe.UEID('a' * 32)},
        'bar': {cobe.UEID('b' * 32)},
        'foo:bar': {cobe.UEID('c' * 32)},
    }


def test_sessionfinish_entities_saved(session, kvstore):
    ueid_a = cobe.UEID('a' * 32)
    ueid_b = cobe.UEID('b' * 32)
    ueid_c = cobe.UEID('c' * 32)
    ueid_a_b64 = base64.b64encode(str(ueid_a).encode()).decode()
    ueid_b_b64 = base64.b64encode(str(ueid_b).encode()).decode()
    ueid_c_b64 = base64.b64encode(str(ueid_c).encode()).decode()
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(session)
    monitor.last_batch = {
        'foo': {ueid_a},
        'bar': {ueid_b},
        'foo:bar': {ueid_c},
    }
    monitor.entityd_sessionfinish()
    assert sorted(kvstore.get('metypes')) == sorted(['foo', 'bar', 'foo:bar'])
    assert kvstore.get('ueids:foo:' + ueid_a_b64) == 'a' * 32
    assert kvstore.get('ueids:bar:' + ueid_b_b64) == 'b' * 32
    assert kvstore.get('ueids:foo:bar' + ueid_c_b64) == 'c' * 32


def test_collect_entities(pm, session, monitor, hookrec):
    update = entityd.entityupdate.EntityUpdate('foo')

    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            return [update]

    plugin = pm.register(FooPlugin(), 'foo')
    session.config.addentity('foo', plugin)
    session.svc.monitor.collect_entities()
    send_entity = dict(hookrec.calls)['entityd_send_entity']
    assert send_entity == {'session': session, 'entity': update}


def test_collect_entities_none_registered(session, monitor, hookrec):
    session.svc.monitor.collect_entities()
    calls = dict(hookrec.calls)
    assert 'entityd_send_entity' not in calls


def test_collect_entities_noent(pm, session, monitor, hookrec):
    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self):
            return []

    plugin = pm.register(FooPlugin(), 'foo')
    session.config.addentity('foo', plugin)
    session.svc.monitor.collect_entities()
    calls = dict(hookrec.calls)
    assert 'entityd_send_entity' not in calls


def test_collect_entities_deleted(pm, session, monitor, hookrec):
    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self):
            return []

    plugin = pm.register(FooPlugin(), 'foo')
    session.config.addentity('foo', plugin)
    session.svc.monitor.last_batch['foo'] = {cobe.UEID('a' * 32)}
    session.svc.monitor.collect_entities()
    send_entity = dict(hookrec.calls)['entityd_send_entity']
    assert not send_entity['entity'].exists
    assert send_entity['entity'].ueid == cobe.UEID('a' * 32)
    assert not any(session.svc.monitor.last_batch.values())


def test_collect_unregistered_type(pm, session, monitor, hookrec):
    session.svc.monitor.last_batch['foo'] = {cobe.UEID('a' * 32)}
    session.svc.monitor.collect_entities()
    send_entity = dict(hookrec.calls)['entityd_send_entity']
    assert not send_entity['entity'].exists
    assert send_entity['entity'].ueid == cobe.UEID('a' * 32)
    assert 'foo' not in session.svc.monitor.last_batch


def test_collect_multiple_entities(pm, session, monitor, hookrec):
    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            yield entityd.entityupdate.EntityUpdate(name)

    plugin1 = pm.register(FooPlugin(), 'foo1')
    plugin2 = pm.register(FooPlugin(), 'foo2')
    session.config.addentity('foo1', plugin1)
    session.config.addentity('foo2', plugin2)
    session.svc.monitor.collect_entities()
    assert hookrec.calls != []
    for call in hookrec.calls:
        if call[0] == 'entityd_find_entity':
            expected = call[1]['name']
        elif call[0] == 'entityd_send_entity':
            assert call[1]['entity'].metype == expected


def test_collect_ondemand_entities(pm, session, monitor, hookrec):
    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            yield entityd.entityupdate.EntityUpdate(name)
            yield entityd.entityupdate.EntityUpdate('ondemand')

    class OnDemandPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            return iter([])

    fooplugin = pm.register(FooPlugin(), 'foo')
    ondemandplugin = pm.register(OnDemandPlugin(), 'ondemand')
    session.config.addentity('foo', fooplugin)
    session.config.addentity('ondemand', ondemandplugin)
    session.svc.monitor.collect_entities()
    for call in hookrec.calls:
        if call[0] == 'entityd_find_entity':
            expected = call[1]['name']
        elif call[0] == 'entityd_send_entity':
            assert call[1]['entity'].metype == expected
            expected = 'ondemand'
    # assert len(session.svc.monitor.last_batch)
    assert len(session.svc.monitor.last_batch['ondemand'])

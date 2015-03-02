import base64

import pytest

import entityd.monitor


# pylint: disable=unused-argument


def test_sessionstart_entities_loaded():
    """Monitor will load entities listed in config.entities that have
    rows stored in the kvstore."""
    session = pytest.Mock()
    config = pytest.Mock()
    config.entities = ['foo']
    session.config = config
    session.svc.kvstore = pytest.Mock()
    session.svc.kvstore.getmany.return_value = {'_': b'ueid'}
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(session)
    assert monitor.session == session
    assert monitor.config == config
    assert b'ueid' in monitor.last_batch['foo']


def test_sessionfinish_entities_saved():
    """Monitor will save previously sent entities"""
    session = pytest.Mock()
    config = pytest.Mock()
    config.entities = ['foo']
    session.config = config
    session.svc.kvstore = pytest.Mock()
    session.svc.kvstore.getmany.return_value = {}
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(session)
    monitor.last_batch['foo'] = {b'ueid'}
    monitor.entityd_sessionfinish()
    session.svc.kvstore.deletemany.assert_called_once_with('ueids:foo:')
    session.svc.kvstore.addmany.assert_called_once_with({
        'ueids:foo:' + base64.b64encode(b'ueid').decode('ascii'): b'ueid'
    })


def test_collect_entities(pm, session, monitor, hookrec):
    update = entityd.entityupdate.EntityUpdate('foo')

    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs):
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
    session.svc.monitor.last_batch['foo'] = {b'ueid'}
    session.svc.monitor.collect_entities()
    send_entity = dict(hookrec.calls)['entityd_send_entity']
    assert send_entity['entity'].deleted
    assert send_entity['entity'].ueid == b'ueid'
    assert not any(session.svc.monitor.last_batch.values())


def test_collect_unregistered_type(pm, session, monitor, hookrec):
    session.svc.monitor.last_batch['foo'] = {b'ueid'}
    session.svc.monitor.collect_entities()
    send_entity = dict(hookrec.calls)['entityd_send_entity']
    assert send_entity['entity'].deleted
    assert send_entity['entity'].ueid == b'ueid'
    assert 'foo' not in session.svc.monitor.last_batch


def test_collect_multiple_entities(pm, session, monitor, hookrec):
    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs):
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

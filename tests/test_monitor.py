import base64

import cobe
import pytest

import entityd.monitor


# pylint: disable=unused-argument

@pytest.fixture
def mock_session():
    session = pytest.Mock()
    config = pytest.Mock()
    config.entities = ['foo']
    session.config = config
    session.svc.kvstore = pytest.Mock()
    session.svc.kvstore.get.return_value = []
    session.svc.kvstore.getmany.return_value = {}
    return session


def test_sessionstart_entities_loaded(mock_session):
    """Monitor will load entities listed in config.entities that have
    rows stored in the kvstore."""
    mock_session.svc.kvstore.getmany.return_value = {'_': 'a' * 32}
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(mock_session)
    assert monitor.session == mock_session
    assert monitor.config == mock_session.config
    assert cobe.UEID('a' * 32) in monitor.last_batch['foo']


def test_sessionfinish_entities_saved(mock_session):
    """Monitor will save previously sent entities"""
    mock_session.config.entities = ['foo']
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(mock_session)
    monitor.last_batch['foo'] = {cobe.UEID('a' * 32)}
    monitor.entityd_sessionfinish()
    mock_session.svc.kvstore.deletemany.assert_called_once_with('ueids:')
    mock_session.svc.kvstore.addmany.assert_called_once_with({
        'ueids:foo:' + base64.b64encode(b'a' * 32).decode(): 'a' * 32
    })


def test_sessionstart_types_loaded(mock_session):
    mock_session.svc.kvstore.get.return_value = ['foo', 'bar']
    mock_session.svc.kvstore.getmany.return_value = {'_': 'a' * 32}
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(mock_session)
    mock_session.svc.kvstore.get.assert_called_once_with('metypes')
    mock_session.svc.kvstore.getmany.assert_any_call('ueids:foo:')
    mock_session.svc.kvstore.getmany.assert_any_call('ueids:bar:')
    assert cobe.UEID('a' * 32) in monitor.last_batch['foo']
    assert cobe.UEID('a' * 32) in monitor.last_batch['bar']


def test_sessionfinish_types_saved(mock_session):
    monitor = entityd.monitor.Monitor()
    monitor.session = mock_session
    print(mock_session)
    monitor.last_batch['foo'].add(cobe.UEID('a' * 32))
    monitor.entityd_sessionfinish()
    mock_session.svc.kvstore.addmany.assert_called_once_with({
        'ueids:foo:' + base64.b64encode(b'a' * 32).decode(): 'a' * 32
    })


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

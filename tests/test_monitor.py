import cobe

import entityd
import entityd.monitor


# pylint: disable=unused-argument


def test_sessionstart_entities_loaded(session, kvstore):
    ueid_a = cobe.UEID('a' * 32)
    ueid_b = cobe.UEID('b' * 32)
    ueid_c = cobe.UEID('c' * 32)
    kvstore.add('metypes', ['foo', 'bar', 'foo:bar'])
    kvstore.add('ueids/foo/' + str(ueid_a), str(ueid_a))
    kvstore.add('ueids/bar/' + str(ueid_b), str(ueid_b))
    kvstore.add('ueids/foo:bar/' + str(ueid_c), str(ueid_c))
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
    monitor = entityd.monitor.Monitor()
    monitor.entityd_sessionstart(session)
    monitor.last_batch = {
        'foo': {ueid_a},
        'bar': {ueid_b},
        'foo:bar': {ueid_c},
    }
    monitor.entityd_sessionfinish()
    assert sorted(kvstore.get('metypes')) == sorted(['foo', 'bar', 'foo:bar'])
    assert kvstore.get('ueids/foo/' + str(ueid_a)) == 'a' * 32
    assert kvstore.get('ueids/bar/' + str(ueid_b)) == 'b' * 32
    assert kvstore.get('ueids/foo:bar/' + str(ueid_c)) == 'c' * 32


def test_collect_entities(pm, session, monitor, hookrec):
    update_1 = entityd.entityupdate.EntityUpdate('foo')
    update_2 = entityd.entityupdate.EntityUpdate('bar')

    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            return [update_1]

        @entityd.pm.hookimpl
        def entityd_emit_entities(self):
            yield update_2

    plugin = pm.register(FooPlugin(), 'foo')
    session.config.addentity('foo', plugin)
    session.svc.monitor.collect_entities()
    send_entity_calls = [call[1] for call
                         in hookrec.calls if call[0] == 'entityd_send_entity']
    assert len(send_entity_calls) == 2
    assert send_entity_calls[0] == {'session': session, 'entity': update_1}
    assert send_entity_calls[1] == {'session': session, 'entity': update_2}


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
    send_entity_calls = [call[1] for call
                         in hookrec.calls if call[0] == 'entityd_send_entity']
    assert {send_entity_calls[0]['entity'].metype,
            send_entity_calls[1]['entity'].metype} == {'foo1', 'foo2'}


def test_collect_ondemand_entities(pm, session, monitor, hookrec):
    class FooPlugin:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            yield entityd.entityupdate.EntityUpdate(name)
            if include_ondemand:
                yield entityd.entityupdate.EntityUpdate('ondemand')

    fooplugin = pm.register(FooPlugin(), 'foo')
    session.config.addentity('foo', fooplugin)
    session.svc.monitor.collect_entities()
    for call in hookrec.calls:
        if call[0] == 'entityd_find_entity':
            expected = call[1]['name']
        elif call[0] == 'entityd_send_entity':
            assert call[1]['entity'].metype == expected
            expected = 'ondemand'
    assert len(session.svc.monitor.last_batch['ondemand'])


def test_merge_updates(monkeypatch, monitor):
    update_1 = entityd.EntityUpdate('Foo')
    update_1.attrs.set('spam', 1)
    update_2 = entityd.EntityUpdate('Bar')
    update_2.attrs.set('spam', 2)
    update_3 = entityd.EntityUpdate('Foo')
    update_3.attrs.set('spam', 3)
    update_4 = entityd.EntityUpdate('Foo')
    update_4.attrs.set('spam', 4)
    update_5 = entityd.EntityUpdate('Baz')
    update_5.attrs.set('spam', 5)
    updates = [update_1, update_2, update_3, update_4, update_5]
    merged_1, merged_2, merged_3 = monitor._merge_updates(updates)
    assert merged_1.ueid == update_1.ueid
    assert merged_1.ueid == update_3.ueid
    assert merged_1.ueid == update_4.ueid
    assert merged_2.ueid == update_2.ueid
    assert merged_3.ueid == update_5.ueid
    assert merged_1.attrs.get('spam').value == 4

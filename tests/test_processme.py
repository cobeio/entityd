import unittest.mock

import entityd.processme


def test_get_uuid():
    process_gen = entityd.processme.ProcessEntity()
    process_gen.session = unittest.mock.Mock()
    # Disable actual sqlite database persistence
    process_gen.session.pluginmanager.hooks.entityd_kvstore_get.return_value\
        = None
    uuid = process_gen.get_uuid(123, 456.83373)

    process_gen.session.pluginmanager.hooks.entityd_kvstore_get \
        .assert_called_once()
    process_gen.session.pluginmanager.hooks.entityd_kvstore_put \
        .assert_called_once()

    assert uuid == process_gen.get_uuid(123, 456.83373)
    assert uuid != process_gen.get_uuid(1234, 456.83373)
    assert uuid != process_gen.get_uuid(123, 789.83373)
    assert len(process_gen.known_uuids) == 3


def test_get_processes():
    process_gen = entityd.processme.ProcessEntity()
    process_gen.session = unittest.mock.Mock()
    # Disable actual sqlite database persistence
    process_gen.session.pluginmanager.hooks.entityd_kvstore_get.return_value \
        = None
    process_gen.session.pluginmanager.hooks.entityd_find_entity.return_value \
        = ({'uuid': unittest.mock.sentinel.uuid},),

    entities = process_gen.entityd_find_entity(name='Process', attrs=None)
    for entity in entities:
        assert entity['type'] == 'Process'
        assert 'uuid' in entity
        assert 'timestamp' in entity
        assert 'delete' in entity or 'attrs' in entity
        assert 'relations' in entity or 'delete' in entity
        # Process can have multiple parents. Most processes will have a
        # parent process and the host.
        for rel in entity['relations']:
            assert rel['type'] in ['me:Host', 'me:Process']
            assert rel['rel'] == 'parent'
            if rel['type'] == 'me:Host':
                assert rel['uuid'] == unittest.mock.sentinel.uuid

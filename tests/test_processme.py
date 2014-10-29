import sys
import unittest.mock

import entityd.core
import entityd.__main__
import entityd.pm
import entityd.processme


def test_generate_process_me():
    entityd.core.entityd_mainloop = entityd_mainloop
    entityd.__main__.main(sys.argv[1:])


@entityd.pm.hookimpl
def entityd_mainloop(session):
    entities, = session.pluginmanager.hooks.entityd_find_entity(
        name='Process', attrs=None)
    for entity in entities:
        assert entity['type'] == 'Process'
        assert 'uuid' in entity
        assert 'timestamp' in entity
        assert 'delete' in entity or 'attrs' in entity
        assert 'relations' in entity or 'delete' in entity
        # Process should have a 'parent' relation. Either a parent process
        # or the host itself.
        rel = entity['relations'][0]
        assert rel['type'] in ['me:Host', 'me:Process']
        assert rel['rel'] == 'parent'


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
        # Process should have a 'parent' relation. Either a parent process
        # or the host itself.
        rel = entity['relations'][0]
        assert rel['type'] in ['me:Host', 'me:Process']
        assert rel['rel'] == 'parent'
        if rel['type'] == 'me:Host':
            assert rel['uuid'] == unittest.mock.sentinel.uuid
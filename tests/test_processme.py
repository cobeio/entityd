import sys

import entityd.core
import entityd.__main__
import entityd.pm


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

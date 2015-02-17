import argparse
import os

import pytest
import textwrap

import entityd.core
import entityd.hookspec
import entityd.kvstore
import entityd.declentity

# pylint: disable=unused-argument


@pytest.fixture
def mock_host(pm, session, config):
    hostent = entityd.EntityUpdate('Host')
    class MockHost:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs):
            if name == 'Host':
                yield hostent
    pm.register(MockHost(), 'entityd.hostme.MockHost')
    return hostent


@pytest.fixture
def declent(pm, mock_host, kvstore):
    """A entityd.declentity.DeclerativeEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    declent = entityd.declentity.DeclerativeEntity()
    pm.register(declent, 'entityd.declentity.DeclerativeEntity')
    return declent


def test_plugin_registered(pm):
    name = 'entityd.declentity'
    entityd.declentity.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.declentity.DeclerativeEntity')


def test_configure(declent, config):
    config.args.declentity_dir = '/service/conf/path'
    declent.entityd_configure(config)
    assert declent._path == '/service/conf/path'


def test_addoption():
    parser = argparse.ArgumentParser()
    entityd.declentity.entityd_addoption(parser)
    args = parser.parse_args(['--declentity-dir', '/file/path'])
    assert args.declentity_dir == '/file/path'


def test_load_files(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.conf')
    conf_file.write("""type: test""")
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    declent._load_files()
    assert 'test' in declent._conf_attrs.keys()
    assert config.entities['test'].obj is declent


def test_load_incorrect_file(declent, config, tmpdir, caplog):
    # A file with a load of rubbish in shouldn't crash entityd
    conf_file = tmpdir.join('test.conf')
    conf_file.write("""\xAA Rabbit, Foobar""")
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent._load_files()
    assert not declent._conf_attrs
    assert "Error loading" in caplog.text()


def test_load_invalid_file(declent, config, tmpdir, caplog):
    # Multidoc files can't have leading spaces
    conf_file = tmpdir.join('test.conf')
    conf_file.write("""
        ---
        type: Type
        ---
        type: File
        """)
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent._load_files()
    assert not declent._conf_attrs
    assert "Error loading" in caplog.text()


@pytest.fixture
def conf_file(tmpdir):
    conf_file = tmpdir.join('test.conf')
    conf_file.write("""
        type: Test
        attrs:
            owner: admin@default
            dict:
                value: 'a_value'
                type: 'a_type'
        children:
            - type: Process
              command: proccommand -a
            - type: File
              path: /path/to/file
        parents:
            - type: Process
              command: redis*
        """)
    return conf_file


def test_load_files_on_start(declent, config, session, conf_file):
    config.args.declentity_dir = os.path.split(conf_file.strpath)[0]
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Test' in declent._conf_attrs.keys()
    attrs = declent._conf_attrs['Test'][0]
    assert attrs['attrs']['owner'] == 'admin@default'
    assert attrs['attrs']['dict'] == {'value': 'a_value', 'type': 'a_type'}
    assert attrs['filepath'] == conf_file.strpath
    assert isinstance(attrs['children'][0], entityd.declentity.RelDesc)
    assert attrs['children'][0].type == 'Process'
    assert attrs['children'][0].attrs['command'] == 'proccommand -a'
    assert attrs['children'][1].type == 'File'
    assert attrs['children'][1].attrs['path'] == '/path/to/file'


def test_load_file_no_type(declent, config, session, tmpdir, caplog):
    conf_file = tmpdir.join('test.conf')
    conf_file.write("""
        attrs:
            owner: foobar
        """)
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert declent._conf_attrs == dict()
    assert 'No type field' in caplog.text()


def test_filename_attr_used(declent, config, session, tmpdir):
    conf_file = tmpdir.join('test.conf')
    conf_file.write("""
        type: test
        attrs:
            filepath: NotARealPath
        """)
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    ent = next(declent.entityd_find_entity('test', attrs=None))
    assert ent.attrs.get('filepath').value == 'NotARealPath'


def test_bad_file_suffix(declent, config, session, tmpdir):
    conf_file = tmpdir.join('test.other')
    conf_file.write("""
        type: Test
        attrs:
            owner: foobar
        """)
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Test' not in declent._conf_attrs.keys()


def test_invalid_type(declent, config, session, tmpdir, caplog):
    conf_file = tmpdir.join('test.conf')
    conf_file.write("""
        type: Invalid/Type
        """)
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Invalid/Type' not in declent._conf_attrs.keys()
    assert 'not allowed in type' in caplog.text()

def test_invalid_relation(declent, config, session, tmpdir, caplog):
    conf_file = tmpdir.join('test.conf')
    conf_file.write("""
        type: Test
        children:
            - - one
              - two
            - two
            - three
        """)
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Test' not in declent._conf_attrs.keys()
    assert 'Bad relation' in caplog.text()

def test_revalidate_relations(declent, session, config, conf_file):
    relations = [entityd.declentity.RelDesc('Test', {})]
    assert declent._validate_relations(relations) == relations

def test_deleted_on_file_remove(declent, session, config, conf_file):
    config.args.declentity_dir = os.path.split(conf_file.strpath)[0]
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    read_ent = next(declent.entityd_find_entity('Test', None))
    assert read_ent.attrs.get('owner').value == 'admin@default'
    assert read_ent.deleted is False
    declent.entityd_sessionfinish()
    declent._conf_attrs.clear()
    declent._deleted.clear()
    os.remove(conf_file.strpath)
    declent.entityd_sessionstart(session)
    assert read_ent.ueid in list(declent._deleted['Test'])
    deleted_ent = next(declent.entityd_find_entity('Test', None))
    assert deleted_ent.deleted is True
    assert read_ent.ueid == deleted_ent.ueid


def test_deleted_ueid_sent_on_get(declent, config, session, conf_file):
    config.args.declentity_dir = os.path.split(conf_file.strpath)[0]
    declent._deleted['Test'].add('DeletedUEID')
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    test_ents = declent.entityd_find_entity('Test', None)
    assert next(test_ents).ueid == 'DeletedUEID'
    assert next(test_ents).attrs.get('owner').value == 'admin@default'
    with pytest.raises(StopIteration):
        next(test_ents)


def test_find_deleted_with_attrs(declent, config, session):
    config.args.declentity_dir = '/tmp'
    declent._deleted['Test'].add('DeletedUEID')
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    with pytest.raises(LookupError):
        next(declent.entityd_find_entity('Test', 1))


@pytest.fixture
def conf_attrs():
    return {
        'type': 'testService',
        'filepath': 'testFilePath',
        'attrs': {
            'owner': 'testOwner',
            'a_dict':{
                'value': 'a_value',
                'type': 'a_type'
                },
            'type': 'attribute_called_type'
        },
        'children': [],
        'parents': []
    }


def test_create_decelarative_me(declent, conf_attrs, session):
    declent.entityd_sessionstart(session)
    entity = declent._create_declerative_entity(conf_attrs)
    assert entity.metype == 'testService'
    assert entity.attrs.get('owner').value == 'testOwner'
    assert entity.attrs.get('owner').type == None
    assert entity.attrs.get('host').value == declent.host_ueid
    assert entity.attrs.get('host').type == 'id'
    assert entity.attrs.get('filepath').value == 'testFilePath'
    assert entity.attrs.get('filepath').type == 'id'
    assert entity.attrs.get('a_dict').value == 'a_value'
    assert entity.attrs.get('a_dict').type == 'a_type'
    assert entity.attrs.get('type').value == 'attribute_called_type'


def test_find_entity(declent, conf_attrs, monkeypatch, session):
    declent.entityd_sessionstart(session)
    ent = entityd.entityupdate.EntityUpdate('Process')
    ent.attrs.set('command', 'procCommand')
    monkeypatch.setattr(session.pluginmanager.hooks,
                        'entityd_find_entity',
                        pytest.Mock(return_value=[[ent, ]]))
    entity = declent._find_entities('Process', {'command': 'procCommand'})
    assert next(entity) == ent


def test_find_entity_regex(declent, conf_attrs, monkeypatch, session):
    declent.entityd_sessionstart(session)
    ents = [entityd.entityupdate.EntityUpdate('Process') for _ in range(5)]
    for idx, ent in enumerate(ents):
        ent.attrs.set('command', 'proccmd{}'.format(idx))
    ents[-1].attrs.set('command', 'notthisone')
    monkeypatch.setattr(session.pluginmanager.hooks,
                        'entityd_find_entity',
                        pytest.Mock(return_value=[ents]))
    found_ents = declent._find_entities('Process', {'command': 'cmd[0-9]'})
    count = 0
    for entity in found_ents:
        assert entity in ents[:-1]
        count += 1
    assert count == 4


def test_find_entity_invalid_key(declent, session, monkeypatch):
    # Checking for a non-existant key shouldn't raise
    declent.entityd_sessionstart(session)
    ents = [entityd.entityupdate.EntityUpdate('Process') for _ in range(5)]
    ents[1].attrs.set('bacon', 'eggs')
    monkeypatch.setattr(session.pluginmanager.hooks,
                        'entityd_find_entity',
                        pytest.Mock(return_value=[ents]))
    found_entities = declent._find_entities('Process', {'bacon': 'eggs'})
    assert next(found_entities) == ents[1]
    with pytest.raises(StopIteration):
        next(found_entities)


def test_entityd_find_entity(declent, session, config, conf_file):
    config.args.declentity_dir = os.path.split(conf_file.strpath)[0]
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = session.pluginmanager.hooks.entityd_find_entity(
        name='Test', attrs=None)
    ent = next(found_ents[1])
    assert ent.attrs.get('owner').value == 'admin@default'
    assert ent.attrs.get('dict').value == 'a_value'
    assert ent.attrs.get('dict').type == 'a_type'

    found_ents = session.pluginmanager.hooks.entityd_find_entity(
        name='Procss', attrs={'pid': 1})
    for ent_gen in found_ents:
        with pytest.raises(StopIteration):
            next(ent_gen)


def test_entityd_find_entity_args(declent, session, config, conf_file):
    config.args.declentity_dir = os.path.split(conf_file.strpath)[0]
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = session.pluginmanager.hooks.entityd_find_entity(
        name='Test', attrs={'name': 'None'})
    with pytest.raises(LookupError):
        next(found_ents[1])


def test_children_and_parents(declent, session, config, conf_file, pm):
    procent = entityd.entityupdate.EntityUpdate('Process')
    procent.attrs.set('command', 'proccommand -a')
    procent2 = entityd.entityupdate.EntityUpdate('Process')
    procent2.attrs.set('command', 'redis-server')
    fileent = entityd.entityupdate.EntityUpdate('File')
    fileent.attrs.set('path', '/path/to/file')

    class MockFile:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs):
            if name == 'File':
                yield fileent
    pm.register(MockFile(), 'entityd.fileme.MockFile')

    class MockProc:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs):
            if name == 'Process':
                yield procent
                yield procent2
    pm.register(MockProc(), 'entityd.procme.MockProc')

    config.args.declentity_dir = os.path.split(conf_file.strpath)[0]
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)

    entity = declent._create_declerative_entity(declent._conf_attrs['Test'][0])
    assert set(entity.children) == set([procent.ueid, fileent.ueid])
    assert set(entity.parents) == set([declent._host_ueid, procent2.ueid])

    entity = declent._create_declerative_entity(declent._conf_attrs['Test'][0])
    assert set(entity.children) == set([procent.ueid, fileent.ueid])
    assert set(entity.parents) == set([declent._host_ueid, procent2.ueid])


def test_one_file_two_entities(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.conf')
    conf_file.write(textwrap.dedent("""
        ---
        type: Ent1
        attrs:
            owner: admin@default
        ---
        type: Ent2
        attrs:
            owner: admin2@default
        """))
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert set(['Ent1', 'Ent2']) == set(declent._conf_attrs.keys())
    assert declent


def test_relation_without_params(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.conf')
    conf_file.write(textwrap.dedent("""
        type: Test
        children:
            - type: File
        ---
        type: File
        attrs:
            path: /path/to/file
        """))
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = next(declent.entityd_find_entity('Test', None))
    child = list(found_ents.children)[0]
    expected = declent._create_declerative_entity(
        declent._conf_attrs['File'][0]).ueid
    assert child == expected


def test_relation_without_type(declent, session, config, tmpdir, caplog):
    conf_file = tmpdir.join('test.conf')
    conf_file.write(textwrap.dedent("""
        type: Test
        children:
            - path: /path/to/file
        ---
        type: File
        attrs:
            path: path/to/file
        """))
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    with pytest.raises(StopIteration):
        next(declent.entityd_find_entity('Test', None))
    assert "'type' is required" in caplog.text()


def test_recursive_relation(declent, session, config, tmpdir, caplog):
    conf_file = tmpdir.join('test.conf')
    conf_file.write(textwrap.dedent("""
        type: Test
        children:
            - type: Test
        parents:
            - type: Test
        """))
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = next(declent.entityd_find_entity('Test', None))
    ent_ueid = declent._create_declerative_entity(
        declent._conf_attrs['Test'][0]).ueid
    assert ent_ueid in found_ents.children
    assert ent_ueid in found_ents.parents


def test_overlapping_entity_names(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.conf')
    conf_file.write(textwrap.dedent("""
        type: Test
        attrs:
            owner: admin
            ident:
                value: 1
                type: id
        ---
        type: Test
        attrs:
            owner: user
            ident:
                value: 2
                type: id
        """))
    config.args.declentity_dir = tmpdir.strpath
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = declent.entityd_find_entity('Test', None)
    owners = set(ent.attrs.get('owner').value for ent in found_ents)
    assert owners == set(['admin', 'user'])

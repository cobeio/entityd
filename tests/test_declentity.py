import argparse
import os
import pathlib
import re
import textwrap
import time

import pytest

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
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            if name == 'Host':
                yield hostent
    pm.register(MockHost(), 'entityd.hostme.MockHost')
    return hostent


@pytest.fixture
def declent(pm, mock_host, kvstore):
    """A entityd.declentity.DeclarativeEntity instance.

    The plugin will be registered with the PluginManager but no hooks
    will have been called.
    """
    declent = entityd.declentity.DeclarativeEntity()
    pm.register(declent, 'entityd.declentity.DeclarativeEntity')
    return declent


def test_configure(declent, config):
    config.args.declentity_dir = '/service/conf/path'
    declent.entityd_configure(config)
    assert declent._path == '/service/conf/path'


def test_addoption():
    parser = argparse.ArgumentParser()
    entityd.declentity.DeclarativeEntity.entityd_addoption(parser)
    args = parser.parse_args(['--declentity-dir', '/file/path'])
    assert args.declentity_dir == pathlib.Path('/file/path')


def test_default_dir():
    parser = argparse.ArgumentParser()
    entityd.declentity.DeclarativeEntity.entityd_addoption(parser)
    args = parser.parse_args([])
    assert args.declentity_dir.stem == 'entity_declarations'


def test_load_files(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""type: test""")
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    declent._update_entities()
    assert 'test' in declent._conf_attrs.keys()
    assert config.entities['test'].obj is declent


def test_load_incorrect_file(declent, config, tmpdir, loghandler):
    # A file with a load of rubbish in shouldn't crash entityd
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""AA Rabbit, Foobar""")
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent._update_entities()
    assert not declent._conf_attrs
    assert loghandler.has_warning(re.compile(r'Error loading'))


def test_load_invalid_file(declent, config, tmpdir, loghandler):
    # Multidoc files can't have leading spaces
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""
        ---
        type: Type
        ---
        type: File
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent._update_entities()
    assert not declent._conf_attrs
    assert loghandler.has_warning(re.compile(r'Could not load'))


def test_load_no_permission(declent, session, config, tmpdir, loghandler):
    conf_file = tmpdir.join('test.entity')
    conf_file.write('type: Test')
    pathlib.Path(conf_file.strpath).chmod(0x000)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Test' not in declent._conf_attrs.keys()
    assert loghandler.has_warning(re.compile(r'Could not open'))


@pytest.fixture
def conf_file(tmpdir):
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""
        type: Test
        attrs:
            owner: admin@default
            dict:
                value: 'a_value'
                traits:
                    - 'a_trait'
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
    config.args.declentity_dir = pathlib.Path(conf_file.strpath).parent
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Test' in declent._conf_attrs.keys()
    declcfg = declent._conf_attrs['Test'][0]
    assert declcfg.attrs['owner'].value == 'admin@default'
    assert declcfg.attrs['dict'].value == 'a_value'
    assert declcfg.attrs['dict'].traits == ['a_trait']
    assert declcfg.filepath == pathlib.Path(conf_file.strpath)
    assert isinstance(declcfg.children[0], entityd.declentity.RelDesc)
    assert declcfg.children[0].type == 'Process'
    assert declcfg.children[0].attrs['command'] == 'proccommand -a'
    assert declcfg.children[1].type == 'File'
    assert declcfg.children[1].attrs['path'] == '/path/to/file'


def test_load_file_no_type(declent, config, session, tmpdir, loghandler):
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""
        attrs:
            owner: foobar
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert declent._conf_attrs == dict()
    assert loghandler.has_warning(re.compile(r'No type field'))


def test_filepath_attr_used(declent, config, session, tmpdir):
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""
        type: test
        attrs:
            filepath: NotARealPath
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    declcfg = declent._conf_attrs['test'][0]
    assert declcfg.filepath == pathlib.Path(conf_file.strpath)
    assert declcfg.attrs['filepath'].value == 'NotARealPath'
    ent = next(declent.entityd_find_entity('test', attrs=None))
    assert ent.attrs.get('filepath').value == 'NotARealPath'


def test_bad_file_suffix(declent, config, session, tmpdir):
    conf_file = tmpdir.join('test.other')
    conf_file.write("""
        type: Test
        attrs:
            owner: foobar
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Test' not in declent._conf_attrs.keys()


def test_invalid_type(declent, config, session, tmpdir, loghandler):
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""
        type: Invalid/Type
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Invalid/Type' not in declent._conf_attrs.keys()
    assert loghandler.has_warning(re.compile(r'not allowed in type'))


def test_invalid_relation(declent, config, session, tmpdir, loghandler):
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""
        type: Test
        children:
            - - one
              - two
            - two
            - three
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert 'Test' not in declent._conf_attrs.keys()
    assert loghandler.has_warning(re.compile(r'Bad relation'))


def test_entity_removed_on_file_remove(declent, session, config, conf_file):
    config.args.declentity_dir = pathlib.Path(conf_file.strpath).parent
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    read_ent = next(declent.entityd_find_entity('Test', None))
    assert read_ent.attrs.get('owner').value == 'admin@default'
    assert read_ent.exists is True
    os.remove(conf_file.strpath)
    session.config.removeentity = pytest.Mock()
    with pytest.raises(StopIteration):
        _ = next(declent.entityd_find_entity('Test', None))
    session.config.removeentity.assert_called_once_with(
        'Test', 'entityd.declentity.DeclarativeEntity')


@pytest.fixture
def conf_attrs():
    conf = {
        'type': 'testService',
        'filepath': 'testFilePath',
        'attrs': {
            'owner': 'testOwner',
            'a_dict': {
                'value': 'a_value',
                'traits': ['a_trait']
            },
            'type': 'attribute_called_type'
        },
        'children': [],
        'parents': []
    }
    return entityd.declentity.DeclCfg(conf)


def test_create_declarative_me(declent, conf_attrs, session):
    declent.entityd_sessionstart(session)
    entity = declent._create_declarative_entity(conf_attrs)
    assert entity.metype == 'testService'
    assert entity.label == 'testService'
    assert entity.attrs.get('owner').value == 'testOwner'
    assert entity.attrs.get('owner').traits == set()
    assert entity.attrs.get('hostueid').value == str(declent.host_ueid)
    assert entity.attrs.get('hostueid').traits == {'entity:id', 'entity:ueid'}
    assert entity.attrs.get('filepath').value == 'testFilePath'
    assert entity.attrs.get('filepath').traits == {'entity:id'}
    assert entity.attrs.get('a_dict').value == 'a_value'
    assert entity.attrs.get('a_dict').traits == {'a_trait'}
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
    config.args.declentity_dir = pathlib.Path(conf_file.strpath).parent
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = session.pluginmanager.hooks.entityd_find_entity(
        name='Test', attrs=None)
    ent = next(found_ents[1])
    assert ent.attrs.get('owner').value == 'admin@default'
    assert ent.attrs.get('dict').value == 'a_value'
    assert ent.attrs.get('dict').traits == {'a_trait'}

    found_ents = session.pluginmanager.hooks.entityd_find_entity(
        name='Procss', attrs={'pid': 1})
    for ent_gen in found_ents:
        with pytest.raises(StopIteration):
            next(ent_gen)


def test_entityd_find_entity_args(declent, session, config, conf_file):
    config.args.declentity_dir = pathlib.Path(conf_file.strpath).parent
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
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            if name == 'File':
                yield fileent
    pm.register(MockFile(), 'entityd.fileme.MockFile')

    class MockProc:
        @entityd.pm.hookimpl
        def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
            if name == 'Process':
                yield procent
                yield procent2
    pm.register(MockProc(), 'entityd.procme.MockProc')

    config.args.declentity_dir = pathlib.Path(conf_file.strpath).parent
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)

    entity = declent._create_declarative_entity(declent._conf_attrs['Test'][0])
    assert set(entity.children) == set([procent.ueid, fileent.ueid])
    assert set(entity.parents) == set([procent2.ueid])

    entity = declent._create_declarative_entity(declent._conf_attrs['Test'][0])
    assert set(entity.children) == set([procent.ueid, fileent.ueid])
    assert set(entity.parents) == set([procent2.ueid])


def test_one_file_two_entities(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.entity')
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
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    assert set(['Ent1', 'Ent2']) == set(declent._conf_attrs.keys())
    assert declent


def test_relation_without_params(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.entity')
    conf_file.write(textwrap.dedent("""
        type: Test
        children:
            - type: File
        ---
        type: File
        attrs:
            path: /path/to/file
        """))
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = next(declent.entityd_find_entity('Test', None))
    child = list(found_ents.children)[0]
    expected = declent._create_declarative_entity(
        declent._conf_attrs['File'][0]).ueid
    assert child == expected


def test_relation_without_type(declent, session, config, tmpdir, loghandler):
    conf_file = tmpdir.join('test.entity')
    conf_file.write(textwrap.dedent("""
        type: Test
        children:
            - path: /path/to/file
        ---
        type: File
        attrs:
            path: path/to/file
        """))
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    with pytest.raises(StopIteration):
        next(declent.entityd_find_entity('Test', None))
    assert loghandler.has_warning(re.compile(r"'type' is required"))


def test_recursive_relation(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.entity')
    conf_file.write(textwrap.dedent("""
        type: Test
        children:
            - type: Test
        parents:
            - type: Test
        """))
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = next(declent.entityd_find_entity('Test', None))
    ent_ueid = declent._create_declarative_entity(
        declent._conf_attrs['Test'][0]).ueid
    assert ent_ueid in found_ents.children
    assert ent_ueid in found_ents.parents


def test_overlapping_entity_names(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.entity')
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
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = declent.entityd_find_entity('Test', None)
    owners = set(ent.attrs.get('owner').value for ent in found_ents)
    assert owners == set(['admin', 'user'])


def test_change_entity(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""
        type: Test
        attrs:
            owner: my_owner
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    found_ents = list(declent.entityd_find_entity('Test', None))
    assert found_ents[0].attrs.get('owner').value == 'my_owner'
    with pytest.raises(KeyError):
        found_ents[0].attrs.get('ident')
    conf_file.write("""
        type: Test
        attrs:
            owner: my_owner
            ident:
                value: 2
                type: id
        """)
    stat = os.stat(conf_file.strpath)
    os.utime(conf_file.strpath, times=(stat.st_atime+10, stat.st_mtime+10))
    declent._update_entities()
    found_ents = list(declent.entityd_find_entity('Test', None))
    assert found_ents[0].attrs.get('owner').value == 'my_owner'
    assert found_ents[0].attrs.get('ident').value == 2


def test_remove_type(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test.entity')
    conf_file.write("""
        type: Test
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    entity = next(declent.entityd_find_entity('Test', None))
    assert entity.metype == 'Test'
    conf_file.write("""
        """)
    stat = os.stat(conf_file.strpath)
    os.utime(conf_file.strpath, times=(stat.st_atime+10, stat.st_mtime+10))
    declent._update_entities()
    with pytest.raises(StopIteration):
        _ = next(declent.entityd_find_entity('Test', None))
    assert 'Test' not in declent._conf_attrs.keys()


def test_remove_file(declent, session, config, tmpdir):
    conf_file = tmpdir.join('test2.entity')
    conf_file.write("""
        type: Test
        """)
    config.args.declentity_dir = pathlib.Path(tmpdir.strpath)
    declent.entityd_configure(config)
    declent.entityd_sessionstart(session)
    entity = next(declent.entityd_find_entity('Test', None))
    assert entity.metype == 'Test'
    os.remove(conf_file.strpath)
    while pathlib.Path(conf_file.strpath).exists():
        time.sleep(1)
    with pytest.raises(StopIteration):
        _ = next(declent.entityd_find_entity('Test', None))
    assert 'Test' not in declent._conf_attrs.keys()


class TestDecCfg:

    def test_blank_default_label(self):
        some_cfg = entityd.declentity.DeclCfg({'type': 'SomeType'})
        assert some_cfg.type == 'SomeType'
        assert some_cfg.filepath == ''
        assert some_cfg.parents == []
        assert some_cfg.children == []
        assert some_cfg.attrs == {}
        assert some_cfg.label == 'SomeType'

    def test_custom_label(self):
        some_cfg = entityd.declentity.DeclCfg({
            'type': 'SomeType',
            'label': 'Custom Label',
            'attrs': {}
        })
        assert some_cfg.type == 'SomeType'
        assert some_cfg.filepath == ''
        assert some_cfg.parents == []
        assert some_cfg.children == []
        assert some_cfg.label == 'Custom Label'

    def test_invalid_type(self):
        with pytest.raises(entityd.declentity.ValidationError):
            _ = entityd.declentity.DeclCfg({'type': 'Some/Type'})

    def test_no_type(self):
        with pytest.raises(entityd.declentity.ValidationError):
            entityd.declentity.DeclCfg({})

    @pytest.fixture
    def data(self):
        return {
            'type': 'TestType',
            'filepath': '/this/is/filepath'
        }

    def test_properties(self, data):
        test_cfg = entityd.declentity.DeclCfg(data)
        assert test_cfg.filepath == '/this/is/filepath'

    def test_attrs(self, data):
        data['attrs'] = {'owner': 'my_owner'}
        test_cfg = entityd.declentity.DeclCfg(data)
        assert test_cfg.attrs['owner'].value == 'my_owner'

    @pytest.mark.parametrize('traits', [
        set(),
        {'entity:id'},
        {'metric:counter', 'time:posix'}
    ])
    def test_attr_traits(self, data, traits):
        data['attrs'] = {'ident': {'value': 1, 'traits': traits}}
        test_cfg = entityd.declentity.DeclCfg(data)
        assert test_cfg.attrs['ident'].value == 1
        assert test_cfg.attrs['ident'].traits == traits

    def test_relation(self, data):
        data['children'] = [{'type': 'EntType'}]
        test_cfg = entityd.declentity.DeclCfg(data)
        ent = entityd.declentity.RelDesc('EntType', {})
        assert test_cfg.children[0] == ent

    def test_relation_attrs(self, data):
        data['children'] = [{'type': 'child', 'command': 'cmd'}]
        test_cfg = entityd.declentity.DeclCfg(data)
        rel = entityd.declentity.RelDesc('child', {'command': 'cmd'})
        assert test_cfg.children[0] == rel

    def test_relation_no_type(self, data):
        data['parents'] = [{'value': 'foo'}]
        with pytest.raises(entityd.declentity.ValidationError):
            entityd.declentity.DeclCfg(data)

    def test_relation_not_dict(self, data):
        data['children'] = ['Not a dictionary']
        with pytest.raises(entityd.declentity.ValidationError):
            entityd.declentity.DeclCfg(data)

    def test_rel_desc_relation(self, data):
        data['children'] = [entityd.declentity.RelDesc('Rel', {})]
        test_cfg = entityd.declentity.DeclCfg(data)
        assert test_cfg.children[0] == entityd.declentity.RelDesc('Rel', {})

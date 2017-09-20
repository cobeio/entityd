import argparse
import pathlib

import cobe
import pytest

import entityd.entityupdate
import entityd.dot


class TestPalette:

    def test(self):
        colours = ['#000000', '#ffffff']
        palette = entityd.dot._Palette(colours)
        assert palette['foo'] in colours

    def test_normalise_case(self):
        palette = entityd.dot._Palette(['#ffFFff'])
        assert palette['foo'] == '#ffffff'

    def test_empty(self):
        with pytest.raises(ValueError):
            entityd.dot._Palette([])

    def test_default(self):
        palette = entityd.dot._Palette.default()
        assert isinstance(palette, entityd.dot._Palette)

    @pytest.mark.parametrize('colour', ['#ffffff', '#ffFFff'])
    def test_exclude(self, colour):
        palette = entityd.dot._Palette(['#000000', '#ffffff'])
        assert palette['spam'] == '#ffffff'
        assert palette['eggs'] == '#000000'
        palette_filtered = palette.exclude(colour)
        assert palette_filtered['spam'] == '#000000'
        assert palette_filtered['eggs'] == '#000000'

    def test_exclude_empty(self):
        palette = entityd.dot._Palette(['#000000'])
        with pytest.raises(ValueError):
            palette.exclude('#000000')


@pytest.mark.parametrize('foreign_entity', [
    entityd.dot._ForeignEntity.DEFAULT,
    entityd.dot._ForeignEntity.UEID,
    entityd.dot._ForeignEntity.UEID_SHORT,
    entityd.dot._ForeignEntity.EXCLUDE,
])
def test_foreign_entity(foreign_entity):
    assert str(foreign_entity) == foreign_entity.value


class TestCommandLineOptions:

    @pytest.fixture
    def parser(self):
        parser = argparse.ArgumentParser()
        entityd.dot.entityd_addoption(parser)
        return parser

    def test_dot_default(self, parser):
        arguments = parser.parse_args([])
        assert arguments.dot is None

    def test_dot(self, tmpdir, parser):
        arguments = parser.parse_args(['--dot', str(tmpdir)])
        assert arguments.dot == pathlib.Path(str(tmpdir))

    def test_dot_foreign_default(self, parser):
        arguments = parser.parse_args([])
        assert arguments.dot_foreign is entityd.dot._ForeignEntity.DEFAULT

    @pytest.mark.parametrize('foreign_entity', [
        entityd.dot._ForeignEntity.DEFAULT,
        entityd.dot._ForeignEntity.UEID,
        entityd.dot._ForeignEntity.UEID_SHORT,
        entityd.dot._ForeignEntity.EXCLUDE,
    ])
    def test_dot_foreign(self, parser, foreign_entity):
        arguments = parser.parse_args(['--dot-foreign', foreign_entity.value])
        assert arguments.dot_foreign is foreign_entity

    def test_dot_foreign_bad_choice(self, parser):
        with pytest.raises(SystemExit) as exception:
            arguments = parser.parse_args(['--dot-foreign', 'boris'])
        assert 'invalid _ForeignEntity value' in str(exception.value.__context__)

    def test_dot_pretty_default(self, parser):
        arguments = parser.parse_args([])
        assert arguments.dot_pretty is False

    def test_dot_pretty(self, parser):
        arguments = parser.parse_args(['--dot-pretty'])
        assert arguments.dot_pretty is True


class TestForeignReferences:

    def test_parent(self):
        entity = entityd.entityupdate.EntityUpdate('Foo')
        relationship = (cobe.UEID('a' * 32), entity.ueid)
        aliens = entityd.dot._foreign_references(
            {entity.ueid: entity}, {relationship})
        assert list(aliens) == [(relationship, relationship[0])]

    def test_child(self):
        entity = entityd.entityupdate.EntityUpdate('Foo')
        relationship = (entity.ueid, cobe.UEID('a' * 32))
        aliens = entityd.dot._foreign_references(
            {entity.ueid: entity}, {relationship})
        assert list(aliens) == [(relationship, relationship[1])]

    def test_both(self):
        relationship = (cobe.UEID('a' * 32), cobe.UEID('b' * 32))
        aliens = entityd.dot._foreign_references({}, {relationship})
        assert list(aliens) == [
            (relationship, relationship[0]),
            (relationship, relationship[1]),
        ]

    def test_neither(self):
        entity_a = entityd.entityupdate.EntityUpdate('Foo')
        entity_b = entityd.entityupdate.EntityUpdate('Bar')
        relationship = (entity_a.ueid, entity_b.ueid)
        aliens = entityd.dot._foreign_references({
            entity_a.ueid: entity_a,
            entity_b.ueid: entity_b,
        }, {relationship})
        assert list(aliens) == []


class TestProcessForeignReferences:

    def test_default(self):
        entity = entityd.entityupdate.EntityUpdate('Foo')
        relationship = (cobe.UEID('a' * 32), entity.ueid)
        entities = {entity.ueid: entity}
        relationships = {relationship}
        entityd.dot._process_foreign_references(
            entityd.dot._ForeignEntity.DEFAULT, entities, relationships)
        assert set(entities.keys()) == {entity.ueid, relationship[0]}
        assert entities[relationship[0]].metype == ''
        assert entities[relationship[0]].label == '?'
        assert relationships == {relationship}

    def test_exclude(self):
        entity = entityd.entityupdate.EntityUpdate('Foo')
        relationship = (cobe.UEID('a' * 32), entity.ueid)
        entities = {entity.ueid: entity}
        relationships = {relationship}
        entityd.dot._process_foreign_references(
            entityd.dot._ForeignEntity.EXCLUDE, entities, relationships)
        assert set(entities.keys()) == {entity.ueid}
        assert relationships == set()

    def test_ueid(self):
        entity = entityd.entityupdate.EntityUpdate('Foo')
        relationship = (cobe.UEID('a' * 32), entity.ueid)
        entities = {entity.ueid: entity}
        relationships = {relationship}
        entityd.dot._process_foreign_references(
            entityd.dot._ForeignEntity.UEID, entities, relationships)
        assert set(entities.keys()) == {entity.ueid, relationship[0]}
        assert entities[relationship[0]].metype == ''
        assert entities[relationship[0]].label == 'a' * 32
        assert relationships == {relationship}

    def test_ueid_short(self):
        entity = entityd.entityupdate.EntityUpdate('Foo')
        relationship = (cobe.UEID('a' * 32), entity.ueid)
        entities = {entity.ueid: entity}
        relationships = {relationship}
        entityd.dot._process_foreign_references(
            entityd.dot._ForeignEntity.UEID_SHORT, entities, relationships)
        assert set(entities.keys()) == {entity.ueid, relationship[0]}
        assert entities[relationship[0]].metype == ''
        assert entities[relationship[0]].label == 'aaaaaa'
        assert relationships == {relationship}


def test_pretty_print():
    chunks = [
        'foo {',
        'spam',
        'eggs',
        '}',
    ]
    assert list(entityd.dot._pretty_print(chunks)) == [
        'foo {\n',
        '  spam\n',
        '  eggs\n',
        '}\n',
    ]


class TestWrite:

    def test(self, tmpdir):
        path = pathlib.Path(str(tmpdir)) / 'test.dot'
        entityd.dot._write_dot(path, set(), set())
        assert path.is_file()
        with path.open() as dot_file:
            dot = dot_file.read()
        assert dot.startswith('digraph G {')
        assert dot.endswith('}')

    def test_write_pretty(self, tmpdir):
        path = pathlib.Path(str(tmpdir)) / 'test.dot'
        entityd.dot._write_dot(path, set(), set(), pretty=True)
        assert path.is_file()
        with path.open() as dot_file:
            dot = dot_file.read()
        assert dot.splitlines()[0] == 'digraph G {'
        assert dot.splitlines()[-1] == '}'


def test_write_header():
    assert list(entityd.dot._write_dot_header()) == [
        'digraph G {',
        'graph [overlap=prism];',
        'graph [rankdir=LR];',
        'graph [splines=true];',
        'graph [bgcolor="#ffffff"];',
        'node [color=white];',
        'node [fillcolor=white];',
        'node [shape=box];',
        'node [style=filled];',
        'edge [arrowhead=open];',
    ]


def test_write_footer():
    assert list(entityd.dot._write_dot_footer()) == ['}']


def test_write_relationships():
    relationships = {
        (cobe.UEID('a' * 32), cobe.UEID('b' * 32)),
        (cobe.UEID('b' * 32), cobe.UEID('c' * 32)),
        (cobe.UEID('c' * 32), cobe.UEID('a' * 32)),
    }
    assert list(entityd.dot._write_dot_relationships(relationships)) == [
        ('"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" '
         '-> "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";'),
        ('"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" '
         '-> "cccccccccccccccccccccccccccccccc";'),
        ('"cccccccccccccccccccccccccccccccc" '
         '-> "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";'),
    ]


class TestWriteEntities:

    def test(self):
        entity_a = entityd.entityupdate.EntityUpdate('Foo')
        entity_a.label = 'spam'
        entity_b = entityd.entityupdate.EntityUpdate('Bar')
        entity_b.label = 'eggs'
        entities = {entity_a, entity_b}
        assert list(entityd.dot._write_dot_entities(entities)) == [
            ('"31f596aa85f36577720cf361cd8715d1" '
             '[color="#f29c9c", fillcolor="#ffffff", label="Bar\\neggs"];'),
            ('"716eec5f78bfa9b97ff69ccda90c7f7a" '
             '[color="#ea8279", fillcolor="#ffffff", label="Foo\\nspam"];'),
        ]

    def test_label_empty(self):
        entity = entityd.entityupdate.EntityUpdate('Foo')
        entities = {entity}
        assert list(entityd.dot._write_dot_entities(entities)) == [
            ('"716eec5f78bfa9b97ff69ccda90c7f7a" '
             '[color="#ea8279", fillcolor="#ffffff", label="Foo"];'),
        ]

    def test_label_empty_type(self):
        entity = entityd.entityupdate.EntityUpdate('')
        entity.label = 'spam'
        entities = {entity}
        assert list(entityd.dot._write_dot_entities(entities)) == [
            ('"833499c9a74ffdd7fa55be6bc113b6e4" '
             '[color="#ea8279", fillcolor="#ffffff", label="spam"];'),
        ]

    def test_color_type_namespace(self):
        entity_a = entityd.entityupdate.EntityUpdate('Test:Foo')
        entity_b = entityd.entityupdate.EntityUpdate('Test:Bar')
        entities = {entity_a, entity_b}
        assert list(entityd.dot._write_dot_entities(entities)) == [
            ('"f594fcfab86ff40e3eba23bed9d223b3" '
             '[color="#8850a4", fillcolor="#c2f488", label="Test:Bar"];'),
            ('"f980732eaf5c4f4105e52fadde79e563" '
             '[color="#8850a4", fillcolor="#ea8279", label="Test:Foo"];'),
        ]


class TestCollectionAfter:

    @pytest.fixture(params=list(entityd.dot._ForeignEntity))
    def foreign_entity(self, request):
        return request.param

    @pytest.fixture(params=[True, False])
    def pretty(self, request):
        return request.param

    def test(self, monkeypatch, tmpdir, session, foreign_entity, pretty):
        monkeypatch.setattr(
            entityd.dot,
            '_write_dot',
            pytest.Mock(wraps=entityd.dot._write_dot),
        )
        monkeypatch.setattr(
            entityd.dot,
            '_process_foreign_references',
            pytest.Mock(wraps=entityd.dot._process_foreign_references),
        )
        path = pathlib.Path(str(tmpdir)) / 'test.dot'
        session.config.args.dot = path
        session.config.args.dot_foreign = foreign_entity
        session.config.args.dot_pretty = pretty
        entity_a = entityd.entityupdate.EntityUpdate('Foo')
        entity_b = entityd.entityupdate.EntityUpdate('Bar')
        entity_c = entityd.entityupdate.EntityUpdate('Baz')
        entity_a.children.add(entity_b)
        entity_a.parents.add(entity_c)
        entityd.dot.entityd_collection_after(
            session, (entity_a, entity_b, entity_c))
        assert path.is_file()
        with path.open() as dot_file:
            dot = dot_file.read()
        assert ('"3d9873001ef496294b3d7f5930b32cac" '
                '-> "716eec5f78bfa9b97ff69ccda90c7f7a"' in dot)
        assert ('"716eec5f78bfa9b97ff69ccda90c7f7a" '
                '-> "31f596aa85f36577720cf361cd8715d1"' in dot)
        assert entityd.dot._write_dot.call_args[0] == (
                path,
                {entity_a, entity_b, entity_c},
                {
                    (entity_a.ueid, entity_b.ueid),
                    (entity_c.ueid, entity_a.ueid),
                },
        )
        assert entityd.dot._write_dot.call_args[1] == {'pretty': pretty}
        assert entityd.dot._process_foreign_references.call_args[0] == (
            foreign_entity,
            {
                entity_a.ueid: entity_a,
                entity_b.ueid: entity_b,
                entity_c.ueid: entity_c,
            },
            {
                (entity_a.ueid, entity_b.ueid),
                (entity_c.ueid, entity_a.ueid),
            },
        )
        assert entityd.dot._process_foreign_references.call_args[1] == {}

    def test_disabled(self, monkeypatch, session):
        monkeypatch.setattr(
            entityd.dot,
            '_write_dot',
            pytest.Mock(wraps=entityd.dot._write_dot),
        )
        monkeypatch.setattr(
            entityd.dot,
            '_process_foreign_references',
            pytest.Mock(wraps=entityd.dot._process_foreign_references),
        )
        session.config.args.dot = None
        entity_a = entityd.entityupdate.EntityUpdate('Foo')
        entity_b = entityd.entityupdate.EntityUpdate('Bar')
        entity_c = entityd.entityupdate.EntityUpdate('Baz')
        entity_a.children.add(entity_b)
        entity_a.parents.add(entity_c)
        entityd.dot.entityd_collection_after(
            session, (entity_a, entity_b, entity_c))
        assert not entityd.dot._write_dot.called
        assert not entityd.dot._process_foreign_references.called

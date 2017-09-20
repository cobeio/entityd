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
    entityd.dot.ForeignEntity.DEFAULT,
    entityd.dot.ForeignEntity.UEID,
    entityd.dot.ForeignEntity.UEID_SHORT,
    entityd.dot.ForeignEntity.EXCLUDE,
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
        assert arguments.dot_foreign is entityd.dot.ForeignEntity.DEFAULT

    @pytest.mark.parametrize('foreign_entity', [
        entityd.dot.ForeignEntity.DEFAULT,
        entityd.dot.ForeignEntity.UEID,
        entityd.dot.ForeignEntity.UEID_SHORT,
        entityd.dot.ForeignEntity.EXCLUDE,
    ])
    def test_dot_foreign(self, parser, foreign_entity):
        arguments = parser.parse_args(['--dot-foreign', foreign_entity.value])
        assert arguments.dot_foreign is foreign_entity

    def test_dot_foreign_bad_choice(self, parser):
        with pytest.raises(SystemExit) as exception:
            arguments = parser.parse_args(['--dot-foreign', 'boris'])
        assert 'invalid ForeignEntity value' in str(exception.value.__context__)


def test_write(tmpdir):
    path = pathlib.Path(str(tmpdir)) / 'test.dot'
    entityd.dot._write_dot(path, set(), set())
    assert path.is_file()
    with path.open() as dot_file:
        dot = dot_file.read()
    assert dot.startswith('digraph G {')
    assert dot.endswith('}')


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

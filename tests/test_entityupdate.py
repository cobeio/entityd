import time

import cobe
import pytest

import entityd
import entityd.entityupdate


@pytest.fixture
def update():
    return entityd.EntityUpdate('Type')


def test_metype(update):
    assert update.metype == 'Type'


def test_timestamp():
    st = time.time()
    update = entityd.EntityUpdate('Time')
    et = time.time()
    assert st < update.timestamp < et


def test_ttl():
    update = entityd.EntityUpdate('Type')
    assert update.ttl == 120


def test_set_ttl():
    update = entityd.EntityUpdate('Type', ttl=60)
    assert update.ttl == 60


def test_children(update):
    ueid = cobe.UEID('a' * 32)
    assert not list(update.children)
    update.children.add(ueid)
    assert ueid in update.children
    update.children.add(update)
    assert update.ueid in update.children


def test_parents(update):
    ueid = cobe.UEID('a' * 32)
    assert not list(update.parents)
    update.parents.add(ueid)
    assert ueid in update.parents
    update.parents.add(update)
    assert update.ueid in update.parents


def test_set_not_exists(update):
    assert update.exists
    update.set_not_exists()
    assert not update.exists


def test_ueid():
    u1 = entityd.EntityUpdate('Type')
    u2 = entityd.EntityUpdate('Type')
    assert isinstance(u1.ueid, cobe.UEID)
    assert isinstance(u2.ueid, cobe.UEID)
    assert u1.ueid == u2.ueid
    u1.attrs.set('notAnId', 'aeue')
    assert u1.ueid == u2.ueid
    u1.attrs.set('id', 'aeaea', {'entity:id'})
    assert u1.ueid != u2.ueid


@pytest.mark.parametrize('ueid', ['a' * 32, cobe.UEID('a' * 32)])
def test_ueid_explicit(ueid):
    update = entityd.EntityUpdate('Type')
    assert update.ueid == cobe.UEID('3a2fc5ca145e8e8f5d3c3d94e6300c2d')
    update.ueid = ueid
    assert isinstance(update.ueid, cobe.UEID)
    assert update.ueid == cobe.UEID(ueid)
    update.attrs.set('id', 'snowflare', traits={'entity:id'})
    assert update.ueid == cobe.UEID(ueid)


@pytest.mark.parametrize('ueid', [
    'a' * 31,
    'a' * 33,
    'x' * 32,
])
def test_ueid_explicit_error(ueid):
    update = entityd.EntityUpdate('Type')
    with pytest.raises(cobe.UEIDError):
        update.ueid = ueid


@pytest.mark.parametrize('value', [None, 0, 0.0, '', b'', [], {}])
def test_attrs(update, value):
    update.attrs.set('key', value, traits=set())
    assert update.attrs.get('key').value == value
    assert update.attrs.get('key').traits == set()
    update.attrs.delete('key')
    with pytest.raises(KeyError):
        update.attrs.get('key')
    assert 'key' in update.attrs.deleted()


def test_attrs_set_clear(update):
    update.attrs.set('key', None, traits=set())
    assert len(list(update.attrs)) == 1
    update.attrs.clear('key')
    assert len(list(update.attrs)) == 0


def test_attrs_delete_clear(update):
    update.attrs.delete('key')
    assert len(update.attrs.deleted()) == 1
    update.attrs.clear('key')
    assert len(update.attrs.deleted()) == 0


def test_attr_delete_nonexistent(update):
    update.attrs.delete('NA')
    assert 'NA' in update.attrs.deleted()


def test_explicit_ueid():
    ueid = 'a' * 32
    update = entityd.EntityUpdate('Endpoint', ueid=ueid)
    assert isinstance(update.ueid, cobe.UEID)
    assert update.ueid == cobe.UEID('a' * 32)


def test_explicit_ueid_too_short():
    with pytest.raises(cobe.UEIDError):
        entityd.EntityUpdate('Foo', ueid='aaa')


@pytest.mark.parametrize('ueid', [object(), b'a' * 32])
def test_explicit_ueid_type_type(ueid):
    # Test a bytestring as previously it was considered an acceptable type.
    with pytest.raises(cobe.UEIDError):
        entityd.EntityUpdate('Foo', ueid=ueid)


def test_explicit_ueid_non_hex_character():
    with pytest.raises(cobe.UEIDError):
        entityd.EntityUpdate('Foo', ueid='z' * 32)


def test_create_deleted_from_ueid():
    ueid = 'a' * 32
    update = entityd.EntityUpdate('Endpoint', ueid=ueid)
    update.set_not_exists()
    assert not update.exists
    assert isinstance(update.ueid, cobe.UEID)
    assert update.ueid == cobe.UEID(ueid)


@pytest.mark.parametrize(('literal', 'string'), [
    (None, 'None'),
    (0, '0'),
    (0.0, '0.0'),
    (b'foo', "b'foo'"),
    ([], '[]'),
    ({}, '{}'),
])
def test_value_string_conversion(literal, string):
    update_literal = entityd.EntityUpdate('Foo')
    update_literal.attrs.set('bar', literal, {'entity:id'})
    update_string = entityd.EntityUpdate('Foo')
    update_string.attrs.set('bar', string, {'entity:id'})
    assert update_literal.ueid != update_string.ueid


@pytest.mark.parametrize('object_', [object(), ()])
def test_ueid_wrong_type(object_):
    # Test a tuple as previously it was considered an acceptable type.
    update = entityd.EntityUpdate('Foo')
    update.attrs.set('bar', object_, {'entity:id'})
    with pytest.raises(cobe.UEIDError):
        assert update.ueid


class TestUpdateRelations:

    @pytest.mark.parametrize('entity', [
        cobe.UEID('a' * 32),
        entityd.EntityUpdate('Foo', ueid='a' * 32),
    ])
    def test_add(self, entity):
        relations = entityd.entityupdate.UpdateRelations()
        relations.add(entity)
        assert list(relations) == [cobe.UEID('a' * 32)]

    @pytest.mark.parametrize('entity', [
        cobe.UEID('a' * 32),
        entityd.EntityUpdate('Foo', ueid='a' * 32),
    ])
    def test_add_duplicate(self, entity):
        relations = entityd.entityupdate.UpdateRelations()
        relations.add(entity)
        relations.add(entity)
        assert list(relations) == [cobe.UEID('a' * 32)]

    def test_add_wrong_type(self):
        relations = entityd.entityupdate.UpdateRelations()
        with pytest.raises(ValueError):
            relations.add('a' * 32)

    def test_contains(self):
        relations = entityd.entityupdate.UpdateRelations()
        relations.add(cobe.UEID('a' * 32))
        assert cobe.UEID('a' * 32) in relations

    def test_contains_update_operand(self):
        relations = entityd.entityupdate.UpdateRelations()
        relations.add(cobe.UEID('a' * 32))
        assert entityd.EntityUpdate('Foo', 'a' * 32) not in relations

    def test_contains_string_operand(self):
        relations = entityd.entityupdate.UpdateRelations()
        relations.add(entityd.EntityUpdate('Foo', 'a' * 32))
        assert 'a' * 32 not in relations

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


def test_attrs_delete_set(update):
    update.attrs.delete('key')
    assert len(list(update.attrs)) == 0
    assert len(update.attrs.deleted()) == 1
    update.attrs.set('key', 'value')
    assert len(list(update.attrs)) == 1
    assert len(update.attrs.deleted()) == 0
    assert update.attrs.get('key').value == 'value'
    assert update.attrs.get('key').traits == set()


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


class TestMerge:

    relations = pytest.mark.parametrize('relations', ['parents', 'children'])

    def test_ueid_mismatch(self):
        old = entityd.EntityUpdate('Foo')
        new = entityd.EntityUpdate('Bar')
        assert old.ueid != new.ueid  # sanity check
        with pytest.raises(ValueError):
            old.merge(new)

    def test_properties(self):
        old = entityd.EntityUpdate('Foo')
        old.label = 'old'
        old.timestamp = 1000.0
        old.ttl = 10
        old.exists = True
        new = entityd.EntityUpdate('Foo')
        new.label = 'new'
        new.timestamp = 2000.0
        new.ttl = 5
        new.exists = False
        merged = old.merge(new)
        assert merged.ueid == old.ueid
        assert merged.ueid == new.ueid  # sanity check
        assert merged.label == 'new'
        assert merged.timestamp == 2000.0
        assert merged.ttl == 5
        assert merged.exists == False

    def test_attributes(self):
        old = entityd.EntityUpdate('Foo')
        old.attrs.set('spam', 'eggs', traits={'qux'})
        new = entityd.EntityUpdate('Foo')
        new.attrs.set('spam', 'chicken', traits={'qux', 'quux'})
        merged = old.merge(new)
        assert len(list(merged.attrs)) == 1
        assert len(list(merged.attrs.deleted())) == 0
        assert merged.attrs.get('spam').value == 'chicken'
        assert merged.attrs.get('spam').traits == {'qux', 'quux'}

    def test_attributes_delete_old(self):
        old = entityd.EntityUpdate('Foo')
        old.attrs.delete('spam')
        new = entityd.EntityUpdate('Foo')
        merged = old.merge(new)
        assert len(list(merged.attrs)) == 0
        assert len(list(merged.attrs.deleted())) == 1
        assert 'spam' in merged.attrs.deleted()
        with pytest.raises(KeyError):
            merged.attrs.get('spam')

    def test_attributes_delete_new(self):
        old = entityd.EntityUpdate('Foo')
        old.attrs.set('spam', 'eggs', traits={'qux'})
        new = entityd.EntityUpdate('Foo')
        new.attrs.delete('spam')
        merged = old.merge(new)
        assert len(list(merged.attrs)) == 0
        assert len(list(merged.attrs.deleted())) == 1
        assert 'spam' in merged.attrs.deleted()
        with pytest.raises(KeyError):
            merged.attrs.get('spam')

    def test_attributes_delete_old_and_new(self):
        old = entityd.EntityUpdate('Foo')
        old.attrs.delete('spam')
        new = entityd.EntityUpdate('Foo')
        new.attrs.delete('spam')
        merged = old.merge(new)
        assert len(list(merged.attrs)) == 0
        assert len(list(merged.attrs.deleted())) == 1
        assert 'spam' in merged.attrs.deleted()
        with pytest.raises(KeyError):
            merged.attrs.get('spam')

    def test_attributes_delete_old_recreate(self):
        old = entityd.EntityUpdate('Foo')
        old.attrs.delete('spam')
        new = entityd.EntityUpdate('Foo')
        new.attrs.set('spam', 'chicken', {'qux', 'quux'})
        merged = old.merge(new)
        assert len(list(merged.attrs)) == 1
        assert len(list(merged.attrs.deleted())) == 0
        assert merged.attrs.get('spam').value == 'chicken'
        assert merged.attrs.get('spam').traits == {'qux', 'quux'}

    @relations
    def test_relations(self, relations):
        relation_1 = entityd.EntityUpdate('Bar')
        relation_2 = entityd.EntityUpdate('Baz')
        old = entityd.EntityUpdate('Foo')
        getattr(old, relations).add(relation_1)
        new = entityd.EntityUpdate('Foo')
        getattr(old, relations).add(relation_2)
        merged = old.merge(new)
        assert set(getattr(merged, relations)) == {
            relation_1.ueid,
            relation_2.ueid,
        }

    @relations
    def test_relations_discard_old(self, relations):
        relation = entityd.EntityUpdate('Bar')
        old = entityd.EntityUpdate('Foo')
        getattr(old, relations).discard(relation)
        new = entityd.EntityUpdate('Foo')
        merged = old.merge(new)
        assert set(getattr(merged, relations)) == set()


    @relations
    def test_relations_discard_new(self, relations):
        relation = entityd.EntityUpdate('Bar')
        old = entityd.EntityUpdate('Foo')
        getattr(old, relations).add(relation)
        new = entityd.EntityUpdate('Foo')
        getattr(old, relations).discard(relation)
        merged = old.merge(new)
        assert set(getattr(merged, relations)) == set()

    @relations
    def test_relations_discard_old_and_new(self, relations):
        relation = entityd.EntityUpdate('Bar')
        old = entityd.EntityUpdate('Foo')
        getattr(old, relations).discard(relation)
        new = entityd.EntityUpdate('Foo')
        getattr(old, relations).discard(relation)
        merged = old.merge(new)
        assert set(getattr(merged, relations)) == set()

    @relations
    def test_relations_discard_old_recreate(self, relations):
        relation = entityd.EntityUpdate('Bar')
        old = entityd.EntityUpdate('Foo')
        getattr(old, relations).discard(relation)
        new = entityd.EntityUpdate('Foo')
        getattr(old, relations).add(relation)
        merged = old.merge(new)
        assert set(getattr(merged, relations)) == {relation.ueid}


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

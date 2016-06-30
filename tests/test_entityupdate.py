import time

import pytest

import entityd


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
    assert not list(update.children)
    update.children.add('ueid')
    assert 'ueid' in update.children
    update.children.add(update)
    assert update.ueid in update.children


def test_parents(update):
    assert not list(update.parents)
    update.parents.add('ueid')
    assert 'ueid' in update.parents
    update.parents.add(update)
    assert update.ueid in update.parents


def test_delete(update):
    assert not update.deleted
    update.delete()
    assert update.deleted


def test_ueid():
    u1 = entityd.EntityUpdate('Type')
    u2 = entityd.EntityUpdate('Type')
    assert u1.ueid == u2.ueid
    u1.attrs.set('notAnId', 'aeue')
    assert u1.ueid == u2.ueid
    u1.attrs.set('id', 'aeaea', {'entity:id'})
    assert u1.ueid != u2.ueid


@pytest.mark.parametrize('value', [None, 0, 0.0, '', b'', [], {}])
def test_attrs(update, value):
    update.attrs.set('key', value, traits=set())
    assert update.attrs.get('key').value == value
    assert update.attrs.get('key').traits == set()
    update.attrs.delete('key')
    with pytest.raises(KeyError):
        update.attrs.get('key')
    assert 'key' in update.attrs.deleted()


def test_attr_delete_nonexistent(update):
    update.attrs.delete('NA')
    assert 'NA' in update.attrs.deleted()


def test_create_deleted_from_ueid():
    ueid = 'abdef'
    update = entityd.EntityUpdate('Endpoint', ueid=ueid)
    update.delete()
    assert update.deleted
    assert update.ueid == ueid


@pytest.mark.xfail(reason='Known deficiency in UEID specification')
@pytest.mark.parametrize(('literal', 'string'), [
    (None, 'None'),
    (0, '0'),
    (0.0, '0.0'),
    (b'foo', "b'foo'"),
    ([], '[]'),
    ((), '()'),
    ({}, '{}'),
])
def test_value_string_conversion(literal, string):
    update_literal = entityd.EntityUpdate('Foo')
    update_literal.attrs.set('bar', literal, {'entity:id'})
    update_string = entityd.EntityUpdate('Foo')
    update_string.attrs.set('bar', string, {'entity:id'})
    assert update_literal.ueid != update_string.ueid

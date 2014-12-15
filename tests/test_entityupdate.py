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


def test_children(update):
    assert not update.children._relations
    update.children.add('ueid')
    assert 'ueid' in update.children._relations
    update.children.add(update)
    assert update.ueid in update.children._relations


def test_parents(update):
    assert not update.parents._relations
    update.parents.add('ueid')
    assert 'ueid' in update.parents._relations
    update.parents.add(update)
    assert update.ueid in update.parents._relations


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
    u1.attrs.set('id', 'aeaea', 'id')
    assert u1.ueid != u2.ueid


def test_attrs(update):
    update.attrs.set('key', 'value', 'type')
    assert update.attrs.getvalue('key') == 'value'
    assert update.attrs._attrs['key']['type'] == 'type'
    update.attrs.delete('key')
    assert update.attrs._attrs['key']['deleted'] is True

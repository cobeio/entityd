import pathlib
import sqlite3

import act
import msgpack
import pytest

import entityd.kvstore


@pytest.fixture()
def kvstore():
    return entityd.kvstore.KVStore(':memory:')


def test_entityd_sessionstart(monkeypatch):
    init = pytest.Mock(return_value=None)
    session = pytest.Mock()
    monkeypatch.setattr(entityd.kvstore.KVStore, '__init__', init)
    entityd.kvstore.entityd_sessionstart(session)
    dbpath = init.call_args[0][0]
    assert isinstance(dbpath, pathlib.Path)
    assert str(dbpath).endswith('/var/lib/entityd/kvstore/store.db')
    name, _ = session.addservice.call_args[0]
    assert name == 'kvstore'


def test_sessionstart_permissionerror(tmpdir, monkeypatch):
    session = pytest.Mock()
    dirpath = tmpdir.join('notallowed')
    dirpath.ensure(dir=True)
    dirpath.chmod(0o000)
    dbpath = pathlib.Path(str(dirpath.join('entityd/kvstore.db')))
    monkeypatch.setattr(act.fsloc, 'statedir', pytest.Mock())
    monkeypatch.setattr(act.fsloc.statedir, 'joinpath',
                        pytest.Mock(return_value=dbpath))
    with pytest.raises(PermissionError):
        entityd.kvstore.entityd_sessionstart(session)


def test_sessionstart_mkdir(tmpdir, monkeypatch):
    session = pytest.Mock()
    dirpath = tmpdir.join('var')
    dirpath.ensure(dir=True)
    dbpath = pathlib.Path(str(dirpath.join('lib/entityd/kvstore.db')))
    monkeypatch.setattr(act.fsloc, 'statedir', pytest.Mock())
    monkeypatch.setattr(act.fsloc.statedir, 'joinpath',
                        pytest.Mock(return_value=dbpath))
    entityd.kvstore.entityd_sessionstart(session)
    assert dbpath.parent.is_dir()
    assert dbpath.is_file()


def test_entityd_sessionfinish():
    session = pytest.Mock()
    entityd.kvstore.entityd_sessionfinish(session)
    assert session.svc.kvstore.close.called


def test_init_table_is_created(kvstore):
    curs = kvstore._conn.cursor()
    curs.execute('SELECT count(*) FROM entityd_kv_store')
    row = curs.fetchone()
    assert row == (0,)


def test_init_permissionerror(tmpdir):
    dbpath = tmpdir.join('notallowed')
    dbpath.ensure()
    dbpath.chmod(0o000)
    with pytest.raises(PermissionError):
        entityd.kvstore.KVStore(dbpath)


def test_close(kvstore):
    kvstore.close()
    with pytest.raises(sqlite3.ProgrammingError):
        curs = kvstore._conn.cursor()
        curs.execute('SELECT count(*) FROM entityd_kv_store')


def test_add(kvstore):
    kvstore.add('foo', [0, 1, 2])
    curs = kvstore._conn.cursor()
    curs.execute('SELECT * FROM entityd_kv_store')
    key, value = curs.fetchone()
    assert isinstance(key, str)
    assert key == 'foo'
    assert isinstance(value, bytes)
    assert value == msgpack.packb([0, 1, 2])


def test_add_dup(kvstore):
    kvstore.add('foo', 0)
    kvstore.add('foo', 1)
    curs = kvstore._conn.cursor()
    curs.execute('SELECT value  from entityd_kv_store')
    value = curs.fetchone()[0]
    assert value == msgpack.packb(1)


def test_add_type(kvstore):
    with pytest.raises(TypeError):
        kvstore.add('foo', {0, 1})
    curs = kvstore._conn.cursor()
    curs.execute('SELECT count(*) from entityd_kv_store')
    count = curs.fetchone()[0]
    assert count == 0


def test_addmany(kvstore):
    kvstore.addmany({'foo': 0, 'bar': 1})
    curs = kvstore._conn.cursor()
    curs.execute('SELECT * FROM entityd_kv_store')
    rows = curs.fetchall()
    rows.sort()
    assert rows[0] == ('bar', msgpack.packb(1))
    assert rows[1] == ('foo', msgpack.packb(0))


def test_addmany_dup(kvstore):
    kvstore.addmany({'foo': 0, 'bar': 1})
    kvstore.addmany({'foo': 2, 'baz': 3})
    curs = kvstore._conn.cursor()
    curs.execute('SELECT * FROM entityd_kv_store')
    rows = curs.fetchall()
    rows.sort()
    assert rows[0] == ('bar', msgpack.packb(1))
    assert rows[1] == ('baz', msgpack.packb(3))
    assert rows[2] == ('foo', msgpack.packb(2))


@pytest.mark.parametrize(['type_', 'item'],
                         [(int, 42),
                          (float, 2.2),
                          (str, 'foo'),
                          (bytes, b'foo'),
                          (list, [0, 1]),
                          (dict, {'a': 0})])
def test_get(kvstore, type_, item):
    kvstore.add('key', item)
    val = kvstore.get('key')
    assert isinstance(val, type_)
    assert val == item


def test_get_missing(kvstore):
    with pytest.raises(KeyError):
        kvstore.get('missing')


def test_getmany(kvstore):
    kvstore.add('foo:0', 0)
    kvstore.add('foo:1', 1)
    items = kvstore.getmany('foo:')
    assert items == {'foo:0': 0, 'foo:1': 1}


def test_getmany_noresult(kvstore):
    assert kvstore.getmany('foo:') == {}


def test_delete(kvstore):
    kvstore.add('foo', 0)
    assert kvstore.get('foo') == 0
    kvstore.delete('foo')
    with pytest.raises(KeyError):
        kvstore.get('foo')


def test_deletemany(kvstore):
    kvstore.add('foo:0', 0)
    kvstore.add('foo:1', 0)
    kvstore.deletemany('foo:')
    assert kvstore.getmany('foo:') == {}

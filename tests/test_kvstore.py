import pytest

import entityd.hookspec
import entityd.kvstore


@pytest.fixture()
def kvstore():
    kvstore = entityd.kvstore.KVStore()
    kvstore.entityd_configure()
    return kvstore


def test_plugin_registered(pm):
    pm.addhooks(entityd.hookspec)
    name = 'entityd.kvstore'
    entityd.kvstore.entityd_plugin_registered(pm, name)
    assert pm.isregistered('entityd.kvstore.KVStore')


def test_configure():
    config = pytest.Mock()
    entityd.kvstore.KVStore().entityd_configure()
    assert config.addentity.called_once_with('Host',
                                             'entityd.hostme.HostEntity')


def test_table_is_created(kvstore):
    conn = kvstore.conn
    curs = conn.cursor()
    curs.execute("SELECT * FROM entityd_kv_store")


def test_fails_with_permission_error():
    kvstore = entityd.kvstore.KVStore()
    kvstore.location = '/not_allowed'
    with pytest.raises(PermissionError):
        kvstore.entityd_configure()


def test_closed_on_unconfigure(kvstore):
    kvstore.entityd_unconfigure()
    assert kvstore.conn is None


def test_kvstore_ops(kvstore):
    key = "key"
    kvstore.entityd_kvstore_delete(key)

    value = [3, 4, '5']
    kvstore.entityd_kvstore_add(key, value)
    assert kvstore.entityd_kvstore_get(key) == [3, 4, '5']

    kvstore.entityd_kvstore_add(key, '55')
    assert kvstore.entityd_kvstore_get(key) == '55'

    kvstore.entityd_kvstore_delete(key)
    assert kvstore.entityd_kvstore_get(key) is None


def test_kvstore_multiops(kvstore):
    kvstore.entityd_kvstore_deletemany('multi.key')

    kvstore.entityd_kvstore_addmany({
        'multi.key1': 'value1',
        'multi.key2': 2.5
    })

    vals = kvstore.entityd_kvstore_getmany('multi.key')
    assert vals['multi.key1'] == 'value1'
    assert vals['multi.key2'] == 2.5

    kvstore.entityd_kvstore_addmany({
        'multi.key2': 3.5
    })

    vals = kvstore.entityd_kvstore_getmany('multi.key')
    assert vals['multi.key1'] == 'value1'
    assert vals['multi.key2'] == 3.5

    kvstore.entityd_kvstore_deletemany('multi.key')
    vals = list(kvstore.entityd_kvstore_getmany('multi.key'))
    assert len(vals) == 0

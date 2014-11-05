import pytest

import entityd.kvstore


@pytest.fixture()
def kvstore():
    kvstore = entityd.kvstore.KVStore()
    kvstore.entityd_configure()
    return kvstore


def test_table_is_created(kvstore):
    conn = kvstore.conn
    curs = conn.cursor()
    curs.execute("SELECT * FROM entityd_kv_store")


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

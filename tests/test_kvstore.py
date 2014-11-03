import unittest.mock
import os

import pytest

import entityd.core
import entityd.__main__
import entityd.pm
import entityd.kvstore


@pytest.fixture()
def kvstore():
    kvstore = entityd.kvstore.KVStore()
    kvstore.entityd_sessionstart()
    kvstore.entityd_kvstore_delete((1, 2))
    return kvstore


def test_table_is_created(kvstore):
    conn = kvstore.conn
    curs = conn.cursor()
    curs.execute("SELECT * FROM entityd_kv_store")


def test_kvstore_ops(kvstore):
    key = (1, 2)
    value = [3, 4, '5']
    kvstore.entityd_kvstore_put(key, value)

    assert kvstore.entityd_kvstore_get((1, 2)) == [3, 4, '5']

    kvstore.entityd_kvstore_delete((1, 2))
    assert kvstore.entityd_kvstore_get((1, 2)) is None
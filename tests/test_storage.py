import argparse
import sys
import unittest.mock
import os

import pytest

import entityd.core
import entityd.__main__
import entityd.pm
import entityd.kvstore


@pytest.fixture()
def kvstore(tmpdir):
    kvstore = entityd.kvstore.KVStore()
    if os.path.exists(kvstore.location):
        os.remove(kvstore.location)
    session = unittest.mock.Mock()
    kvstore.entityd_sessionstart(session=session)
    return kvstore


def test_table_is_created(kvstore):
    conn = kvstore.conn
    curs = conn.cursor()
    curs.execute("SELECT * FROM entityd_kv_store")
    assert curs.fetchall() == []


def test_kvstore_put(kvstore):
    key = (1, 2)
    value = [3, 4, '5']
    kvstore.entityd_kvstore_put(key, value)
    curs = kvstore.conn.cursor()
    curs.execute('SELECT * from entityd_kv_store')
    res = curs.fetchall()
    assert len(res) == 1


def test_kvstore_get(kvstore):
    key = (1, 2)

    value = [3, 4, '5']
    kvstore.entityd_kvstore_put(key, value)
    curs = kvstore.conn.cursor()
    curs.execute('SELECT * from entityd_kv_store')
    res = curs.fetchall()
    assert len(res) == 1

    # couldn't we depend on previous?
    assert kvstore.entityd_kvstore_get((1, 2)) == [3, 4, '5']


def test_kvstore_delete(kvstore):
    key = (1, 2)

    value = [3, 4, '5']
    kvstore.entityd_kvstore_put(key, value)
    curs = kvstore.conn.cursor()
    curs.execute('SELECT * from entityd_kv_store')
    res = curs.fetchall()
    assert len(res) == 1

    # couldn't we depend on previous test data?
    assert kvstore.entityd_kvstore_get((1, 2)) == [3, 4, '5']

    kvstore.entityd_kvstore_delete((1, 2))
    assert kvstore.entityd_kvstore_get((1, 2)) is None
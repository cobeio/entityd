import argparse
import sys
import unittest.mock
import os

import pytest

import entityd.core
import entityd.__main__
import entityd.pm
import entityd.storage


@pytest.fixture()
def storage_plugin(tmpdir):
    storage_plugin = entityd.storage.Storage()
    session = unittest.mock.Mock()
    session.config.args.database = tmpdir.strpath

    storage_plugin.entityd_sessionstart(session=session)
    assert os.path.exists(os.path.join(session.config.args.database,
                                       'entityd.db'))
    return storage_plugin


def test_storage_adds_database_param():
    storage_plugin = entityd.storage.Storage()
    assert hasattr(storage_plugin, 'entityd_addoption')
    option_parser = unittest.mock.create_autospec(argparse.ArgumentParser)
    storage_plugin.entityd_addoption(parser=option_parser)
    option_parser.add_argument.assert_called_once_with(
        '--database',
        type=str,
        help='Location on disk to put SQLite database for persistence.'
    )


def test_storage_creates_database_if_file_provided(tmpdir):
    storage_plugin = entityd.storage.Storage()
    assert hasattr(storage_plugin, 'entityd_sessionstart')
    session = unittest.mock.Mock()
    session.config.args.database = tmpdir.join('database.db').strpath

    storage_plugin.entityd_sessionstart(session=session)
    assert os.path.exists(session.config.args.database)


def test_storage_creates_database_if_dir_provided(tmpdir):
    storage_plugin = entityd.storage.Storage()
    assert hasattr(storage_plugin, 'entityd_sessionstart')
    session = unittest.mock.Mock()
    session.config.args.database = tmpdir.strpath

    storage_plugin.entityd_sessionstart(session=session)
    assert os.path.exists(os.path.join(session.config.args.database,
                                       'entityd.db'))


def test_table_is_created(storage_plugin):
    conn = storage_plugin.conn
    curs = conn.cursor()
    curs.execute("SELECT * FROM entityd_kv_store")
    assert curs.fetchall() == []


def test_storage_put(storage_plugin):
    key = (1, 2)
    value = [3, 4, '5']
    storage_plugin.entityd_storage_put(key, value)
    curs = storage_plugin.conn.cursor()
    curs.execute('SELECT * from entityd_kv_store')
    res = curs.fetchall()
    assert len(res) == 1


def test_storage_get(storage_plugin):
    key = (1, 2)

    value = [3, 4, '5']
    storage_plugin.entityd_storage_put(key, value)
    curs = storage_plugin.conn.cursor()
    curs.execute('SELECT * from entityd_kv_store')
    res = curs.fetchall()
    assert len(res) == 1

    # couldn't we depend on previous?
    assert storage_plugin.entityd_storage_get((1, 2)) == [3, 4, '5']


def test_storage_delete(storage_plugin):
    key = (1, 2)

    value = [3, 4, '5']
    storage_plugin.entityd_storage_put(key, value)
    curs = storage_plugin.conn.cursor()
    curs.execute('SELECT * from entityd_kv_store')
    res = curs.fetchall()
    assert len(res) == 1

    # couldn't we depend on previous test data?
    assert storage_plugin.entityd_storage_get((1, 2)) == [3, 4, '5']

    storage_plugin.entityd_storage_delete((1, 2))
    assert storage_plugin.entityd_storage_get((1, 2)) is None
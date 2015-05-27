"""Key-Value Storage plugin"""

import sqlite3

import act
import msgpack

import entityd.core
import entityd.pm


@entityd.pm.hookimpl
def entityd_sessionstart(session):
    """Register the kvstore service."""
    dbpath = act.fsloc.statedir.joinpath('lib/entityd/kvstore/store.db')
    if not dbpath.parent.is_dir():
        dbpath.parent.mkdir(parents=True)
    kvstore = KVStore(dbpath)
    session.addservice('kvstore', kvstore)


@entityd.pm.hookimpl
def entityd_sessionfinish(session):
    """Terminate the kvstore service."""
    session.svc.kvstore.close()


class KVStore:
    """A key-value store service for entityd.

    This can store and retrieve key-value data using the :meth:`set`
    and :meth:`get` methods.  Keys must be strings while values must
    data which can be serialised using msgpack.

    It is a common practice to start the key with the module name of
    the plugin.  E.g. the entityd.processme module has all keys
    starting with ``entityd.processme:``.

    After calling :meth:`close` the store can no longer be used.

    """

    def __init__(self, dbpath):
        try:
            self._conn = sqlite3.connect(str(dbpath))
        except sqlite3.OperationalError as err:
            raise PermissionError(
                'Unable to write to database at {}.'.format(dbpath)) from err
        else:
            self._conn.execute("""\
                CREATE TABLE IF NOT EXISTS entityd_kv_store
                (key TEXT PRIMARY KEY, value BLOB)
            """)
            self._conn.commit()

    def close(self):
        """Close the store.

        After calling this you can no longer use the KVStore.  This
        should not be called by a user, the kvstore plugin will
        normally call this in ``entityd_sessionfinish()``.  Attempting
        to use the store after this will probably result in a
        sqlite3.ProgrammingError exception.

        """
        self._conn.close()

    def add(self, key, value):
        """Persist this key -> value mapping.

        :param key: A unicode string.
        :param value: Any data which can be serialised using msgpack.

        """
        packed_value = msgpack.packb(value, use_bin_type=True)
        self._conn.execute('INSERT OR REPLACE INTO entityd_kv_store'
                           ' VALUES (?, ?)', (key, packed_value))
        self._conn.commit()

    def addmany(self, mapping):
        """Persist the keys and values in the mapping.

        Any key which already exists will be overwritten.  As with
        ``.add()` keys must be unicode strings while values must be
        serialisable by msgpack.

        :param mapping: Dictionary of values to persist.

        """
        insert_list = []
        for key, value in mapping.items():
            packed_value = msgpack.packb(value, use_bin_type=True)
            insert_list.append((key, packed_value))
        self._conn.executemany('INSERT OR REPLACE INTO entityd_kv_store'
                               ' VALUES (?,?)', insert_list)
        self._conn.commit()

    def get(self, key):
        """Retrieve the value for the given key.

        :param key: A unicode key.
        :raises KeyError: If the key does not exist.

        """
        curs = self._conn.cursor()
        curs.execute('SELECT value FROM entityd_kv_store'
                     ' WHERE key = ?', (key,))
        result = curs.fetchone()
        if result:
            return msgpack.unpackb(result[0], encoding='utf8')
        else:
            raise KeyError('No such key: {}'.format(key))

    def getmany(self, prefix):
        """Retrieve all key-value pairs who's keys start with the given prefix.

        This queries the store for all keys starting with the prefix.
        A dictionary of all the resulting key-value pairs is returned.

        :param prefix: A unicode string with the key prefix.
        :return: If no keys are found an empty dictionary will be
           returned.

        """
        curs = self._conn.cursor()
        curs.execute('SELECT key, value FROM entityd_kv_store'
                     ' WHERE key LIKE ?', (prefix + '%',))
        result = curs.fetchall()
        return {k: msgpack.unpackb(v, encoding='utf8') for k, v in result}

    def delete(self, key):
        """Delete the stored value for a key.

        If the key does not exist it is silently ignored.

        :param key: The unicode string of a key.

        """
        self._conn.execute('DELETE FROM entityd_kv_store'
                           ' WHERE key = ?', (key,))
        self._conn.commit()

    def deletemany(self, prefix):
        """Delete all items where the key starts with the given prefix.

        :param prefix: The unicode prefix of keys to delete.

        """
        self._conn.execute('DELETE FROM entityd_kv_store'
                           ' WHERE key LIKE ?', (prefix + '%',))
        self._conn.commit()

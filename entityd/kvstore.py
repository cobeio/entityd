"""Key-Value Storage plugin"""

import os
import sqlite3

import msgpack

import entityd.core
import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.kvstore':
        service = KVStore()
        pluginmanager.register(service,
                               name='entityd.kvstore.KVStore')


class KVStore:
    """Plugin to manage a key-value store.

    Accessed via hooks. Will store data in a SQLite database in the
    project top level directory.
    """

    def __init__(self):
        self.conn = None
        self.location = os.path.join(os.path.dirname(__file__),
                                     '..',
                                     'entityd_kvstore.db')

    @entityd.pm.hookimpl
    def entityd_configure(self):
        """Called before the monitoring session starts."""
        try:
            self.conn = sqlite3.connect(self.location)
        except sqlite3.OperationalError:
            raise PermissionError(
                "Unable to write to database at {}.".format(self.location))
        self.conn.execute("""\
        CREATE TABLE IF NOT EXISTS entityd_kv_store
        (key BLOB PRIMARY KEY, value BLOB)
        """)
        self.conn.commit()

    @entityd.pm.hookimpl
    def entityd_unconfigure(self):
        """Called after the monitoring session finishes."""
        self.conn.close()
        self.conn = None

    @entityd.pm.hookimpl
    def entityd_kvstore_add(self, key, value):
        """Persist this key -> value mapping."""
        packed_value = msgpack.packb(value, use_bin_type=True)
        self.conn.execute("""REPLACE INTO entityd_kv_store VALUES (?, ?)""",
                          (key, packed_value))
        self.conn.commit()

    @entityd.pm.hookimpl
    def entityd_kvstore_addmany(self, values):
        """Persist the keys and values in ``values``.

        :param values: Dictionary of values to persist.
        """
        insert_list = []
        for key, value in values.items():
            packed_value = msgpack.packb(value, use_bin_type=True)
            insert_list.append((key, packed_value))
        self.conn.executemany("""REPLACE INTO entityd_kv_store
                                 VALUES (?,?)""", insert_list)
        self.conn.commit()

    @entityd.pm.hookimpl
    def entityd_kvstore_get(self, key):
        """Retrieve the value for ``key``."""
        curs = self.conn.cursor()
        curs.execute("""SELECT value FROM entityd_kv_store WHERE key = ?""",
                     (key,))
        result = curs.fetchone()
        if result:
            return msgpack.unpackb(result[0], encoding='utf8')

    @entityd.pm.hookimpl
    def entityd_kvstore_getmany(self, key_begins_with):
        """Retrieve the value for ``key``."""
        curs = self.conn.cursor()
        curs.execute("""SELECT key, value FROM entityd_kv_store
                     WHERE key LIKE ?""", (key_begins_with + '%',))
        result = curs.fetchall()

        return {k: msgpack.unpackb(v, encoding='utf8') for k, v in result}

    @entityd.pm.hookimpl
    def entityd_kvstore_delete(self, key):
        """Delete the mapping for ``key``."""
        self.conn.execute("""DELETE FROM entityd_kv_store WHERE key = ?""",
                          (key,))
        self.conn.commit()

    @entityd.pm.hookimpl
    def entityd_kvstore_deletemany(self, key_begins_with):
        """Delete the mappings for pattern ``key_begins_with``."""
        self.conn.execute("""DELETE FROM entityd_kv_store WHERE key LIKE ?""",
                          (key_begins_with + '%',))
        self.conn.commit()

"""Key-Value Storage plugin"""
import os
import sqlite3

import msgpack

import entityd.core
import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    if name == 'entityd.kvstore':
        service = KVStore()
        pluginmanager.register(service,
                               name='entityd.kvstore.KVStore')


class KVStore:
    def __init__(self):
        self.session = None
        self.config = None
        self.conn = None
        # Until we have some common resources, store db in project dir.
        self.location = os.path.join(os.path.dirname(__file__),
                                     '..',
                                     'entityd_kvstore.db')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        self.session = session
        self.config = session.config

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
    def entityd_sessionfinish(self, session):
        self.conn.close()
        self.conn = None

    @entityd.pm.hookimpl
    def entityd_kvstore_put(self, key, value):
        """Persist this key -> value mapping."""
        packed_key = msgpack.packb(key, use_bin_type=True)
        packed_value = msgpack.packb(value, use_bin_type=True)
        self.conn.execute("""INSERT INTO entityd_kv_store VALUES (?, ?)""",
                          (packed_key, packed_value))
        self.conn.commit()

    @entityd.pm.hookimpl
    def entityd_kvstore_get(self, key):
        """Retrieve the value for ``key``."""
        packed_key = msgpack.packb(key, use_bin_type=True)
        curs = self.conn.cursor()
        curs.execute("""SELECT value FROM entityd_kv_store WHERE key = ?""",
                     (packed_key,))
        result = curs.fetchone()
        if result:
            return msgpack.unpackb(result[0], encoding='utf8')

    @entityd.pm.hookimpl
    def entityd_kvstore_delete(self, key):
        """Delete the mapping for ``key``."""
        packed_key = msgpack.packb(key, use_bin_type=True)
        self.conn.execute("""DELETE FROM entityd_kv_store WHERE key = ?""",
                          (packed_key,))
        self.conn.commit()
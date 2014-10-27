"""Storage plugin"""
import logging
import os
import sqlite3

import msgpack

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    if name == 'entityd.storage':
        service = Storage()
        pluginmanager.register(service,
                               name='entityd.storage.Storage')


class Storage:
    def __init__(self):
        self.session = None
        self.config = None
        self.conn = None

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        self.session = session
        self.config = session.config
        location = self.config.args.database
        if location is None:
            logging.info("Not creating persistence datastore; --database "
                         "parameter not provided.")
            return None

        if os.path.isdir(location):
            location = os.path.join(location, 'entityd.db')

        try:
            self.conn = sqlite3.connect(location)
        except sqlite3.OperationalError:
            logging.info("Unable to write to database at {}. Persistence "
                         "will be disabled.".format(location))
            return None

        self.conn.execute("""\
        CREATE TABLE IF NOT EXISTS entityd_kv_store
        (key BLOB PRIMARY KEY, value BLOB)
        """)
        self.conn.commit()

    @entityd.pm.hookimpl
    def entityd_addoption(self, parser):
        parser.add_argument(
            '--database',
            type=str,
            help='Location on disk to put SQLite database for persistence.',
        )

    @entityd.pm.hookimpl
    def entityd_storage_put(self, key, value):
        """Persist this key -> value mapping"""
        if self.conn is None:
            return False
        packed_key = msgpack.packb(key, use_bin_type=True)
        packed_value = msgpack.packb(value, use_bin_type=True)
        self.conn.execute("""INSERT INTO entityd_kv_store VALUES (?, ?)""",
                          (packed_key, packed_value))
        self.conn.commit()
        return True

    @entityd.pm.hookimpl
    def entityd_storage_get(self, key):
        """Retrieve the value for ``key``"""
        if self.conn is None:
            return None
        packed_key = msgpack.packb(key, use_bin_type=True)
        curs = self.conn.cursor()
        curs.execute("""SELECT value FROM entityd_kv_store WHERE key = ?""",
                     (packed_key,))
        result = curs.fetchone()
        if result:
            return msgpack.unpackb(result[0], encoding='utf8')

    @entityd.pm.hookimpl
    def entityd_storage_delete(self, key):
        """Delete the mapping for ``key``"""
        if self.conn is None:
            return False
        packed_key = msgpack.packb(key, use_bin_type=True)
        self.conn.execute("""DELETE FROM entityd_kv_store WHERE key = ?""",
                          (packed_key,))
        self.conn.commit()
        return True
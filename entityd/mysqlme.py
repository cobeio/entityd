"""Plugin providing MySQL monitored entities.

Supporting multiple instances on a host, identified by the configuration
file location.

MySQL entities are related to top-level processes, configuration file
entity and the parent host.

Assumes that the MySQL binary is called 'mysqld' for discovery.
"""

import argparse
import itertools
import os
import shlex

import logbook

import entityd.pm


log = logbook.Logger(__name__)


class MySQLEntity:
    """Monitor for MySQL instances."""

    def __init__(self):
        self.session = None
        self._host_ueid = None
        self._log_flag = False

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the MySQL Monitored Entity."""
        config.addentity('MySQL', 'entityd.mysqlme.MySQLEntity')

    @entityd.pm.hookimpl()
    def entityd_sessionstart(self, session):
        """Store session for later use."""
        self.session = session

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):
        """Return an iterator of "MySQL" Monitored Entities."""
        if name == 'MySQL':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported '
                                  'for attrs {}'.format(attrs))
            return self.entities(include_ondemand=include_ondemand)

    @property
    def host_ueid(self):  # pragma: no cover
        """Get and store the host ueid.

        :raises LookupError: If a host UEID cannot be found.

        :returns: A :class:`cobe.UEID` for the host.
        """
        if self._host_ueid:
            return self._host_ueid
        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Host', attrs=None)
        for hosts in results:
            for host in hosts:
                self._host_ueid = host.ueid
                return self._host_ueid
        raise LookupError('Could not find the host UEID')

    def entities(self, include_ondemand):
        """Return MySQLEntity objects."""
        for proc in self.top_level_mysql_processes():
            mysql = MySQL(proc)
            update = entityd.EntityUpdate('MySQL')
            update.attrs.set('host', str(self.host_ueid),
                             traits={'entity:id', 'entity:ueid'})
            try:
                update.attrs.set('config_path',
                                 mysql.config_path(), traits={'entity:id'})
            except MySQLNotFoundError:
                if not self._log_flag:
                    log.warning('Could not find config path for MySQL.')
                    self._log_flag = True
                return
            update.attrs.set('process_id', proc.attrs.get('pid').value)
            if include_ondemand:
                files = list(itertools.chain.from_iterable(
                    self.session.pluginmanager.hooks.entityd_find_entity(
                        name='File', attrs={'path': mysql.config_path()})
                ))
                if files:
                    update.children.add(files[0])
                    yield files[0]
            update.children.add(proc)
            update.parents.add(self.host_ueid)
            yield update

    def top_level_mysql_processes(self):
        """Find top level MySQL processes.

        :return: List of Process ``EntityUpdate``s whose parent is not
           also 'mysqld'.
        """
        processes = {}
        proc_gens = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs={'binary': 'mysqld'})
        for entity in itertools.chain.from_iterable(proc_gens):
            processes[entity.attrs.get('pid').value] = entity
        return [e for e in processes.values()
                if e.attrs.get('ppid').value not in processes]


class MySQLNotFoundError(Exception):
    """Thrown if the MySQL instance cannot be found."""


class MySQL:
    """Abstract MySQL instance.

    :ivar process: The main MySQL process as an EntityUpdate
    """

    def __init__(self, process):
        self.process = process

    def config_path(self):
        """Find the path for the my.cnf config file.

        Checks standard locations for the file, else checks instruction text
        from processes.

        :return: Full path for postgresql config file.
        """

        paths = ['/etc/my.cnf',
                 '/etc/mysql/my.cnf',
                 '/usr/etc/my.cnf',
                 os.path.expanduser('~/.my.cnf')]
        command = self.process.attrs.get('command').value
        parser = argparse.ArgumentParser()
        parser.add_argument('--defaults-file', dest='config')
        args, _ = parser.parse_known_args(shlex.split(command))
        if args.config:
            return args.config
        else:
            for path in paths:
                if os.path.isfile(path):
                    return path
        raise MySQLNotFoundError('Could not find config path for MySQL.')

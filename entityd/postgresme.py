"""Plugin providing PostgreSQL monitored entities.

Supporting multiple instances on a host, identified by the configuration
file location.

PostrgesSQL entities are related to top-level processes, configuration file
entity and the parent host.

Assumes that the PostgreSQL binary is called 'postgresql.conf' for discovery.
"""

import itertools
import os
import shlex
import re

import logbook

import entityd.pm


log = logbook.Logger(__name__)


class PostgreSQLEntity:
    """Monitor for PostgreSQL instances."""

    def __init__(self):
        self.session = None
        self._host_ueid = None
        self._log_flag = False

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the PostgreSQL Monitored Entity."""
        config.addentity('PostgreSQL', 'entityd.postgresme.PostgreSQLEntity')

    @entityd.pm.hookimpl()
    def entityd_sessionstart(self, session):
        """Store session for later use."""
        self.session = session

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):
        """Return an iterator of "PostgreSQL" Monitored Entities."""
        if name == 'PostgreSQL':
            if attrs is not None:
                raise LookupError('Attribute based filtering'
                                  ' not supported for attrs {}'.format(attrs))
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
        """Return PostgreSQLEntity objects."""
        for proc in self.top_level_postgresql_processes():
            postgres = PostgreSQL(proc)
            update = entityd.EntityUpdate('PostgreSQL')
            update.attrs.set('host', str(self.host_ueid),
                             traits={'entity:id', 'entity:ueid'})
            try:
                update.attrs.set(
                    'config_path',
                    postgres.config_path(), traits={'entity:id'}
                )
            except PostgreSQLNotFoundError:
                if not self._log_flag:
                    log.warning('Could not find config path for PostgreSQL.')
                    self._log_flag = True
                return
            update.attrs.set('process_id', proc.attrs.get('pid').value)
            if include_ondemand:
                files = list(itertools.chain.from_iterable(
                    self.session.pluginmanager.hooks.entityd_find_entity(
                        name='File', attrs={'path': postgres.config_path()})
                ))
                if files:
                    update.children.add(files[0])
                    yield files[0]
            update.children.add(proc)
            update.parents.add(self.host_ueid)
            yield update

    def top_level_postgresql_processes(self):
        """Find top level PostgreSQL processes.

        :return: List of Process ``EntityUpdate``s whose parent is not
           also 'PostgreSQL'.
        """
        processes = {}
        proc_gens = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs={'binary': 'postgres'})
        for entity in itertools.chain.from_iterable(proc_gens):
            processes[entity.attrs.get('pid').value] = entity
        return [e for e in processes.values()
                if e.attrs.get('ppid').value not in processes]


class PostgreSQLNotFoundError(Exception):
    """Thrown if the PostgreSQL instance cannot be found."""


class PostgreSQL:
    """Abstract PostgreSQL instance.

    :ivar process: The main PostgreSQL process as an EntityUpdate
    """
    def __init__(self, process):
        self.process = process

    def config_path(self):
        """Finds the path for the postgresql config file.

        Searches in process command text, then in directory usual suspects
        including format ``/etc/postgresql/*.*/main/postgresql.conf``
        where *.* is postgres version.

        :return: Full path for postgresql config file.
        """
        command = self.process.attrs.get('command').value
        comm = shlex.split(command)
        for param in comm:
            if (param.startswith('-cconfig_file=') or
                    param.startswith('config_file=')):
                path = param.split('=', 1)[1]
                return path
        paths = ['/var/lib/pgsql/data/postgresql.conf']
        if os.path.isdir('/etc/postgresql/'):
            for directory in os.listdir('/etc/postgresql/'):
                match = re.fullmatch(r'[0-9]+\.[0-9]+', directory)
                if match:
                    paths.append('/etc/postgresql/' +
                                 match.group() +
                                 '/main/postgresql.conf')
        paths.append(os.path.expanduser('~/postgresql.conf'))
        for path in paths:
            if os.path.isfile(path):
                return path
        raise PostgreSQLNotFoundError(
            'Could not find config path for PostgreSQL.')

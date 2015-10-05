"""Plugin providing PostgreSQL monitored entities

Supporting multiple instances on a host, identified by the configuration
file location.

PostrgesSQL entities are related to top-level processes, configuration file
entity and the parent host.

Assumes that the PostgreSQL binary is called 'postgresql.conf' for discovery.
"""

import argparse
import itertools
import os
import shlex

import entityd.pm


class PostgreSQLEntity:
    """Monitor for PostgreSQL instances."""

    def __init__(self):
        self.session = None
        self._host_ueid = None

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
                raise LookupError('Attribute based filtering not supported '
                                  'for attrs {}'.format(attrs))
            return self.entities(include_ondemand=include_ondemand)

    @property
    def host_ueid(self):
        """Get and store the host ueid."""
        if self._host_ueid:
            return self._host_ueid
        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Host', attrs=None)
        for hosts in results:
            for host in hosts:
                self._host_ueid = host.ueid
                return self._host_ueid

    def entities(self, include_ondemand):
        """Return PostgreSQLEntity objects."""
        for proc in self.top_level_postgresql_processes():
            postgres = PostgreSQL(proc)
            update = entityd.EntityUpdate('PostgreSQL')
            update.attrs.set('host', self.host_ueid, attrtype='id')
            update.attrs.set('config_path',
                             postgres.config_path(), attrtype='id')
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


class PostgreSQLNotFound(Exception):
    """Thrown if the PostgreSQL instance cannot be found"""
    pass


class PostgreSQL:
    """Abstract PostgreSQL instance.

    :ivar process: The main PostgreSQL process as an EntityUpdate
    """
    def __init__(self, process):
        self.process = process

    def config_path(self):
        """Get the path for postgresql.conf"""
        # Test obtaining config file from the most likely generic paths,
        # incl. format ``/etc/postgresql/*.*/main/postgresql.conf``
        # where *.* is postgres version nr, testing for paths for versions from
        # 8.0 to 12.9. This should cover next ~17 years(!), present being 9.4
        paths = ['/var/lib/pgsql/data/postgresql.conf'] + \
                ['/etc/postgresql/' + str(8.0 + n/10) +
                 '/main/postgresql.conf' for n in range(50)] + \
                [os.path.expanduser('~/') + 'postgresql.conf']
        command = self.process.attrs.get('command').value
        parser = argparse.ArgumentParser()
        parser.add_argument('-c', dest='config')
        args, _ = parser.parse_known_args(shlex.split(command))
        if (args.config and
                args.config[:12] == 'config_file=' and
                args.config[-15:] == 'postgresql.conf'):
            path = args.config.split('=')[1]
            return path
        else:
            for path in paths:
                if os.path.isfile(path):
                    return path
        raise PostgreSQLNotFound('Could not find config path for PostgreSQL.')

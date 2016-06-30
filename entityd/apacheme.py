"""Entity providing monitoring information on an Apache2 service.

This has been tested on CentOS 7 and Ubuntu 14.04.
In order to get Apache entities, Apache must be configured with mod_status
accessible on the localhost ip address. This is enabled by default on
Ubuntu, but for CentOS a section needs to be added to httpd.conf:

<Location /server-status>
        SetHandler server-status
        Require local
</Location>

If multiple Apache instances are running with different configurations
(but the same executable) then they will be exposed as separate entities
with different config_path values.

"""

import argparse
import collections
import itertools
import logging
import os
import pathlib
import re
import shlex
import subprocess

import requests

import entityd.fileme
import entityd.pm


class ApacheEntity:
    """Class to generate Apache MEs."""

    def __init__(self):
        self.session = None
        self._host_ueid = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Configure this entity.

        Register the Apache Monitored Entity.
        Reduce logging noise of requests library.
        """
        config.addentity('Apache', 'entityd.apacheme.ApacheEntity')
        logging.getLogger('requests').setLevel(logging.WARNING)

    @entityd.pm.hookimpl()
    def entityd_sessionstart(self, session):
        """Store session for later use."""
        self.session = session

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):
        """Return an iterator of "Apache" Monitored Entities."""
        if name == 'Apache':
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

    def entities(self, include_ondemand=False):
        """Return a generator of ApacheEntity objects

        :param include_ondemand: If True, return related `ondemand` entities
           that wouldn't be emitted otherwise.
        """
        for apache in self.active_apaches():
            try:
                perfdata = apache.performance_data()
            except ApacheNotFound:
                continue
            update = entityd.EntityUpdate('Apache')
            update.label = 'Apache'
            update.attrs.set('host', str(self.host_ueid),
                             traits={'entity:id', 'entity:ueid'})
            update.attrs.set('version', apache.version)
            update.attrs.set('config_path',
                             apache.config_path, traits={'entity:id'})
            update.attrs.set('config_ok', apache.check_config())
            update.attrs.set('config_last_mod', apache.config_last_modified(),
                             traits={'time:posix', 'unit:seconds'})
            for name, (value, traits) in perfdata.items():
                update.attrs.set(name, value, traits)
            update.children.add(apache.main_process)
            for address, port, path in apache.vhosts():
                vhost = self.create_vhost(address, port, apache=update)
                update.children.add(vhost)
                if include_ondemand:
                    files = itertools.chain.from_iterable(
                        self.session.pluginmanager.hooks.entityd_find_entity(
                            name='File', attrs={'path': path}))
                    files = list(files)
                    if files:
                        vhost.children.add(files[0])
                        yield files[0]
                    yield vhost

            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='File', attrs={'path': apache.config_path})
            for entity in itertools.chain.from_iterable(results):
                update.children.add(entity)
                if include_ondemand:
                    yield entity
            update.parents.add(self.host_ueid)
            yield update

    def top_level_apache_processes(self):
        """Find top level Apache processes."""
        processes = {}
        try:
            binary = Apache.apache_binary()
        except ApacheNotFound:
            return []
        proc_gens = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs={'binary': binary})
        for entity in itertools.chain.from_iterable(proc_gens):
            processes[entity.attrs.get('pid').value] = entity
        return [e for e in processes.values()
                if e.attrs.get('ppid').value not in processes]

    def active_apaches(self):
        """Return running apache instances on this machine."""
        for proc in self.top_level_apache_processes():
            try:
                apache = Apache(proc)
            except ApacheNotFound:
                continue
            else:
                yield apache

    @staticmethod
    def create_vhost(address, port, apache):
        """Create a VHost Entity"""
        vhost = entityd.EntityUpdate('ApacheVHost')
        vhost.label = "{}:{}".format(address, port)
        vhost.attrs.set('address', address, traits={'entity:id'})
        vhost.attrs.set('port', port, traits={'entity:id'})
        vhost.attrs.set('apache', str(apache.ueid),
                        traits={'entity:id', 'entity:ueid'})
        return vhost


VHost = collections.namedtuple('VHost', ['address', 'port', 'config_path'])


class ApacheNotFound(Exception):
    """Raised if Apache is not running, or the binaries are not found."""
    pass


class Apache:
    """Class providing access to Apache information.

    Keeps track of relevant binaries and config files.

    By default, config path will be discovered via the apache binary.
    If a config path is set on the Apache process command line, then that
    will be used instead.

    The Apache binaries will be shared across instances. If they cannot be
    found, then instantiating an instance will fail.
    """

    _apache_binary = None
    _apachectl_binary = None

    def __init__(self, proc=None):
        self._version = None
        self._config_path = None
        self.main_process = proc
        # Call these so that if they are missing, we fail early.
        self.apache_binary()
        self.apachectl_binary()

    @classmethod
    def apachectl_binary(cls):
        """The binary to call to get apache status.

        :returns: String, the apachectl command
        :raises ApacheNotFound: If the Apache binary is not discovered.
        """
        if not cls._apachectl_binary:
            apache_binarys = ['apachectl', 'apache2ctl', 'httpd']
            for name in apache_binarys:
                try:
                    subprocess.check_call([name, '-S'],
                                          stdout=subprocess.DEVNULL,
                                          stderr=subprocess.STDOUT)
                except FileNotFoundError:
                    continue
                except subprocess.CalledProcessError:
                    cls._apachectl_binary = name
                    break
                else:
                    cls._apachectl_binary = name
                    break
            else:
                raise ApacheNotFound("Couldn't find binary for Apache.")
        return cls._apachectl_binary

    @classmethod
    def apache_binary(cls):
        """The binary to check for in process lists.

        :returns: String, the apache command
        :raises ApacheNotFound: If the Apache binary is not discovered.
        """
        if not cls._apache_binary:
            apache_binarys = ['apache2', 'httpd']
            for name in apache_binarys:
                try:
                    subprocess.check_call([name, '-S'],
                                          stdout=subprocess.DEVNULL,
                                          stderr=subprocess.STDOUT)
                except FileNotFoundError:
                    continue
                except subprocess.CalledProcessError:
                    cls._apache_binary = name
                    break
                else:
                    cls._apache_binary = name
                    break
            else:
                raise ApacheNotFound("Couldn't find binary for Apache.")
        return cls._apache_binary

    @property
    def config_path(self):
        """The root configuration file."""
        if not self._config_path:
            self._config_path = self.apache_config()
        return self._config_path

    @property
    def version(self):
        """The Apache version as a string."""
        if not self._version:
            output = subprocess.check_output([self.apachectl_binary(), '-v'],
                                             universal_newlines=True)
            lines = output.split('\n')
            self._version = lines[0].split(':')[1].strip()
        return self._version

    def check_config(self):
        """Check if the config passes basic checks."""
        try:
            exit_code = subprocess.check_call(
                [self.apachectl_binary(), '-t', '-f', self.config_path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT)
            return exit_code == 0
        except subprocess.CalledProcessError:
            return False

    def config_last_modified(self):
        """Return the most recent last modified date on config files."""
        config_files = self.find_all_includes()
        return max(os.path.getmtime(file) for file in
                   [self.config_path] + config_files)

    def performance_data(self):
        """Apache performance information from mod_status.

        :returns: Dictionary with performance data.
        """
        perfdata = {}
        response = None
        for vhost in self.vhosts():
            addr, port = vhost.address, vhost.port
            try:
                response = self.get_apache_status(addr, port)
                break
            except ApacheNotFound:
                continue
        if not response:
            raise ApacheNotFound('Could not find address for Apache status')
        lines = response.text.split('\n')
        for line in lines:
            if line.startswith('Total Accesses'):
                perfdata['TotalAccesses'] = (int(line.split(':')[1].strip()),
                                             {'metric:counter'})
            elif line.startswith('Total kBytes'):
                perfdata['TotalkBytes'] = (int(line.split(':')[1].strip()),
                                           {'metric:counter', 'unit:bytes'})
            elif line.startswith('Uptime'):
                key, value, *_ = [s.strip() for s in line.split(':')]
                perfdata[key] = (
                    float(value),
                    {'metric:counter', 'time:duration', 'unit:seconds'})
            elif line.startswith(('BusyWorkers', 'IdleWorkers',
                                  'ConnsTotal', 'ConnsAsyncWriting',
                                  'ConnsAsyncKeepAlive', 'ConnsAsyncClosing')):
                key, value, *_ = [s.strip() for s in line.split(':')]
                perfdata[key] = (int(value), {'metric:gauge'})
            elif line.startswith(('CPULoad', 'ReqPerSec', 'BytesPerSec',
                                  'BytesPerReq')):
                key = line.split(':')[0]
                perfdata[key] = (float(
                    line.split(':')[1].strip()), {'metric:gauge'})
                if line.startswith(('BytesPerSec', 'BytesPerReq')):
                    perfdata[key][1].add('unit:bytes')
            elif line.startswith('Scoreboard:'):
                scoreboard = line.split(':')[1].strip()
                names = {
                    '_': 'workers:waiting',
                    'S': 'workers:starting',
                    'R': 'workers:reading',
                    'W': 'workers:sending',
                    'K': 'workers:keepalive',
                    'D': 'workers:dns',
                    'C': 'workers:closing',
                    'L': 'workers:logging',
                    'G': 'workers:finishing',
                    'I': 'workers:idle',
                    '.': 'workers:open'
                }
                for symbol, desc in names.items():
                    perfdata[desc] = (scoreboard.count(symbol),
                                      {'metric:gauge'})
        return perfdata

    def vhosts(self):
        """Get addresses where Apache is listening"""
        patterns = [r'(?P<addr>\*):(?P<port>\d+)',
                    r'port (?P<port>\d+) namevhost (?P<addr>[^ ]+)',
                    r'(?P<addr>\d+\.\d+\.\d+\.\d+):(?P<port>\d+)']
        lines = subprocess.check_output(
            [self.apachectl_binary(),
             '-d', os.path.dirname(self.config_path),
             '-f', self.config_path,
             '-t', '-D', 'DUMP_VHOSTS'],
            universal_newlines=True).split('\n')
        vhosts = set()
        for line in lines:
            for pattern in patterns:
                match = re.search(pattern, line)
                if match:
                    port = match.group('port')
                    addr = match.group('addr')
                    if addr == '*':
                        addr = 'localhost'
                    path = re.search(r'\(([^:]+):(\d+)\)', line)
                    config_file = ''
                    if path:
                        config_file = path.group(1)
                    vhosts.add(VHost(addr, int(port), config_file))
        return vhosts

    def apache_config(self):
        """Find the location of apache config files."""
        config_file = config_path = None
        try:
            output = subprocess.check_output([self.apachectl_binary(), '-V'],
                                             universal_newlines=True)
        except subprocess.CalledProcessError:
            raise ApacheNotFound('Could not call apachectl binary {}.'.format(
                self.apache_binary))

        if output:
            for line in output.split('\n'):
                if line.startswith(' -D HTTPD_ROOT='):
                    config_path = line.split('"')[1]
                if line.startswith(' -D SERVER_CONFIG_FILE='):
                    config_file = line.split('"')[1]
        if self.main_process:
            parser = argparse.ArgumentParser()
            parser.add_argument('-d')
            parser.add_argument('-f')
            args, _ = parser.parse_known_args(
                shlex.split(self.main_process.attrs.get('command').value))
            if args.f:
                config_file = args.f
            if args.d:
                config_path = args.d
        if config_file and config_path:
            return os.path.join(config_path, config_file)
        raise ApacheNotFound('Apache config not found')

    def find_all_includes(self):
        """Find all included config files in this file.

        :returns: A list of string file paths.
        """
        include_globs = []
        config_path = pathlib.Path(self.config_path)
        with config_path.open() as config:
            for line in config:
                if line.strip().startswith('Include'):
                    include_globs.append(line.split()[1].strip())

        includes = []
        for pattern in include_globs:
            files = config_path.parent.glob(pattern)  # pylint: disable=no-member
            includes.extend(map(str, files))
        return includes

    @staticmethod
    def get_apache_status(addr, port):
        """Gets the response from Apache's server-status page.

        :returns: requests.Response with the result.
        :raises ApacheNotFound: If the Apache binary is not discovered.
        """
        status_url = 'http://{}:{}/server-status?auto'.format(addr, port)
        try:
            response = requests.get(status_url)
        except requests.exceptions.ConnectionError:
            raise ApacheNotFound(
                'Running Apache server with mod_status not found at {}'
                .format(status_url))
        else:
            return response

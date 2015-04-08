"""Entity providing monitoring information on an Apache2 service.

This has been tested on CentOS 7 and Ubuntu 14.04.
In order to get Apache entities, Apache must be configured with mod_status
accessible on the localhost ip address. This is enabled by default on
Ubuntu, but for CentOS a section needs to be added to httpd.conf:

<Location /server-status>
        SetHandler server-status
        Require local
</Location>

"""

import argparse
import logging
import os
import pathlib
import shlex
import subprocess

import requests

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
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Apache" Monitored Entities."""
        if name == 'Apache':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported '
                                  'for attrs {}'.format(attrs))
            return self.entities()

    @property
    def host_ueid(self):
        """Get and store the host entity."""
        if self._host_ueid:
            return self._host_ueid
        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Host', attrs=None)
        for hosts in results:
            for host in hosts:
                self._host_ueid = host.ueid
                return self._host_ueid

    def entities(self):
        """Return a generator of ApacheEntity objects"""
        apache_instances = self.active_apaches()
        for apache in apache_instances:
            try:
                perfdata = apache.performance_data()
            except ApacheNotFound:
                continue
            update = entityd.EntityUpdate('Apache')
            update.label = 'Apache'
            update.attrs.set('host', self.host_ueid, attrtype='id')
            update.attrs.set('version', apache.version)
            update.attrs.set('config_path', apache.config_path, attrtype='id')
            update.attrs.set('config_ok', apache.check_config())
            update.attrs.set('config_last_mod', apache.config_last_modified())
            for name, value in perfdata.items():
                update.attrs.set(name, value)
            update.children.add(apache.main_process)
            if self.host_ueid:
                update.parents.add(self.host_ueid)
            yield update

    def top_level_apache_processes(self):
        """Find top level Apache processes."""
        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs={'binary': _apache_binary()})
        for generator in results:
            process_table = {e.attrs.get('pid').value: e for e in generator}
            if not process_table:
                continue
            parents = {e.attrs.get('ppid').value for e in process_table.values()}
            top_level_processes = [process_table.get(pid) for pid in parents]
            return [proc for proc in top_level_processes if proc]
        return []

    def active_apaches(self):
        """Return running apache instances on this machine."""

        return [Apache(proc) for proc in self.top_level_apache_processes()]


class ApacheNotFound(Exception):
    """Raised if Apache is not running, or the binaries are not found."""
    pass


class Apache:
    """Class providing access to Apache information.

    Keeps track of relevant binaries and config files.

    By default, config path will be discovered via the apache binary.
    If a path is passed in instead, then that will be passed to the
    calls.
    """

    def __init__(self, proc=None):
        self._apachectl_binary = None
        self._apache_binary = None
        self._version = None
        self._config_path = None
        self.main_process = proc

    @property
    def apachectl_binary(self):
        """The binary to call to get apache status."""
        if not self._apachectl_binary:
            try:
                self._apachectl_binary = _apachectl_binary()
            except ApacheNotFound:
                return None
        return self._apachectl_binary

    @property
    def apache_binary(self):
        """The binary to check for in process lists."""
        if not self._apache_binary:
            try:
                self._apache_binary = _apache_binary()
            except ApacheNotFound:
                return None
        return self._apache_binary

    @property
    def config_path(self):
        """The root configuration file."""
        if not self._config_path:
            self._config_path = _apache_config(self.apachectl_binary,
                                               self.main_process)
        return self._config_path

    @property
    def version(self):
        """The Apache version as a string."""
        if not self._version:
            self._version = _version(self.apachectl_binary)
        return self._version

    def check_config(self, path=None):
        """Check if the config passes basic checks.

        :param path: Optionally supply a config file path to check.
        """
        if path is None:
            path = self.config_path
        try:
            exit_code = subprocess.check_call(
                [self.apachectl_binary, '-t', '-f', path],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT)
            return exit_code == 0
        except subprocess.CalledProcessError:
            return False

    def config_last_modified(self):
        """Return the most recent last modified date on config files."""
        config_files = _find_all_includes(self.config_path)
        return max(os.path.getmtime(file) for file in
                   [self.config_path] + config_files)

    @staticmethod
    def performance_data():
        """Apache performance information from mod_status.

        :returns: Dictionary with performance data.
        """
        perfdata = {}
        response = _get_apache_status()
        lines = response.text.split('\n')
        for line in lines:
            if line.startswith('Total Accesses'):
                perfdata['TotalAccesses'] = int(line.split(':')[1].strip())
            elif line.startswith('Total kBytes'):
                perfdata['TotalkBytes'] = int(line.split(':')[1].strip())
            elif line.startswith(('Uptime', 'BusyWorkers', 'IdleWorkers',
                                  'ConnsTotal', 'ConnsAsyncWriting',
                                  'ConnsAsyncKeepAlive', 'ConnsAsyncClosing')):
                perfdata[line.split(':')[0]] = int(line.split(':')[1].strip())
            elif line.startswith(('CPULoad', 'ReqPerSec', 'BytesPerSec',
                                  'BytesPerReq')):
                perfdata[line.split(':')[0]] = float(line.split(':')[1].strip())
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
                    perfdata[desc] = scoreboard.count(symbol)
        return perfdata


def _apache_config(binary, proc):
    """Find the location of apache config files.

    :param binary: The path to the apachectl binary to use.
    :param proc: The main process. This is used to check the process command,
       as Apache may be started with a config file or path argument specified.
    :raises: ApacheNotFound if Apache configuration is not found.
    """
    config_file = config_path = None
    try:
        output = subprocess.check_output([binary, '-V'], universal_newlines=True)
    except subprocess.CalledProcessError:
        raise ApacheNotFound('Could not call apachectl binary {}.'.format(binary))

    if output:
        for line in output.split('\n'):
            if line.startswith(' -D HTTPD_ROOT='):
                config_path = line.split('"')[1]
            if line.startswith(' -D SERVER_CONFIG_FILE='):
                config_file = line.split('"')[1]
    if proc:
        parser = argparse.ArgumentParser()
        parser.add_argument('-d')
        parser.add_argument('-f')
        args, _ = parser.parse_known_args(
            shlex.split(proc.attrs.get('command').value))
        if args.f:
            config_file = args.f
        if args.d:
            config_path = args.d
    if config_file and config_path:
        return os.path.join(config_path, config_file)
    raise ApacheNotFound('Apache config not found')


def _apachectl_binary():
    """Find the installed apachectl executable

    :raises: ApacheNotFound if Apache binaries are not found.
    """
    apache_binarys = ['apachectl', 'apache2ctl', 'httpd']
    for name in apache_binarys:
        try:
            subprocess.check_call([name, '-S'],
                                  stdout=subprocess.DEVNULL,
                                  stderr=subprocess.STDOUT)
        except FileNotFoundError:
            continue
        except subprocess.CalledProcessError:
            return name
        else:
            return name
    raise ApacheNotFound('Apache executable not found.')


def _apache_binary():
    """Get the process that Apache will be running as.

    :param binary: The path to the apachectl binary to use.
    """
    apache_binarys = ['apache2', 'httpd']
    for name in apache_binarys:
        try:
            subprocess.check_call([name, '-S'],
                                  stdout=subprocess.DEVNULL,
                                  stderr=subprocess.STDOUT)
        except FileNotFoundError:
            continue
        except subprocess.CalledProcessError:
            return name
        else:
            return name
    raise ApacheNotFound('Apache executable not found.')


def _version(binary):
    """Get the Apache version string."""
    output = subprocess.check_output([binary, '-v'], universal_newlines=True)
    lines = output.split('\n')
    version = lines[0].split(':')[1].strip()
    return version


def _find_all_includes(config_file_path):
    """Find all included config files in this file.

    :param config_file_path: The path to check.
    :returns: A list of string file paths.
    """
    include_globs = []
    config_file_path = pathlib.Path(config_file_path)
    with config_file_path.open() as config:
        for line in config:
            if line.strip().startswith(('Include', 'IncludeOptional')):
                include_globs.append(line.split()[1].strip())

    includes = []
    for pattern in include_globs:
        files = config_file_path.parent.glob(pattern)  # pylint: disable=no-member
        includes.extend(map(str, files))
    return includes


def _get_apache_status():
    """Gets the response from Apache's server-status page.

    :returns: requests.Response with the result.
    """
    status_url = 'http://localhost/server-status?auto'
    try:
        response = requests.get(status_url)
    except requests.exceptions.ConnectionError:
        raise ApacheNotFound('Running Apache server with mod_status not found '
                             'at {}'.format(status_url))
    else:
        return response

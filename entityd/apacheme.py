"""Entity providing monitoring information on an Apache2 service"""

import glob
import os
import subprocess

import requests

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.apacheme':
        gen = ApacheEntity()
        pluginmanager.register(gen,
                               name='entityd.apacheme.ApacheEntity')


class ApacheEntity:
    """Class for all things related to Apache"""

    def __init__(self):
        self.session = None
        self._apache = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Apache Monitored Entity."""
        config.addentity('Apache', 'entityd.apacheme.ApacheEntity')

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
    def apache(self):
        if not self._apache:
            self._apache = Apache()
        return self._apache

    def entities(self):
        """Return a generator of ApacheEntity objects"""
        try:
            apache = self.apache
        except RuntimeError:
            return
        else:
            update = entityd.EntityUpdate('Apache')

            # TODO: ID attributes: Host+Config root? Root process?
            # TODO; add version
            # TODO: multiple Apache instances?
            update.attrs.set('id', 'Apache', attrtype='id')
            update.attrs.set('version', apache.version())
            update.attrs.set('config_path', apache.config_path)
            update.attrs.set('config_ok', apache.check_config())
            update.attrs.set('config_last_mod', apache.config_last_modified())
            perfdata = apache.performance_data()
            for name, value in perfdata.items():
                update.attrs.set(name, value)

            update.attrs.set('SitesEnabled', apache.sites_enabled())
            for child in self.processes():
                update.children.add(child)
            yield update

    def processes(self):
        """Find child processes for Apache"""
        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs={'binary': self.apache.apache_binary})
        for processes in results:
            yield from processes


class Apache:
    def __init__(self):
        self._apachectl_binary = None
        self._apache_binary = None
        self._config_path = None

    @property
    def apachectl_binary(self):
        if not self._apachectl_binary:
            self._apachectl_binary = _apachectl_binary()
        return self._apachectl_binary

    @property
    def apache_binary(self):
        if not self._apache_binary:
            self._apache_binary = _apache_binary(self.apachectl_binary)
        return self._apache_binary

    @property
    def config_path(self):
        if not self._config_path:
            self._config_path = _apache_config(self.apachectl_binary)
        return self._config_path

    def version(self):
        output = subprocess.check_output([self.apachectl_binary, '-v'])
        lines = output.split(b'\n')
        version = lines[0].split(b':')[1].strip()
        return version

    def is_running(self):
        """Is apache running on this host?"""


    def check_config(self, path=None):
        """Check if the config passes basic checks"""
        if path is None:
            path = self.config_path
        try:
            exit_code = subprocess.check_call([self.apachectl_binary, '-t',
                                               '-f', path])
            return exit_code == 0
        except subprocess.CalledProcessError:
            return False

    def config_last_modified(self):
        """Return the most recent last modified date on config files"""
        config_files = _find_all_includes(self.config_path)
        last_mod = None
        for file in [self.config_path] + config_files:
            file_lastmod = os.path.getmtime(file)
            if last_mod is None or file_lastmod > last_mod:
                last_mod = file_lastmod
        return last_mod

    @staticmethod
    def performance_data():
        """Return information from mod_status"""
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

    def sites_enabled(self):
        """Get a list of sites enabled

        Note: We can't actually send lists of items in attributes
        """
        sites = []
        output = subprocess.check_output([self.apachectl_binary, '-S'])
        for line in output.split(b'\n'):
            print(line)
            # TODO: Extract site details
            # TODO: These will be included as additional entities (child rel)
        return sites


def _apache_config(binary):
    """Find the location of apache config files"""
    output = subprocess.check_output([binary, '-V'])
    config_file = config_path = None
    for line in output.split(b'\n'):
        if line.startswith(b' -D HTTPD_ROOT='):
            config_path = line.split(b'"')[1]
        if line.startswith(b' -D SERVER_CONFIG_FILE='):
            config_file = line.split(b'"')[1]
    if config_file and config_path:
        return os.path.join(config_path, config_file)
    raise RuntimeError("Apache config not found")


def _apachectl_binary():
    """Find the installed apachectl executable

    :raises Runtime error if Apache is not installed.
    """
    apache_binarys = ['apachectl', 'apache2ctl', 'httpd']
    for name in apache_binarys:
        try:
            subprocess.check_output([name, '-S'])
        except FileNotFoundError:
            continue
        except subprocess.CalledProcessError:
            continue
        else:
            return name
    raise RuntimeError("Apache executable not found.")


def _apache_binary(apachectl_binary):
    """What process will Apache be running as"""
    if apachectl_binary in ['apachectl', 'apache2ctl']:
        return 'apache2'
    else:
        return 'httpd'


def _find_all_includes(config_file_path):
    """Find all included config files in ``config_file_path``."""
    include_globs = []
    with open(config_file_path, 'rb') as config:
        for line in config:
            if b'Include ' in line or b'IncludeOptional ' in line:
                include_globs.append(line.split()[1].strip())

    includes = []
    for pattern in include_globs:
        files = glob.glob(os.path.join(os.path.dirname(config_file_path),
                                       pattern))
        includes.extend(files)
    return includes


def _get_apache_status():
    status_url = 'http://127.0.1.1:80/server-status?auto'
    try:
        response = requests.get(status_url)
    except requests.exceptions.ConnectionError:
        raise RuntimeError("Running Apache server with mod_status not found "
                           "at {}".format(status_url))
    else:
        return response

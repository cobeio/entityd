"""Entity providing monitoring information on an Apache2 service"""

import glob
import os
import subprocess

import requests

import entityd


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

    def entities(self):
        """Return a generator of ApacheEntity objects"""
        update = entityd.EntityUpdate('Apache')

        # TODO: ID attributes: Host+Config root? Root process?
        # TODO; add version
        update.attrs.set('id', 'Apache', attrtype='id')
        configpath = apache_config()
        update.attrs.set('config_path', configpath)
        update.attrs.set('config_ok', check_config(configpath))
        update.attrs.set('config_last_mod', config_last_modified())
        perfdata = performance_data()
        for name, value in perfdata.items():
            update.attrs.set(name, value)

        update.attrs.set('SitesEnabled', sites_enabled())
        for child in self.relations():
            update.children.add(child)
        yield update

    # TODO: Have a class-level identifier whether we're using apache2 or httpd

    def relations(self):
        """Find relations for Apache

         - Processes
         - Endpoints
         """
        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs={'binary': apache_binary()})
        for processes in results:
            yield from processes


def check_config(configpath):
    """Check if the Apache config passes basic checks.

    Return True config is OK, False otherwise.
    """
    binary = apachectl_binary()
    try:
        exit_code = subprocess.check_call([binary, '-t', '-f', configpath])
        return exit_code == 0
    except subprocess.CalledProcessError:
        return False


def apache_config():
    """Find the location of apache config files"""
    binary = apachectl_binary()
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


def apachectl_binary():
    """Find the installed apachectl executable"""
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


def apache_binary():
    """What process will Apache be running as"""
    binary = apachectl_binary()
    if binary in ['apachectl', 'apache2ctl']:
        return 'apache2'
    else:
        return 'httpd'


def sites_enabled():
    """Get a list of sites enabled

    Note: We can't actually send lists of items in attributes
    """
    sites = []
    binary = apachectl_binary()
    output = subprocess.check_output([binary, '-S'])
    for line in output.split(b'\n'):
        print(line)
        # TODO: Extract site details
        # TODO: These will be included as additional entities (child rel)
    return sites


def config_last_modified():
    """The datetime of the last modified config file"""
    config_file = apache_config()
    config_files = find_all_includes(config_file)
    last_mod = None
    for file in [config_file] + config_files:
        file_lastmod = os.path.getmtime(file)
        if last_mod is None or file_lastmod > last_mod:
            last_mod = file_lastmod
    return last_mod


def find_all_includes(config_file_path):
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


def performance_data():
    """From server-status, get some performance details"""
    perfdata = {}
    status_url = 'http://127.0.1.1:80/server-status?auto'
    try:
        response = requests.get(status_url)
    except requests.exceptions.ConnectionError:
        raise RuntimeError("Running Apache server with mod_status not found "
                           "at {}".format(status_url))

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

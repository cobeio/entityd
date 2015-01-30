"""Entity providing monitoring information on an Apache2 service

Monitoring:

Sites available
Sites enabled

Config file test [pass/fail]

Apache specific performance details:
memory
threads
cpu load
requests
bandwidth
active connections
Errors 404 / 500
Response time
Packet loss
Uptime
Idle Workers
Busy Workers

Relations
Child and parent processes
Endpoints
Config files, log files

"""

import subprocess
import types

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
        pass

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Apache Monitored Entity."""
        config.addentity('Apache', 'entityd.apacheme.ApacheEntity')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Apache" Monitored Entities."""
        if name == 'Apache':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported '
                                  'for attrs {}'.format(attrs))
            return self.entities()

    @staticmethod
    def entities():
        """Return a generator of ApacheEntity objects"""
        update = entityd.EntityUpdate('Apache')
        update.attrs.set('config_ok', check_config())
        perfdata = performance_data()
        update.attrs.set('TotalAccesses', perfdata.TotalAccesses)
        update.attrs.set('Waiting', perfdata.waiting)

        yield update


def check_config():
    """Check if the Apache config passes basic checks.

    Return True config is OK, False otherwise.
    """
    try:
        exit_code = subprocess.check_call(['apachectl', 'configtest'])
        return exit_code == 0
    except subprocess.CalledProcessError:
        return False


def apache_config():
    """Find the location of apache config files"""


def sites_available():
    """Get a list of sites available"""


def sites_enabled():
    """Get a list of sites enabled"""


def performance_data():
    """From server-status, get some performance details"""
    perfdata = types.SimpleNamespace()
    response = requests.get('http://127.0.1.1:80/server-status?auto')
    lines = response.text.split('\n')
    for line in lines:
        if line.startswith('Total Accesses'):
            perfdata.TotalAccesses = int(line.split(':')[1].strip())
        elif line.startswith('Total kBytes'):
            perfdata.TotalAccesses = int(line.split(':')[1].strip())
        elif line.startswith(('Uptime', 'BusyWorkers', 'IdleWorkers',
                              'ConnsTotal', 'ConnsAsyncWriting',
                              'ConnsAsyncKeepAlive', 'ConnsAsyncClosing')):
            setattr(perfdata,
                    line.split(':')[0],
                    int(line.split(':')[1].strip()))
        elif line.startswith(('CPULoad', 'ReqPerSec', 'BytesPerSec',
                              'BytesPerReq')):
            setattr(perfdata,
                    line.split(':')[0],
                    float(line.split(':')[1].strip()))
        elif line.startswith('Scoreboard:'):
            scoreboard = line.split(':')[1].strip()
            names = {
                '_': 'waiting',
                'S': 'starting',
                'R': 'reading',
                'W': 'sending',
                'K': 'keepalive',
                'D': 'dns',
                'C': 'closing',
                'L': 'logging',
                'G': 'finishing',
                'I': 'idle',
                '.': 'open'
            }
            for symbol, desc in names.items():
                setattr(perfdata, desc, scoreboard.count(symbol))

    return perfdata

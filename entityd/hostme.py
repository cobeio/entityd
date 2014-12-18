"""Plugin providing the Host Monitored Entity."""

import socket
import time
import uuid

import syskit

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.hostme':
        gen = HostEntity()
        pluginmanager.register(gen,
                               name='entityd.hostme.HostEntity')


class HostEntity:
    """Plugin to generate Host MEs."""

    def __init__(self):
        self.host_uuid = None
        self.session = None

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Called when the monitoring session starts."""
        self.session = session

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Host Monitored Entity."""
        config.addentity('Host', 'entityd.hostme.HostEntity')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Host" Monitored Entities."""
        if name == 'Host':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.hosts()

    def get_uuid(self):
        """Get a uuid for host."""
        if self.host_uuid:
            return self.host_uuid
        key = 'entityd.hostme'
        try:
            value = self.session.svc.kvstore.get(key)
        except KeyError:
            value = uuid.uuid4().hex
            self.session.svc.kvstore.add(key, value)
        self.host_uuid = value
        return value

    def hosts(self):
        """Generator of Host MEs."""
        fqdn = socket.getfqdn()
        uptime = int(syskit.uptime())
        yield {
            'type': 'Host',
            'uuid': self.get_uuid(),
            'timestamp': time.time(),
            'attrs': {
                'fqdn': {
                    'value': fqdn,
                },
                'uptime': {
                    'value': uptime,
                    'type': "perf:counter"
                }
            }
        }

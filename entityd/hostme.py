"""Plugin providing the Host Monitored Entity."""
import socket
import time
import uuid

import syskit

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    if name == 'entityd.hostme':
        gen = HostEntity()
        pluginmanager.register(gen,
                               name='entityd.hostme.HostEntity')


class HostEntity:

    def __init__(self):
        self.known_uuids = {}
        self.session = None

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        self.session = session

    @entityd.pm.hookimpl
    def entityd_configure(self, config):
        """Register the Host Monitored Entity."""
        config.addentity('Host', 'entityd.hostme.HostEntity')

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Host" Monitored Entities."""
        if name == 'Host':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.hosts()

    def get_uuid(self, fqdn):
        """Get a uuid for fqdn if one exists, else generate one.

        :param fqdn: Fully qualified domain name of the host
        """
        key = 'entityd.hostme:{}'.format(fqdn)
        if key in self.known_uuids:
            return self.known_uuids[key]

        value = self.session.pluginmanager.hooks.entityd_kvstore_get(key=key)
        if not value:
            value = uuid.uuid4().hex
            self.session.pluginmanager.hooks.entityd_kvstore_put(key=key,
                                                                 value=value)

        self.known_uuids[key] = value
        return value

    def hosts(self):
        """Generator of Host MEs."""
        fqdn = socket.getfqdn()
        uptime = int(syskit.uptime())

        yield {
            'type': 'Host',
            'uuid': self.get_uuid(fqdn),
            'timestamp': time.time(),
            'attrs': {
                'fqdn': fqdn,
                'uptime': {
                    'value': uptime,
                    'type': "perf:counter"
                }
            }
        }

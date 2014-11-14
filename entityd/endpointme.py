"""Plugin providing the Endpoint Monitored Entity."""

import os
import uuid

import syskit

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.endpointme':
        gen = EndpointEntity()
        pluginmanager.register(gen,
                               name='entityd.endpointme.EndpointEntity')


class EndpointEntity:
    """Plugin to generate endpoint MEs."""

    def __init__(self):
        self.endpoint_uuid = None
        self.session = None

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Endpoint" Monitored Entities."""
        if name == 'Endpoint':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.endpoints()

    def get_uuid(self):
        """Get a uuid for endpoint."""
        value = uuid.uuid4().hex
        return value

    def endpoints(self):
        processes = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs=None)

        for process in processes:
            endpoints = syskit.Process(process['attrs']['pid']).connections
            for conn in endpoints:
                yield {
                    'type': 'Endpoint',
                    'uuid': self.get_uuid(),
                    'attrs': {
                        'local_addr': conn.laddr,
                        'remote_addr': conn.raddr
                    },
                    'relations': [
                        {}
                    ]
                }

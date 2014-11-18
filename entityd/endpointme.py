"""Plugin providing the Endpoint Monitored Entity."""

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

    @staticmethod
    def get_uuid():
        """Get a uuid for endpoint."""

        value = uuid.uuid4().hex
        return value

    def endpoints(self):
        """Generator of all endpoints."""
        processes, = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs=None)

        for process in processes:
            yield from self.endpoints_for_process(process)

    def endpoints_for_process(self, proc):
        """Generator of endpoints for the provided process.

        :param proc: Process Entity owning the connections to look for.
                     Must have a uuid & pid
        """
        endpoints = syskit.Process(proc['attrs']['pid']).connections
        for conn in endpoints:
            yield {
                'type': 'Endpoint',
                'uuid': self.get_uuid(),
                'attrs': {
                    'local_addr': conn.laddr,
                    'remote_addr': conn.raddr
                },
                'relations': [
                    {
                        'uuid': proc['uuid'],
                        'type': 'me:Process',
                        'rel': 'parent'
                    }
                ]
            }

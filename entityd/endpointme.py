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
        self.known_uuids = {}
        self.session = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Endpoint Monitored Entity."""
        config.addentity('Endpoint', 'entityd.endpointme.EndpointEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Load up all the known endpoint UUIDs."""
        self.session = session
        loaded_values = \
            self.session.svc.kvstore.getmany(
                prefix='entityd.endpointme:'
            )
        if loaded_values:
            self.known_uuids = loaded_values
        else:
            self.known_uuids = {}

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Store out all our known endpoint UUIDs."""
        self.session.svc.kvstore.deletemany(
            prefix='entityd.endpointme:'
        )

        self.session.svc.kvstore.addmany(
            mapping=self.known_uuids
        )

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Endpoint" Monitored Entities."""
        if name == 'Endpoint':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.endpoints()

    @staticmethod
    def _cache_key(pid, fd):
        """Get a standard cache key for an Endpoint entity."""
        return 'entityd.endpointme:{}-{}'.format(pid, fd)

    def get_uuid(self, conn):
        """Get a uuid for this endpoint if one exists, else generate one.

        :param fd: Endpoint fd
        """
        key = self._cache_key(conn.bound_pid, conn.fd)
        if self.known_uuids and key in self.known_uuids:
            return self.known_uuids[key]
        else:
            value = uuid.uuid4().hex
            self.known_uuids[key] = value
            return value

    def forget_entity(self, bound_pid, fd):
        """Remove the cached version of this Endpoint Entity."""
        key = self._cache_key(bound_pid, fd)
        try:
            del self.known_uuids[key]
        except KeyError:
            pass

    def endpoints(self):
        """Generator of all endpoints.

        Yields all connections of all active processes.
        """
        processes, = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Process', attrs=None)

        for process in processes:
            if 'delete' in process and process['delete']:
                continue
            yield from self.endpoints_for_process(process)

    def endpoints_for_process(self, proc):
        """Generator of endpoints for the provided process.

        :param proc: Process Entity (dict) owning the connections to look for.
                     Must have a uuid & pid
        """
        endpoints = syskit.Process(proc['attrs']['pid']).connections
        for conn in endpoints:
            yield {
                'type': 'Endpoint',
                'uuid': self.get_uuid(conn),
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

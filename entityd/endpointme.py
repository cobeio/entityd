"""Plugin providing the Endpoint Monitored Entity."""

import socket
import uuid

import entityd
import entityd.connections
import entityd.pm

FAMILIES = {
    socket.AF_INET: 'INET',
    socket.AF_INET6: 'INET6',
    socket.AF_UNIX: 'UNIX'
}

PROTOCOLS = {
    socket.SOCK_STREAM: 'TCP',
    socket.SOCK_DGRAM: 'UDP',
    socket.SOCK_RAW: 'RAW'
}


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
        self.known_uuids = session.svc.kvstore.getmany('entityd.endpointme:')

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Store out all our known endpoint UUIDs."""
        self.session.svc.kvstore.deletemany('entityd.endpointme:')
        self.session.svc.kvstore.addmany(self.known_uuids)

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Endpoint" Monitored Entities."""
        if name == 'Endpoint':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.endpoints()

    @staticmethod
    def _cache_key(pid, fd):
        """Get a standard cache key for an Endpoint entity.

        :param pid: Process ID owning the Endpoint
        :param fd: File descriptor of the Endpoint
        """
        return 'entityd.endpointme:{}-{}'.format(pid, fd)

    def get_uuid(self, conn):
        """Get a uuid for this endpoint if one exists, else generate one.

        :param conn: a syskit.Connection
        """
        key = self._cache_key(conn.bound_pid, conn.fd)
        if key in self.known_uuids:
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

    def endpoints(self, pid=None):
        """Generator of all endpoints.

        Yields all connections of all active processes.

        :param pid: Optional. Find only connections for this process.
        """
        connections = entityd.connections.Connections()
        for conn in connections.retrieve('inet', pid):
            process = None
            if conn.bound_pid:
                results = self.session.pluginmanager.hooks.entityd_find_entity(
                    name='Process', attrs={'pid': conn.bound_pid})
                if results:
                    process = next(iter(results[0]))

            if process and process.deleted:
                continue

            update = entityd.EntityUpdate('Endpoint')
            update.attrs.set('addr', conn.laddr[0], attrtype='id')
            update.attrs.set('port', conn.laddr[1], attrtype='id')

            update.attrs.set('family', FAMILIES.get(conn.family),
                             attrtype='id')
            update.attrs.set('protocol', PROTOCOLS.get(conn.type),
                             attrtype='id')
            update.attrs.set('listening', conn.status == 'LISTEN')

            if conn.raddr:
                # Remote endpoint relation goes in parents and children
                remote = entityd.EntityUpdate(metype='Endpoint')
                remote.attrs.set('addr', conn.raddr[0], attrtype='id')
                remote.attrs.set('port', conn.raddr[1], attrtype='id')
                remote.attrs.set('family', update.attrs.getvalue('family'),
                                 attrtype='id')
                remote.attrs.set('protocol', update.attrs.getvalue('protocol'),
                                 attrtype='id')
                update.parents.add(remote.ueid)
                update.children.add(remote.ueid)

            if process:
                update.parents.add(process)
            yield update

    def endpoints_for_process(self, pid):
        """Generator of endpoints for the provided process.

        :param pid: Process ID which owns the endpoints returned
        """
        return self.endpoints(pid)

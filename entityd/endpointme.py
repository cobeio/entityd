"""Plugin providing the Endpoint Monitored Entity."""

import socket

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
        self.known_ueids = {}
        self.active_endpoints = {}
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
        self.known_ueids = session.svc.kvstore.getmany('entityd.endpointme:')

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Store out all our known endpoint UUIDs."""
        self.session.svc.kvstore.deletemany('entityd.endpointme:')
        self.session.svc.kvstore.addmany(self.known_ueids)

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs):
        """Return an iterator of "Endpoint" Monitored Entities."""
        if name == 'Endpoint':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.endpoints()

    @staticmethod
    def _cache_key(addr, family, type_):
        """Get a standard cache key for an Endpoint entity.

        :param pid: Process ID owning the Endpoint
        :param fd: File descriptor of the Endpoint
        """
        return 'entityd.endpointme:{}-{}-{}'.format(addr, family, type_)

    def get_ueid(self, conn):
        """Get a ueid for this endpoint if one exists, else generate one.

        :param conn: an entityd.connections.Connection
        """
        key = self._cache_key(conn.laddr, conn.family, conn.type)
        if key in self.known_ueids:
            return self.known_ueids[key]
        ueid = self.create_local_update(conn).ueid
        self.known_ueids[key] = ueid
        return ueid

    @staticmethod
    def create_local_update(conn):
        """Create a basic Endpoint update, with no relations.

        Useful for getting the UEID without constructing a complete update.
        """
        update = entityd.EntityUpdate('Endpoint')
        update.attrs.set('addr', conn.laddr[0], attrtype='id')
        update.attrs.set('port', conn.laddr[1], attrtype='id')

        update.attrs.set('family', FAMILIES.get(conn.family),
                         attrtype='id')
        update.attrs.set('protocol', PROTOCOLS.get(conn.type),
                         attrtype='id')
        update.attrs.set('listening', conn.status == 'LISTEN')
        return update

    def create_update(self, conn):
        """Create an EntityUpdate from a Connection."""
        update = self.create_local_update(conn)
        if conn.bound_pid:
            results = self.session.pluginmanager.hooks.entityd_find_entity(
                name='Process', attrs={'pid': conn.bound_pid})
            if results:
                process = next(iter(results[0]))
                if process.deleted:
                    return None
                else:
                    update.parents.add(process)
        if conn.raddr:
            # Remote endpoint relation goes in parents and children
            remote = self.get_remote_endpoint(conn)
            update.parents.add(remote.ueid)
            update.children.add(remote.ueid)
        return update

    @staticmethod
    def get_remote_endpoint(conn):
        """Get the UEID of the remote Endpoint of conn."""
        remote = entityd.EntityUpdate(metype='Endpoint')
        remote.attrs.set('addr', conn.raddr[0], attrtype='id')
        remote.attrs.set('port', conn.raddr[1], attrtype='id')
        remote.attrs.set('family',
                         FAMILIES.get(conn.family),
                         attrtype='id')
        remote.attrs.set('protocol',
                         PROTOCOLS.get(conn.type),
                         attrtype='id')
        return remote

    def forget_entity(self, update):
        """Remove the cached version of this Endpoint Entity."""

        try:
            key = self._cache_key((update.attrs.get('addr').value,
                                   update.attrs.get('port').value),
                                  update.attrs.get('family').value,
                                  update.attrs.get('protocol').value)
            del self.known_ueids[key]
        except KeyError:
            key_to_delete = None
            for key, value in self.known_ueids.items():
                if value == update.ueid:
                    key_to_delete = key
            if key_to_delete:
                del self.known_ueids[key_to_delete]

    def endpoints(self, pid=None):
        """Generator of all endpoints.

        Yields all connections of all active processes.

        :param pid: Optional. Find only connections for this process.
        """
        previous_endpoints = self.active_endpoints
        self.active_endpoints = {}
        connections = entityd.connections.Connections()
        for conn in connections.retrieve('inet', pid):
            update = self.create_update(conn)
            if update:
                self.active_endpoints[update.ueid] = update
                yield update

        deleted_ueids = ((set(previous_endpoints.keys()) |
                          set(self.known_ueids.values())) -
                         set(self.active_endpoints.keys()))

        for endpoint_ueid in deleted_ueids:
            try:
                update = previous_endpoints[endpoint_ueid]
            except KeyError:
                update = entityd.EntityUpdate('Endpoint', ueid=endpoint_ueid)
            update.delete()
            self.forget_entity(update)
            yield update

    def endpoints_for_process(self, pid):
        """Generator of endpoints for the provided process.

        :param pid: Process ID which owns the endpoints returned
        """
        return self.endpoints(pid)

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


class EndpointEntity:
    """Plugin to generate endpoint MEs."""

    prefix = 'entityd.endpointme:'

    def __init__(self):
        self.session = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Register the Endpoint Monitored Entity."""
        config.addentity('Endpoint', 'entityd.endpointme.EndpointEntity')

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Store the session for later usage."""
        self.session = session

    @entityd.pm.hookimpl
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
        """Return an iterator of "Endpoint" Monitored Entities."""
        if name == 'Endpoint':
            if attrs is not None:
                raise LookupError('Attribute based filtering not supported')
            return self.endpoints()

    def get_ueid(self, conn):
        """Get a ueid for this endpoint if one exists, else generate one.

        :param conn: an entityd.connections.Connection
        """
        ueid = self.create_local_update(conn).ueid
        return ueid

    @staticmethod
    def create_local_update(conn):
        """Create a basic Endpoint update, with no relations.

        Useful for getting the UEID without constructing a complete update.
        """
        update = entityd.EntityUpdate('Endpoint')
        update.label = '{}:{}'.format(conn.laddr[0], conn.laddr[1])
        update.attrs.set('addr', conn.laddr[0], traits={'entity:id'})
        update.attrs.set('port', conn.laddr[1], traits={'entity:id'})

        update.attrs.set('family', FAMILIES.get(conn.family),
                         traits={'entity:id'})
        update.attrs.set('protocol', PROTOCOLS.get(conn.type),
                         traits={'entity:id'})
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
                if not process.exists:
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
        family = FAMILIES.get(conn.family)
        addrtraits = {'entity:id'}
        if family == 'INET':
            addrtraits.add('ipaddr:v4')
        elif family == 'INET6':
            addrtraits.add('ipaddr:v6')
        remote.attrs.set('addr', conn.raddr[0], traits=addrtraits)
        remote.attrs.set('port', conn.raddr[1], traits={'entity:id'})
        remote.attrs.set('family',
                         FAMILIES.get(conn.family),
                         traits={'entity:id'})
        remote.attrs.set('protocol',
                         PROTOCOLS.get(conn.type),
                         traits={'entity:id'})
        return remote

    def endpoints(self, pid=None):
        """Generator of all endpoints.

        Yields all connections of all active processes.

        :param pid: Optional. Find only connections for this process.
        """
        connections = entityd.connections.Connections()
        for conn in connections.retrieve('inet', pid):
            update = self.create_update(conn)
            if update:
                yield update

    def endpoints_for_process(self, pid):
        """Generator of endpoints for the provided process.

        :param pid: Process ID which owns the endpoints returned
        """
        return self.endpoints(pid)

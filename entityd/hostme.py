"""Plugin providing the Host Monitored Entity."""

import platform
import socket
import uuid

import syskit

import entityd.pm


class HostEntity:
    """Plugin to generate Host MEs."""

    def __init__(self):
        self.host_uuid = None
        self.session = None
        self.cputimes = None

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
    def entityd_find_entity(self, name, attrs, include_ondemand=False):  # pylint: disable=unused-argument
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
        update = entityd.EntityUpdate('Host')
        update.label = fqdn
        update.attrs.set('id', self.get_uuid(), {'entity:id'})
        update.attrs.set('fqdn', fqdn)
        update.attrs.set('uptime', uptime,
                         {'time:duration', 'unit:seconds', 'metric:counter'})
        update.attrs.set('boottime', syskit.boottime().timestamp(),
                         {'time:posix', 'unit:seconds'})
        load = syskit.loadavg()
        update.attrs.set('loadavg_1', load[0], {'metric:gauge'})
        update.attrs.set('loadavg_5', load[1], {'metric:gauge'})
        update.attrs.set('loadavg_15', load[2], {'metric:gauge'})
        memorystats = syskit.MemoryStats()
        free = memorystats.free + memorystats.buffers + memorystats.cached
        update.attrs.set('free', free, {'unit:bytes', 'metric:gauge'})
        update.attrs.set('total', memorystats.total,
                         {'unit:bytes', 'metric:gauge'})
        update.attrs.set('used', memorystats.total - free,
                         {'unit:bytes', 'metric:gauge'})
        update.attrs.set('os', platform.system())
        update.attrs.set('osversion', platform.release())
        self._add_cputime_attrs(update)
        yield update

    def _add_cputime_attrs(self, update):
        """Add cputimes and their % values to update

        The first call will return values since system boot; subsequent calls
        will return the values for the period in between calls.
        """
        attrs = ['usr', 'nice', 'sys', 'idle', 'iowait', 'irq', 'softirq',
                 'steal', 'guest', 'guest_nice']
        new_cputimes = syskit.cputimes()
        if self.cputimes:
            cputime_diff = [x - y for x, y in zip(new_cputimes, self.cputimes)]
        else:
            cputime_diff = new_cputimes
        cputimes = {attr: diff for attr, diff in zip(attrs, cputime_diff)}
        total = sum(cputimes.values())
        for attr in attrs:
            attr_name = 'cpu:' + attr
            update.attrs.set(attr, float(cputimes[attr]),
                             {'time:duration', 'unit:seconds'})
            if total == 0:
                update.attrs.set(attr_name, 0,
                                 {'metric:gauge', 'unit:percent'})
            else:
                update.attrs.set(
                    attr_name,
                    float(cputimes[attr]) / float(total) * 100,
                    {'metric:gauge', 'unit:percent'})
        self.cputimes = new_cputimes
        return None

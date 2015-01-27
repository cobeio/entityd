"""Plugin providing the Host Monitored Entity."""

import os
import socket
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
        update = entityd.EntityUpdate('Host')
        update.attrs.set('id', self.get_uuid(), 'id')
        update.attrs.set('fqdn', fqdn)
        update.attrs.set('uptime', uptime, 'perf:counter')
        update.attrs.set('boottime', syskit.boottime().timestamp())
        update.attrs.set('loadavg', syskit.loadavg())
        update.attrs.set('free', syskit.free())
        update.attrs.set('os', os.uname().sysname)
        update.attrs.set('osversion', os.uname().release)
        self._add_cputime_attrs(update)
        yield update

    def _add_cputime_attrs(self, update):
        """Add cputimes and their % values to update

        The first call will return values since system boot; subsequent calls
        will return the values for the period in between calls.
        """
        attrs = ['usr', 'sys', 'nice', 'idle', 'iowait', 'irq', 'softirq',
                 'steal']
        new_cputimes = syskit.cputimes()
        if self.cputimes:
            cputime_diff = [x - y for x, y in zip(new_cputimes, self.cputimes)]
        else:
            cputime_diff = new_cputimes
        cputimes = {attr: diff for attr, diff in zip(attrs, cputime_diff)}
        total = sum(cputimes.values())
        for attr in attrs:
            update.attrs.set(attr, float(cputimes[attr]))
            update.attrs.set(
                attr + '%',
                float(cputimes[attr]) / float(total) * 100)
        self.cputimes = new_cputimes
        return None

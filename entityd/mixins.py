"""Mixins.

A place for commonly needed Mixins to live
"""
import entityd


class HostUEID:
    """Mixin to help get the host UEID"""

    def __init__(self):
        self._host_ueid = None
        self.session = None

    @entityd.pm.hookimpl()
    def entityd_sessionstart(self, session):
        """Store session for later use."""
        self.session = session

    @property
    def host_ueid(self):
        """Get and store the host ueid.

        :raises LookupError: If a host UEID cannot be found.

        :returns: A :class:`cobe.UEID` for the host.
        """
        if self._host_ueid:
            return self._host_ueid
        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Host', attrs=None)
        for hosts in results:
            for host in hosts:
                self._host_ueid = host.ueid
                return self._host_ueid
        raise LookupError('Could not find the host UEID')
"""Mixins.

A place for commonly needed Mixins to live
"""
import entityd


class HostEntity:
    """Mixin to help get the host UEID"""

    def __init__(self):
        self._host_entity = None
        self.session = None

    @entityd.pm.hookimpl()
    def entityd_sessionstart(self, session):
        """Store session for later use."""
        self.session = session

    @property
    def host_entity(self):
        """Get and store the host entity.

        :raises LookupError: If a host cannot be found.

        :returns: A :class:`cobe.UEID` for the host.
        """
        if self._host_entity:
            return self._host_entity

        results = self.session.pluginmanager.hooks.entityd_find_entity(
            name='Host', attrs=None)
        for hosts in results:
            for host in hosts:
                self._host_entity = host
                return self._host_entity
        raise LookupError('Could not find the host')

    @property
    def host_ueid(self):
        """Get and store the host ueid.

        :raises LookupError: If a host cannot be found.

        :returns: A :class:`cobe.UEID` for the host.
        """
        return self.host_entity.ueid

    @property
    def hostname(self):
        """Get and store the hostname.

        :raises LookupError: If a host cannot be found.

        :returns: A :class:`cobe.UEID` for the host.
        """
        return self.host_entity.hostname

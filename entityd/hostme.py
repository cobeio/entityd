"""Example plugin providing the Host Monitored Entity."""


import entityd.pm


@entityd.pm.hookimpl
def entityd_configure(config):
    """Register the Host Monitored Entity."""
    config.addentity('Host', 'entityd.hostme')


@entityd.pm.hookimpl
def entityd_find_entity(name, attrs):
    """Return an iterator of "Host" Monitored Entities."""
    if name == 'Host':
        if attrs is not None:
            raise LookupError('Attribute based filtering not supported')
        return hosts()


def hosts():
    """Generator of Host MEs"""
    yield {'fqdn': 'foo.example.com',
           'uptime': 1234}

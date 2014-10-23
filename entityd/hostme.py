"""Example plugin providing the Host Monitored Entity."""
import socket
import time
import uuid

import syskit

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


def _get_uuid(fqdn):
    """Get a uuid for fqdn if one exists, else generate one

    XXX: Persistent uuids will come when the storage plugin is made

    :param fqdn
    """
    return uuid.uuid4().hex


def hosts():
    """Generator of Host MEs"""
    fqdn = socket.getfqdn()
    uptime = syskit.uptime().timestamp()

    yield {
        'type': 'Host',
        'uuid': _get_uuid(fqdn),
        'created_timestamp': time.time(),
        'attrs': {
            'fqdn': fqdn,
            'uptime': {
                'value': uptime,
                'type': "perf:counter"
            }
        }
    }

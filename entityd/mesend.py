"""Monitored Entity sender.

This plugin implements the sending of Monitored Entities to the modeld
destination.

"""

import logging
import struct

import msgpack
import zmq

import entityd.pm


log = logging.getLogger(__name__)


class MonitoredEntitySender:
    """Plugin to send entities to modeld."""

    def __init__(self):
        self.context = None
        self.session = None
        self.packed_protocol_version = struct.pack('!I', 1)
        self.socket = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_addoption(parser):
        """Add the required options to the command line."""
        parser.add_argument(
            '--dest',                         # XXX choose a better name
            default='tcp://127.0.0.1:25010',  # XXX should not have a default
            type=str,
            help='ZeroMQ address of modeld destination.',
        )

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Called when the monitoring session starts."""
        self.context = zmq.Context()
        self.session = session

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Called when the monitoring session ends.

        Allows 500ms for any buffered messages to be sent.
        """
        if self.socket:
            self.socket.close(linger=500)
        self.context.term()
        self.context = None
        self.session = None

    @entityd.pm.hookimpl
    def entityd_send_entity(self, entity):
        """Send a Monitored Entity to a modeld destination.

        If msgpack fails to serialize ``entity`` then a TypeError will
        be raised.

        Uses zmq.DONTWAIT, so that an error is raised when the buffer is
        full, rather than blocking on send.
        Uses linger=0 and closes the socket in order to empty the buffers.
        """
        if not self.socket:
            log.debug("Creating new socket to %s",
                      self.session.config.args.dest)
            self.socket = self.context.socket(zmq.PUSH)
            self.socket.set(zmq.SNDHWM, 500)
            self.socket.set(zmq.LINGER, 0)
            self.socket.connect(self.session.config.args.dest)
        if isinstance(entity, entityd.EntityUpdate):
            packed_entity = self.encode_entity(entity)
        else:
            packed_entity = msgpack.packb(entity, use_bin_type=True)
        try:
            self.socket.send_multipart([self.packed_protocol_version,
                                        packed_entity],
                                       flags=zmq.DONTWAIT)
        except zmq.Again:
            log.warning("Could not send, message buffers are full. "
                        "Discarding buffer.")
            self.socket.close()
            self.socket = None

    @staticmethod
    def encode_entity(entity):
        """Encode the given entity for sending

        :param entity: The entity to encode.
        :type entity: entityd.EntityUpdate

        """
        if entity.deleted:
            data = {
                'type': entity.metype,
                'ueid': entity.ueid,
                'timestamp': entity.timestamp,
                'deleted': True,
            }
        else:
            data = {
                'type': entity.metype,
                'ueid': entity.ueid,
                'timestamp': entity.timestamp,
                'attrs': {},
                'parents': list(entity.parents),
                'children': list(entity.children)
            }
            for attr in entity.attrs:
                data['attrs'][attr.name] = {'value': attr.value}
                if attr.type:
                    data['attrs'][attr.name]['type'] = attr.type
            for del_attr in entity.attrs.deleted():
                data['attrs'][del_attr] = {'deleted': True}

        return msgpack.packb(data, use_bin_type=True)

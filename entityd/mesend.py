"""Monitored Entity sender.

This plugin implements the sending of Monitored Entities to the modeld
destination.

"""

import act
import logbook
import msgpack
import zmq
import zmq.auth

import entityd.pm


log = logbook.Logger(__name__)


class MonitoredEntitySender:
    """Plugin to send entities to modeld."""

    def __init__(self):
        self.context = None
        self.session = None
        self.packed_protocol_version = b'streamapi/4'
        self._socket = None

    @property
    def socket(self):
        """Return the sender socket, creating it first if necessary.

        If the socket does not yet exist it will be created and have any
        default socket options set before connecting to the destination
        given in the config.args and being returned.
        """
        if not self._socket:
            log.debug("Creating new socket to {}",
                      self.session.config.args.dest)
            keydir = self.session.config.keydir
            modeld_public, _ = zmq.auth.load_certificate(
                str(keydir.joinpath('modeld.key')))
            entityd_public, entityd_secret = zmq.auth.load_certificate(
                str(keydir.joinpath('entityd.key_secret')))
            self._socket = self.context.socket(zmq.PUSH)
            self._socket.SNDHWM = 500
            self._socket.LINGER = 0
            self._socket.CURVE_PUBLICKEY = entityd_public
            self._socket.CURVE_SECRETKEY = entityd_secret
            self._socket.CURVE_SERVERKEY = modeld_public
            self._socket.connect(self.session.config.args.dest)
        return self._socket

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

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_configure(config):
        """Add the key directory to the config."""
        config.keydir = act.fsloc.sysconfdir.joinpath('entityd', 'keys')

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
        if self._socket:
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
            self._socket = None

    @staticmethod
    def encode_entity(entity):
        """Encode the given entity for sending

        :param entity: The entity to encode.
        :type entity: entityd.EntityUpdate

        """
        if entity.deleted:
            data = {
                'type': entity.metype,
                'ueid': str(entity.ueid),
                'timestamp': entity.timestamp,
                'ttl': entity.ttl,
                'deleted': True,
                'label': entity.label
            }
        else:
            data = {
                'type': entity.metype,
                'ueid': str(entity.ueid),
                'timestamp': entity.timestamp,
                'ttl': entity.ttl,
                'attrs': {},
                'parents': [str(parent) for parent in entity.parents],
                'children': [str(child) for child in entity.children],
            }
            if entity.label is not None:
                data['label'] = entity.label
            for attr in entity.attrs:
                data['attrs'][attr.name] = {'value': attr.value}
                data['attrs'][attr.name]['traits'] = list(attr.traits)
            for del_attr in entity.attrs.deleted():
                data['attrs'][del_attr] = {'deleted': True}

        return msgpack.packb(data, use_bin_type=True)

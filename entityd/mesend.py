"""Monitored Entity sender.

This plugin implements the sending of Monitored Entities to the modeld
destination.
"""

import pathlib
import random
import struct

import act
import logbook
import msgpack
import zmq
import zmq.auth

import entityd.pm


log = logbook.Logger(__name__)


class MonitoredEntitySender:  # pylint: disable=too-many-instance-attributes
    """Plugin to send entities to modeld."""

    _DEFAULT_KEY_CLIENT = \
        act.fsloc.sysconfdir.joinpath('entityd', 'keys', 'entityd.key_secret')
    _DEFAULT_KEY_SERVER = \
        act.fsloc.sysconfdir.joinpath('entityd', 'keys', 'modeld.key')

    def __init__(self):
        self.context = None
        self.session = None
        self.packed_protocol_version = b'streamapi/5'
        self._socket = None
        self._stream_file = None
        self._optimised = False
        self._optimised_cycles = {}  # ueid : count
        self._optimised_cycles_max = 1
        self._seen_attributes = {}  # ueid : {name : UpdateAttr}

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
            log.debug('Using client key: {}', self.session.config.args.key)
            key_public, key_private = zmq.auth.load_certificate(
                str(self.session.config.args.key))
            log.debug(
                'Using server key: {}', self.session.config.args.key_receiver)
            key_receiver, _ = zmq.auth.load_certificate(
                str(self.session.config.args.key_receiver))
            self._socket = self.context.socket(zmq.PUSH)
            self._socket.SNDHWM = 10000
            self._socket.LINGER = 0
            self._socket.CURVE_PUBLICKEY = key_public
            self._socket.CURVE_SECRETKEY = key_private
            self._socket.CURVE_SERVERKEY = key_receiver
            self._socket.connect(self.session.config.args.dest)
        return self._socket

    @entityd.pm.hookimpl
    def entityd_addoption(self, parser):
        """Add the required options to the command line."""
        parser.add_argument(
            '--dest',                         # XXX choose a better name
            default='tcp://127.0.0.1:25010',  # XXX should not have a default
            type=str,
            help='ZeroMQ address of modeld destination.',
        )
        parser.add_argument(
            '--key',
            type=pathlib.Path,
            default=self._DEFAULT_KEY_CLIENT,
            help=('Public-private key pair used to encrypt '
                  'communication with the configured receiver.'),
        )
        parser.add_argument(
            '--key-receiver',
            type=pathlib.Path,
            default=self._DEFAULT_KEY_SERVER,
            help='Public key used to identify the configured receiver.',
        )
        parser.add_argument(
            '--stream-write',
            default=None,
            type=pathlib.Path,
        )
        parser.add_argument(
            '--stream-optimise',
            action='store_true',
            help='Use optimised Streaming API format.',
        )
        parser.add_argument(
            '--stream-optimise-frequency',
            type=int,
            default=5,
            help=('How often to send whole updates when '
                  'using optimised Streaming API format. '
                  'Otherwise this option is ignored.'),
        )

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Called when the monitoring session starts."""
        self.context = zmq.Context()
        self.session = session
        if self.session.config.args.stream_write:
            self._stream_file = \
                self.session.config.args.stream_write.open("ab")
        self._optimised = self.session.config.args.stream_optimise
        self._optimised_cycles_max = \
            max(1, self.session.config.args.stream_optimise_frequency)

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
        if self._stream_file:
            self._stream_file.close()

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
            self._optimise_update(entity)
            packed_entity = self.encode_entity(entity)
        else:
            packed_entity = msgpack.packb(entity, use_bin_type=True)
        if self._stream_file:
            self._stream_file.write(
                struct.pack('<I', len(packed_entity)) + packed_entity)
        try:
            self.socket.send_multipart([self.packed_protocol_version,
                                        packed_entity],
                                       flags=zmq.DONTWAIT)
        except zmq.Again:
            # TODO: Purge optimisation caches
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
        if not entity.exists:
            data = {
                'type': entity.metype,
                'ueid': str(entity.ueid),
                'timestamp': entity.timestamp,
                'ttl': entity.ttl,
                'exists': False,
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
            for attr in entity.attrs:
                data['attrs'][attr.name] = {'value': attr.value}
                data['attrs'][attr.name]['traits'] = list(attr.traits)
            for del_attr in entity.attrs.deleted():
                data['attrs'][del_attr] = {'deleted': True}
        if entity.label is not None:
            data['label'] = entity.label
        return msgpack.packb(data, use_bin_type=True, unicode_errors='ignore')

    def _should_optimise_update(self, update):
        """Determine if an update should be optimised.

        For each time a given UEID is seen, a corresponding counter is
        incremented. Whilst this counter is lower than the maximum optimised
        cycles ``True`` will be returned.

        Once the counter limit is reached, it is reset to zero, and
        ``False`` is returned.

        If stream optimisation is disabled then this will always return
        ``False``.

        The first time a UEID is seen, the counter is set to a random
        number which is less than the maximum optimised cycles. This smooths
        out the distribution of optimised updates to avoid large spikes in
        outgoing update sizes everytime the maximum optimised cycles is
        reached.

        :param update: The entity update to consider for optimisation.
        :type update: entityd.EntityUpdate

        :returns: Whether or not the update should be optimised as a boolean.
        """
        if not self._optimised:
            return False
        ueid = update.ueid
        if ueid not in self._optimised_cycles:
            self._optimised_cycles[ueid] = \
                random.randrange(0, self._optimised_cycles_max)
        self._optimised_cycles[ueid] += 1
        if self._optimised_cycles[ueid] >= self._optimised_cycles_max:
            self._optimised_cycles[ueid] = 0
            return False
        return True

    def _optimise_update(self, update):
        """Optimise the attributes of an entity update.

        This checks a given update to see if any attributes are duplicates
        of the previous attribute that were sent. If they are the same,
        they are dropped, in-place, from the update.

        An attributes is considered to be a duplicate if it has the exact
        same value *and* traits.

        The method :meth:`_should_optimise_update` is consulted to determine
        whether or not the update should be optimised. Hence, it is possible
        that this method doesn't modify the update at all.

        Deleted attributes are never dropped from the update. However, it
        will mean that, should the attribute reappear later, it will be sent.

        If the update is marked with :attr:`entityd.EntityUpdate.exists` as
        ``false`` then the previously seen attribute values will be forgotten.
        Hence, a subsequent update will not be optimised. This is to avoid
        holding attribute values in memory for entities that are likely dead
        and unlikely to have any further updates sent for it.

        Note that the given update's UEID is preserved even if the identifying
        attributes are optimised away.
        """
        ueid = update.ueid
        update.ueid = ueid  # Explicit UEID set
        if ueid not in self._seen_attributes:
            self._seen_attributes[ueid] = {}
        if not self._should_optimise_update(update):
            self._seen_attributes[ueid].clear()
        attributes_seen = set(self._seen_attributes[ueid].keys())
        attributes_deleted = update.attrs.deleted()
        attributes_new = set()
        attributes_changed = set()
        for attribute in update.attrs:
            if attribute.name not in attributes_seen:
                attributes_new.add(attribute.name)
            else:
                if attribute != self._seen_attributes[ueid][attribute.name]:
                    attributes_changed.add(attribute.name)
            self._seen_attributes[ueid][attribute.name] = attribute
        for name in attributes_deleted:
            if name in self._seen_attributes[ueid]:
                del self._seen_attributes[ueid][name]
        attributes_send = attributes_new | attributes_changed
        attributes_clear = \
            {attribute.name for attribute in update.attrs} - attributes_send
        for attribute_name in attributes_clear:
            update.attrs.clear(attribute_name)
        if not update.exists:
            self._seen_attributes[ueid].clear()

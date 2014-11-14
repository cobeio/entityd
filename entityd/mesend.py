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


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.mesend':
        sender = MonitoredEntitySender()
        pluginmanager.register(sender,
                               name='entityd.mesend.MonitoredEntitySender')


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

        Uses zmq.DONTWAIT, so that an error is raised when the buffer is
        full, rather than blocking on send.
        Uses linger=0 and closes the socket in order to empty the buffers.
        """
        if not self.socket:
            log.debug("Creating new socket to {}".format(
                self.session.config.args.dest))
            self.socket = self.context.socket(zmq.PUSH)
            self.socket.set(zmq.SNDHWM, 500)
            self.socket.set(zmq.LINGER, 0)
            self.socket.connect(self.session.config.args.dest)
        try:
            packed_entity = msgpack.packb(entity, use_bin_type=True)
        except TypeError:
            log.error("Cannot serialize entity {}".format(entity))
            return
        try:
            self.socket.send_multipart([self.packed_protocol_version,
                                        packed_entity],
                                       flags=zmq.DONTWAIT)
        except zmq.error.Again:
            log.warning("Could not send, message buffers are full. "
                        "Discarding buffer.")
            self.socket.close()
            self.socket = None

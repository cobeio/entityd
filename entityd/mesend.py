"""Monitored Entity sender.

This plugin implements the sending of Monitored Entities to the modeld
destination.

"""
import logging
import struct

import msgpack
import zmq

import entityd.pm


log = logging.getLogger('sender')


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    """Called to register the plugin."""
    if name == 'entityd.mesend':
        sender = MonitoredEntitySender()
        pluginmanager.register(sender,
                               name='entityd.mesend.MonitoredEntitySender')


class MonitoredEntitySender:
    """Plugin to send entities to modeld"""

    def __init__(self):
        self.context = None
        self.session = None
        self.config = None
        self.packed_protocol_version = struct.pack('!I', 1)
        self.socket = None

    @staticmethod
    @entityd.pm.hookimpl
    def entityd_addoption(parser):
        """Add the required options to the command line"""
        parser.add_argument(
            '--dest',                         # XXX choose a better name
            default='tcp://127.0.0.1:25010',  # XXX should not have a default
            type=str,
            help='ZeroMQ address of modeld destination.',
        )

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        """Called when the monitoring session starts"""
        self.context = zmq.Context()
        self.session = session
        self.config = session.config

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self):
        """Called when the monitoring session ends."""
        if self.socket:
            self.socket.close(linger=1)
        self.context.term()
        self.context = None
        self.session = None
        self.config = None

    @entityd.pm.hookimpl
    def entityd_send_entity(self, session, entity):
        """Send a Monitored Entity to a modeld destination."""
        if not self.socket:
            log.info("Creating new socket to {}".format(
                session.config.args.dest))
            self.socket = self.context.socket(zmq.PUSH)
            self.socket.set(zmq.SNDHWM, 500)
            self.socket.set(zmq.LINGER, 0)
            self.socket.connect(session.config.args.dest)
        try:
            # zmq.DONTWAIT: exception when buffers are full, rather than block
            self.socket.send_multipart([self.packed_protocol_version,
                                        msgpack.packb(entity,
                                                      use_bin_type=True)],
                                       flags=zmq.DONTWAIT)
        except zmq.error.Again:
            log.info("Could not send - message buffers are full")
            log.info("Discarding buffer")
            self.socket.close()
            self.socket = None

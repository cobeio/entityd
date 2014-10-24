"""Monitored Entity sender.

This plugin implements the sending of Monitored Entities to the modeld
destination.

"""

import struct

import msgpack
import zmq

import entityd.pm


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    if name == 'entityd.mesend':
        sender = MonitoredEntitySender()
        pluginmanager.register(sender,
                               name='entityd.mesend.MonitoredEntitySender')


class MonitoredEntitySender:

    def __init__(self):
        self.context = None
        self.session = None
        self.config = None
        self.packed_protocol_version = struct.pack('!I', 1)

    @entityd.pm.hookimpl
    def entityd_addoption(self, parser):
        parser.add_argument(
            '--dest',                         # XXX choose a better name
            default='tcp://127.0.0.1:25010',  # XXX should not have a default
            type=str,
            help='ZeroMQ address of modeld destination.',
        )

    @entityd.pm.hookimpl
    def entityd_sessionstart(self, session):
        self.context = zmq.Context()
        self.session = session
        self.config = session.config

    @entityd.pm.hookimpl
    def entityd_sessionfinish(self, session):
        self.context.term()
        self.context = None
        self.session = None
        self.config = None

    @entityd.pm.hookimpl
    def entityd_send_entity(self, session, entity):
        print('sending:', entity)
        sock = self.context.socket(zmq.REQ)
        try:
            sock.connect(session.config.args.dest)
            sock.send_multipart([self.packed_protocol_version,
                                 msgpack.packb(entity, use_bin_type=True)])
            ack = sock.recv_multipart()
            print(ack)
        finally:
            sock.close()

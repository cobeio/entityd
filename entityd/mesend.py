"""Monitored Entity Sender

This plugin implements the sending of Monitored Entities to the modeld
destination.

"""

import struct

import zmq

import entityd.pm


# @entityd.pm.hookimpl
# def entityd_addoption(parser):
#     parser.add_argument(
#         '--dest',                         # XXX choose a better name
#         default='tcp://127.0.0.1:25010',  # XXX should not have a default
#         type=str,
#         help='ZeroMQ address of modeld destination.',
#     )


# @entityd.pm.hookimpl
# def entityd_sessionstart(session):
#     session.mesend_zmq_context = zmq.Context()


# @entityd.pm.hookimpl
# def entityd_sessionfinish(session):
#     session.mesend_zmq_context.close()


# @entityd.pm.hookimpl
# def entityd_send_entity(session, config, entity):
#     zmqcontext = session.mesend_zmq_context
#     sock = zmqcontext.socket(zmq.REQ)
#     try:
#         sock.connect(config.dest)
#         sock.send_multipart([struct.pack('!I', 1), entity])
#         ack = sock.recv_multipart()
#         print(ack)
#     finally:
#         sock.close()


@entityd.pm.hookimpl
def entityd_plugin_registered(pluginmanager, name):
    if name == 'entityd.mesend':
        sender = ManagedEntitySender()
        pluginmanager.register(sender, name='ManagedEntitySender')


class ManagedEntitySender:

    def __init__(self):
        self.context = None
        self.session = None
        self.config = None

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
        self.context.close()
        self.context = None
        self.session = None
        self.config = None

    @entityd.pm.hookimpl
    def entityd_send_entity(self, entity):
        sock = self.context.socket(zmq.REQ)
        try:
            sock.connect(self.config.dest)
            sock.send_multipart([struct.pack('!I', 1), entity])
            ack = sock.recv_multipart()
            print(ack)
        finally:
            sock.close()

"""Monitored Entity Sender

This module implements the sending of Monitored Entities to the modeld
destination.
"""

import zmq


class MonitoredEntitySender:

    def __init__(self, addr):
        self._context = zmq.Context()
        self._addr = addr

    def send(self, me):
        """Send a Monitored Entity to modeld"""
        sock = self._context.socket(zmq.REQ)
        try:
            sock.connect(self._addr)
            sock.send_multipart([b'1', me])
            ack = sock.recv_multipart()
            print(ack)
        finally:
            sock.close()

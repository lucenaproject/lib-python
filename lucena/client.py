# -*- coding: utf-8 -*-
import zmq

from lucena.exceptions import IOTimeout
from lucena.io2.socket import Socket


class RemoteClient(object):

    def __init__(self, default_timeout=None):
        self.default_timeout = default_timeout
        self.socket = Socket(zmq.Context.instance(), zmq.REQ)
        self.socket.setsockopt(zmq.LINGER, 0)
        if default_timeout is not None:
            self.socket.setsockopt(zmq.RCVTIMEO, default_timeout)
        # self.socket.setsockopt(zmq.REQ_CORRELATE, 1)
        # self.socket.setsockopt(zmq.REQ_RELAXED, 1)

    def connect(self, endpoint):
        self.socket.connect(endpoint)

    def run(self, message):
        self.socket.send_to_service(message)
        try:
            return self.socket.recv_from_service()
        except zmq.error.Again:
            raise IOTimeout()

    def close(self):
        self.socket.close()

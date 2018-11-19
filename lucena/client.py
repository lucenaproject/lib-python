# -*- coding: utf-8 -*-
import zmq


class RemoteClient(object):

    def __init__(self):
        self.socket = zmq.Context.instance().socket(zmq.REQ)
        self.socket.identity = u"client-{}".format(client_name).encode("ascii")
        self.socket.connect(self.endpoint)
        self.socket.send(b'{"$req": "HELLO"}')
        reply = self.socket.recv()

    def connect(self, endpoint):
        pass

    def send(self, message):
        pass

    def recv(self, timeout=None):
        pass


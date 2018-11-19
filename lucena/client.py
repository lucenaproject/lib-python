# -*- coding: utf-8 -*-
import zmq

from lucena.io2.socket import Socket


class RemoteClient(object):

    def __init__(self):
        self.socket = Socket(zmq.Context.instance(), zmq.REQ)

    def connect(self, endpoint):
        self.socket.connect(endpoint)

    def send(self, message):
        self.socket.send_to_service(message)

    def recv(self, timeout=None):
        return self.socket.recv_from_service()

    def close(self):
        self.socket.close()

# -*- coding: utf-8 -*-

import json
import logging

import zmq

import lucena.majordomo.MDP as MDP


class Client(object):
    """
    Majordomo Protocol Client API, Python version.
    Implements the MDP/Worker spec at http:#rfc.zeromq.org/spec:7.
    """
    broker = None
    ctx = None
    client = None
    poller = None
    timeout = 2500
    verbose = False

    def __init__(self, broker):
        self.broker = broker
        self.ctx = zmq.Context()
        self.poller = zmq.Poller()
        self.reconnect_to_broker()

    def reconnect_to_broker(self):
        """Connect or reconnect to broker"""
        if self.client:
            self.poller.unregister(self.client)
            self.client.close()
        self.client = self.ctx.socket(zmq.DEALER)
        self.client.linger = 0
        self.client.connect(self.broker)
        self.poller.register(self.client, zmq.POLLIN)
        logging.info("[CLIENT] connecting to broker at %s", self.broker)

    def send(self, message):
        """
        Send request to broker
        """
        request = [b'', MDP.C_CLIENT, json.dumps(message).encode('utf-8')]
        self.client.send_multipart(request)

    def recv(self):
        """
        Returns the reply message or None if there was no reply.
        """
        try:
            items = self.poller.poll(10000)
        except KeyboardInterrupt:
            return
        if items:
            msg = self.client.recv_multipart()
            empty = msg.pop(0)
            assert empty == b''
            header = msg.pop(0)
            assert MDP.C_CLIENT == header
            return json.loads(msg[0].decode('utf-8'))
        else:
            logging.warning("permanent error, abandoning request")


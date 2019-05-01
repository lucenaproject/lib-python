# -*- coding: utf-8 -*-

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
        logging.info("connecting to broker at %s", self.broker)

    def send(self, service, request):
        """
        Send request to broker
        """
        if not isinstance(request, list):
            request = [request]

        # Prefix request with protocol frames
        # Frame 0: empty (REQ emulation)
        # Frame 1: "MDPCxy" (six bytes, MDP/Client x.y)
        # Frame 2: Service name (printable string)

        request = [b'', MDP.C_CLIENT, service] + request
        if self.verbose:
            logging.warning("send request to '%s' service: ", service)
            print(request)
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
            # if we got a reply, process it
            msg = self.client.recv_multipart()
            if self.verbose:
                logging.info("received reply:")
                print(msg)

            # Don't try to handle errors, just assert noisily
            assert len(msg) >= 4
            empty = msg.pop(0)
            header = msg.pop(0)
            assert MDP.C_CLIENT == header
            service = msg.pop(0)
            return msg
        else:
            logging.warning("permanent error, abandoning request")


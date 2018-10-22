# -*- coding: utf-8 -*-
import tempfile
import unittest

import zmq

from lucena.service import create_service
from lucena.worker import Worker


class TestClientService(unittest.TestCase):

    def setUp(self):
        super(TestClientService, self).setUp()
        self.endpoint = "ipc://{}.ipc".format(tempfile.NamedTemporaryFile().name)
        self.service = create_service(
            Worker,
            number_of_workers=4,
            endpoint=self.endpoint
        )

    def client_task(self, client_name):
        """
        Basic request-reply client using REQ socket.
        """
        socket = zmq.Context.instance().socket(zmq.REQ)
        socket.identity = u"client-{}".format(client_name).encode("ascii")
        socket.connect(self.endpoint)
        socket.send(b'{"$req": "HELLO"}')
        reply = socket.recv()
        self.assertEqual(
            reply,
            b'{"$req": "HELLO", "$rep": null, "$error": "No handler match"}'
        )

    def test_total_client_requests(self):
        client_requests = 2
        self.service.start()
        for i in range(client_requests):
            self.client_task(i)

        self.service.send({'$req': 'eval', '$attr': 'total_client_requests'})
        reply = self.service.recv()
        self.assertEqual(client_requests, reply.get('$rep'))
        self.service.stop()

    def test_service_restart(self):
        for i in range(10):
            self.service.start()
            self.service.stop()

# -*- coding: utf-8 -*-
import unittest

import zmq

from lucena.service import Service
from lucena.worker import Worker


class TestClientService(unittest.TestCase):

    def setUp(self):
        super(TestClientService, self).setUp()
        self.service = Service(
            Worker,
            number_of_workers=4
        )
        self.service.start()

    def client_task(self, client_name):
        """
        Basic request-reply client using REQ socket.
        """
        socket = zmq.Context().socket(zmq.REQ)
        socket.identity = u"client-{}".format(client_name).encode("ascii")
        socket.connect(self.service.endpoint)
        socket.send(b'{"$req": "HELLO"}')
        reply = socket.recv()
        self.assertEqual(
            reply,
            b'{"$req": "HELLO", "$rep": null, "$error": "No handler match"}'
        )

    def test_total_client_requests(self):

        client_requests = 10
        for i in range(client_requests):
            self.client_task(i)
        self.assertEqual(client_requests, self.service.total_client_requests)

    def tearDown(self):
        self.service.stop()
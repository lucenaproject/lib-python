# -*- coding: utf-8 -*-
import tempfile
import threading
import unittest
from unittest.mock import MagicMock, patch

import zmq

from lucena.exceptions import ServiceAlreadyStarted, ServiceNotStarted
from lucena.service import Service, create_service
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
        client_requests = 256
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


class TestServiceController(unittest.TestCase):

    def test_service_controller_start_thread(self):
        controller = Service.Controller(worker_factory=Worker)
        rv = (Service.identity(0), b'$controller', {"$signal": "ready"})
        with patch.object(controller.control_socket, 'recv_from_worker', return_value=rv):
            with patch.object(threading, 'Thread', return_value=MagicMock()) as m_thread:
                controller.start()
                m_thread.assert_called_once()

    def test_start_service_fails_if_already_started(self):
        controller = Service.Controller(worker_factory=Worker)
        controller.start()
        self.assertRaises(ServiceAlreadyStarted, controller.start)
        controller.stop()

    def test_send_to_service_fails_if_not_started(self):
        controller = Service.Controller(worker_factory=Worker)
        self.assertRaises(
            ServiceNotStarted,
            controller.send,
            {'$req': 'hello'}
        )

    def test_recv_from_service_fails_if_not_started(self):
        controller = Service.Controller(worker_factory=Worker)
        self.assertRaises(
            ServiceNotStarted,
            controller.recv
        )

# -*- coding: utf-8 -*-
import tempfile
import time
import threading
import unittest
from unittest.mock import MagicMock, patch

from lucena.client import RemoteClient
from lucena.exceptions import ServiceAlreadyStarted, ServiceNotStarted, \
    IOTimeout
from lucena.service import Service, create_service
from lucena.worker import Worker


class MyWorker(Worker):
    def __init__(self, *args, **kwargs):
        super(MyWorker, self).__init__(*args, **kwargs)
        self.bind_handler({'$req': 'sleep'}, MyWorker.handler_sleep)

    @staticmethod
    def handler_sleep(message):
        time.sleep(1)
        response = {}
        response.update(message)
        response.update({'$rep': 'sleep 1 sec'})
        return response


class TestClientService(unittest.TestCase):
    def setUp(self):
        super(TestClientService, self).setUp()
        self.endpoint = "ipc://{}.ipc".format(
            tempfile.NamedTemporaryFile().name
        )
        self.service = create_service(
            'MyService',
            worker_factory=MyWorker,
            number_of_workers=4,
            endpoint=self.endpoint
        )

    def client_task(self, message):
        client = RemoteClient(default_timeout=500)
        client.connect(self.endpoint)
        reply = client.run(message)
        client.close()
        return reply

    def test_total_client_requests(self):
        client_requests = 256
        self.service.start()
        for i in range(client_requests):
            reply = self.client_task({"$req": "HELLO"})
            self.assertEqual(
                reply,
                {"$req": "HELLO", "$rep": None, "$error": "No handler match"}
            )
        self.service.send({'$req': 'eval', '$attr': 'total_client_requests'})
        reply = self.service.recv()
        self.assertEqual(client_requests, reply.get('$rep'))
        self.service.stop()

    def test_service_restart(self):
        for i in range(10):
            self.service.start()
            self.service.stop()

    def test_pending_workers(self):
        self.service.start()
        self.service.send({'$req': 'eval', '$attr': 'pending_workers'})
        reply = self.service.recv()
        self.assertEqual(reply.get('$rep'), False)
        self.service.stop()

    def test_req_timeout(self):
        self.service.start()
        with self.assertRaises(IOTimeout):
            self.client_task({"$req": "sleep"})
        self.service.stop()


class TestServiceController(unittest.TestCase):

    def test_service_controller_start_thread(self):
        controller = Service.Controller(worker_factory=Worker)
        rv = (b'$service', b'$controller', {"$signal": "ready"})
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

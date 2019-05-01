# -*- coding: utf-8 -*-
import time
import threading
import unittest
from unittest.mock import MagicMock, patch

from lucena.client import RemoteClient
from lucena.exceptions import ServiceAlreadyStarted, ServiceNotStarted, \
    IOTimeout
from lucena.service import Service, create_service
from lucena.io2.socket import ipc_unique_endpoint, Response
from lucena.worker2 import Worker


class MyWorker(Worker):
    def __init__(self, **kwargs):
        super(MyWorker, self).__init__(**kwargs)
        self.bind_handler({'$req': 'sleep'}, MyWorker.handler_sleep)

    @staticmethod
    def handler_sleep(message):
        print("Sleeping 1 sec...")
        time.sleep(1)
        response = {}
        response.update(message)
        response.update({'$rep': 'Sleep 1 sec...'})
        return response


@unittest.skip('deprecated tests')
class TestClientService(unittest.TestCase):
    def setUp(self):
        super(TestClientService, self).setUp()
        self.endpoint = ipc_unique_endpoint()
        self.service = create_service(
            'MyService',
            worker_factory=MyWorker,
            number_of_workers=4,
            endpoint=self.endpoint
        )

    def client_task(self, message):
        client = RemoteClient(default_timeout=700)
        client.connect(self.endpoint)
        response = client.resolve(message)
        client.close()
        return response

    def test_total_client_requests(self):
        client_requests = 1
        self.service.start()
        for i in range(client_requests):
            response = self.client_task({"$req": "HELLO"})
            self.assertEqual(
                response,
                {"$req": "HELLO", "$rep": None, "$error": "No handler match"}
            )
        response = self.service.resolve({
            '$req': 'eval',
            '$attr': 'total_client_requests'
        })
        self.assertEqual(client_requests, response.get('$rep'))
        self.service.stop()

    def test_service_restart(self):
        for i in range(10):
            self.service.start()
            self.service.stop()

    def test_pending_workers(self):
        self.service.start()
        response = self.service.resolve({
            '$req': 'eval',
            '$attr': 'pending_workers'
        })
        self.assertEqual(response.get('$rep'), False)
        self.service.stop()

    def test_req_timeout(self):
        self.service.start()
        with self.assertRaises(IOTimeout):
            self.client_task({"$req": "sleep"})
        self.service.stop()


@unittest.skip('deprecated tests')
class TestServiceController(unittest.TestCase):

    def test_service_controller_start_thread(self):
        controller = Service.Controller(worker_factory=Worker)
        rv = Response({"$signal": "ready"}, worker=b'$service', client=b'$controller', uuid=b'$uuid')
        with patch.object(controller.control_socket, 'recv_from_worker', return_value=rv):
            with patch.object(threading, 'Thread', return_value=MagicMock()) as m_thread:
                controller.start()
                m_thread.assert_called_once()

    def test_start_service_fails_if_already_started(self):
        controller = Service.Controller(worker_factory=Worker)
        controller.start()
        self.assertRaises(ServiceAlreadyStarted, controller.start)
        controller.stop()

    def test_resolve_fails_if_not_started(self):
        controller = Service.Controller(worker_factory=Worker)
        self.assertRaises(
            ServiceNotStarted,
            controller.resolve,
            {'$req': 'hello'}
        )

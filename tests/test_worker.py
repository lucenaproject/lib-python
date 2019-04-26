# -*- coding: utf-8 -*-
import threading
import unittest
from unittest.mock import MagicMock, patch

from lucena.exceptions import WorkerAlreadyStarted, WorkerNotStarted, \
    LookupHandlerError
from lucena.worker import Worker
from lucena.io2.socket import Response


class TestWorker(unittest.TestCase):

    def setUp(self):
        super(TestWorker, self).setUp()
        self.message = {'a': 123, 'b': 'hello'}
        self.worker = Worker()
        self.basic_handler = lambda: None

    def test_lookup_handler(self):
        self.worker.bind_handler(self.message, self.basic_handler)
        handler = self.worker.get_handler_for(self.message)
        self.assertEqual(handler, self.basic_handler)

    def test_lookup_unknown_message_returns_default_handler(self):
        handler = self.worker.get_handler_for(self.message)
        self.assertEqual(handler, Worker.handler_default)

    def test_lookup_unknown_handler_raises_an_exception(self):
        self.worker.unbind_handler({})
        self.assertRaises(
            LookupHandlerError,
            self.worker.get_handler_for,
            self.message
        )

    def test_unbind_unknown_handler_raises_an_exception(self):
        self.worker.bind_handler(self.message, self.basic_handler)
        self.worker.unbind_handler(self.message)
        self.assertRaises(
            LookupHandlerError,
            self.worker.unbind_handler,
            {'a': 456}
        )


class TestWorkerController(unittest.TestCase):

    def test_worker_controller_start_thread(self):
        controller = Worker.Controller()
        rv = Response(
            {"$signal": "ready"},
            worker=b'$worker#0',
            client=b'$controller',
            uuid=b'$uuid'
        )
        with patch.object(controller.control_socket, 'recv_from_worker', return_value=rv):
            with patch.object(threading, 'Thread', return_value=MagicMock()) as m_thread:
                controller.start()
                m_thread.assert_called_once()

    def test_start_worker_fails_if_already_started(self):
        controller = Worker.Controller()
        controller.start()
        self.assertRaises(WorkerAlreadyStarted, controller.start)
        controller.stop()

    def test_send_to_worker_fails_if_not_started(self):
        controller = Worker.Controller()
        self.assertRaises(
            WorkerNotStarted,
            controller.send,
            b'worker',
            b'client',
            b'$uuid',
            {'$req': 'hello'}
        )

    def test_recv_from_worker_fails_if_not_started(self):
        controller = Worker.Controller()
        self.assertRaises(
            WorkerNotStarted,
            controller.recv
        )

    def test_number_of_worker_fails_with_invalid_parameters(self):
        controller = Worker.Controller()
        self.assertRaises(
            ValueError,
            controller.start,
            number_of_workers='a'
        )
        self.assertRaises(
            ValueError,
            controller.start,
            number_of_workers=0
        )
        self.assertRaises(
            ValueError,
            controller.start,
            number_of_workers=-10
        )

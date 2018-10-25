# -*- coding: utf-8 -*-
import threading
import unittest
from unittest.mock import MagicMock, patch

from lucena.exceptions import WorkerAlreadyStarted, WorkerNotStarted, \
    UnexpectedParameterValue
from lucena.worker import Worker


class TestWorker(unittest.TestCase):
    @staticmethod
    def basic_handler():
        pass

    def setUp(self):
        super(TestWorker, self).setUp()

    def test_lookup_handler(self):
        message = {'a': 123, 'b': 'hello'}
        worker = Worker()
        worker.bind_handler(message, self.basic_handler)
        handler = worker.get_handler_for(message)
        self.assertEqual(handler, self.basic_handler)

    def test_lookup_unknown_message_returns_default_handler(self):
        message = {'a': 123}
        worker = Worker()
        handler = worker.get_handler_for(message)
        self.assertEqual(handler, Worker.handler_default)


class TestWorkerController(unittest.TestCase):

    def test_worker_controller_start_thread(self):
        controller = Worker.Controller()
        rv = (Worker.identity(0), b'$controller', {"$signal": "ready"})
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
            UnexpectedParameterValue,
            controller.start,
            number_of_workers='a'
        )
        self.assertRaises(
            UnexpectedParameterValue,
            controller.start,
            number_of_workers=0
        )
        self.assertRaises(
            UnexpectedParameterValue,
            controller.start,
            number_of_workers=-10
        )

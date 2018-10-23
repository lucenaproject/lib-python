# -*- coding: utf-8 -*-
import threading
import unittest
from unittest.mock import MagicMock, patch

from lucena.exceptions import AlreadyStarted
from lucena.worker import Worker


class TestWorkerController(unittest.TestCase):

    def test_worker_controller_start_thread(self):
        controller = Worker.Controller()
        rv = (controller.identity_for(0), b'$controller', {"$signal": "ready"})
        with patch.object(controller.control_socket, 'recv_from_worker', return_value=rv):
            with patch.object(threading, 'Thread', return_value=MagicMock()) as m_thread:
                controller.start()
                m_thread.assert_called_once()

    def test_start_worker_fails_if_already_started(self):
        controller = Worker.Controller()
        controller.start()
        self.assertRaises(AlreadyStarted, controller.start)
        controller.stop()

# -*- coding: utf-8 -*-
import unittest

from lucena.exceptions import AlreadyStarted
from lucena.worker import Worker


class TestWorkerController(unittest.TestCase):

    def test_worker_controller_start(self):
        pass


    def test_start_worker_fails_if_already_started(self):
        controller = Worker.Controller()
        controller.start()
        self.assertRaises(AlreadyStarted, controller.start)
        controller.stop()

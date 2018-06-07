# -*- coding: utf-8 -*-
import unittest

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
        self.assertEqual(
            handler,
            Worker.default_handler
        )

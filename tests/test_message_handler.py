# -*- coding: utf-8 -*-
import unittest

from lucena.message_handler import MessageHandler


class TestMessageHandler(unittest.TestCase):
    
    def setUp(self):
        super(TestMessageHandler, self).setUp()

    def test_more_properties_win(self):
        mh1 = MessageHandler(
            {'a': 123, 'b': 999, 'c': 'hello'},
            None
        )
        mh2 = MessageHandler(
            {'a': 123, 'b': 999},
            None
        )
        self.assertTrue(mh1 < mh2)

    def test_alphabetical_order_win(self):
        mh1 = MessageHandler(
            {'a': 123, 'b': 999, 'c': 'hello'},
            None
        )
        mh2 = MessageHandler(
            {'a': 123, 'b': 999, 'd': 'hello'},
            None
        )
        self.assertTrue(mh1 < mh2)

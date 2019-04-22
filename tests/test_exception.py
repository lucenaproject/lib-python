# -*- coding: utf-8 -*-
import unittest

from lucena import exceptions


class TestClientService(unittest.TestCase):

    def test_lucena_exceptions(self):
        all_exceptions = dict([
            (name, cls) for name, cls in exceptions.__dict__.items()
            if isinstance(cls, type)
        ])
        for exception_name, exception_class in all_exceptions.items():
            try:
                raise exception_class()
            except exceptions.LucenaException as error:
                self.assertEqual(error.__str__(), error.__doc__)

    def test_custom_exception(self):
        try:
            raise exceptions.LucenaException("Unexpected error")
        except exceptions.LucenaException as error:
            self.assertEqual(error.__str__(), "Unexpected error")

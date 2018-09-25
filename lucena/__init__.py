# -*- coding: utf-8 -*-

VOID_FRAME = b'<void>'
READY_MESSAGE = {"$signal": "READY"}
STOP_MESSAGE = {"$signal": "STOP"}


class SignalException(Exception):
    pass


class StopSignal(SignalException):
    pass


class ErrorSignal(SignalException):
    pass

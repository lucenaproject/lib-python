# -*- coding: utf-8 -*-

import struct

import zmq


class Socket(zmq.Socket):
    SIGNAL_READY = 0x7f000001
    SIGNAL_STOP = 0x7f000002

    @staticmethod
    def is_signal(message):
        return len(message) == 4 and message[3] == 0x7f

    def signal(self, status=0):
        assert status < 0x7fffffff
        self.send(struct.pack("I", status))

    def wait(self, timeout=-1):
        self.setsockopt(zmq.RCVTIMEO, timeout)
        message = self.recv()
        assert Socket.is_signal(message)
        return struct.unpack('I', message)[0]

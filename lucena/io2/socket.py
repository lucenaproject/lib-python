# -*- coding: utf-8 -*-

import struct

import zmq


class Socket(zmq.Socket):
    MAGIC = 0x7d9afffe00000000
    SIGNAL_READY = 1
    SIGNAL_STOP = 2

    def signal(self, status=0):
        assert status < 1000
        self.send(struct.pack("Q", Socket.MAGIC + status))

    def wait(self, timeout=-1):
        self.setsockopt(zmq.RCVTIMEO, timeout)
        try:
            message = self.recv()
        except zmq.ZMQError:
            message = ''

        if not len(message) == 8:
            return None
        signal_value = struct.unpack('Q', message)[0]
        if (signal_value & 0xffffffff00000000) == Socket.MAGIC:
            return signal_value & 0xffffffff
        return None

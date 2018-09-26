# -*- coding: utf-8 -*-
import json
import struct
import uuid

import zmq


class Socket(zmq.Socket):
    DELIMITER_FRAME = b''
    SIGNAL_READY = 0x7f000001
    SIGNAL_STOP = 0x7f000002

    @staticmethod
    def is_signal(message):
        return len(message) == 4 and message[3] == 0x7f

    @staticmethod
    def inproc_unique_endpoint():
        """
        http://zguide.zeromq.org/page:all#Unicast-Transports
        The inter-thread transport, inproc, is a connected signaling transport.
        It is much faster than tcp or ipc. This transport has a specific limitation
        compared to tcp and ipc: the server must issue a bind before
        any client issues a connect.
        """
        endpoint = "inproc://lucena-{}".format(str(uuid.uuid4()))
        return endpoint

    @staticmethod
    def socket_pair(context, hwm=1000):
        """
        Pair of connected sockets:
        socket_0 (bind) <--> socket_1 (connect)
        """
        socket_0 = Socket(context, zmq.PAIR)
        socket_1 = Socket(context, zmq.PAIR)
        socket_0.set_hwm(hwm)
        socket_1.set_hwm(hwm)
        # Close immediately on shutdown.
        socket_0.setsockopt(zmq.LINGER, 0)
        socket_1.setsockopt(zmq.LINGER, 0)
        # inproc is much faster than tcp or ipc but the server must
        # issue a bind before any client issues a connect.
        # http://zguide.zeromq.org/page:all#Unicast-Transports
        endpoint = Socket.inproc_unique_endpoint()
        socket_0.bind(endpoint)
        socket_1.connect(endpoint)
        return socket_0, socket_1

    def signal(self, status=0):
        assert status < 0x7fffffff
        self.send(struct.pack("I", status))

    def wait(self, timeout=-1):
        self.setsockopt(zmq.RCVTIMEO, timeout)
        message = self.recv()
        assert Socket.is_signal(message)
        return struct.unpack('I', message)[0]

    def send_to_service(self, client, message):
        self.send_multipart([
            client,
            Socket.DELIMITER_FRAME,
            bytes(json.dumps(message).encode('utf-8'))
        ])

    def recv_from_service(self):
        frames = self.recv_multipart()
        assert len(frames) == 3
        assert frames[1] == Socket.DELIMITER_FRAME
        client = frames[0]
        message = json.loads(frames[2].decode('utf-8'))
        return client, message

    def send_to_client(self, worker, client, message):
        self.send_multipart([
            worker,
            Socket.DELIMITER_FRAME,
            client,
            Socket.DELIMITER_FRAME,
            bytes(json.dumps(message).encode('utf-8'))
        ])

    def recv_from_client(self):
        frames = self.recv_multipart()
        assert len(frames) == 5
        assert frames[1] == Socket.DELIMITER_FRAME
        assert frames[3] == Socket.DELIMITER_FRAME
        worker = frames[0]
        client = frames[2]
        message = json.loads(frames[4].decode('utf-8'))
        return worker, client, message


if __name__ == '__main__':
    s0, s1 = Socket.socket_pair(zmq.Context())
    print(s0.last_endpoint)

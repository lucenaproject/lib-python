# -*- coding: utf-8 -*-
import json
import struct
import uuid

import zmq


class Response(object):
    def __init__(self, message, worker=None, client=None, uuid=None):
        self.message = message
        self.worker = worker
        self.client = client
        self.uuid = uuid

    # def validate(self, message=None, worker=None, client=None, uuid=None):
    #     if message and not message == self.message:
    #         return False
    #     if worker and not worker == self.worker:
    #         return False
    #     if client and not client == self.client:
    #         return False
    #     if uuid and not uuid == self.uuid:
    #         return False
    #     return True


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
        It is much faster than tcp or ipc.
        This transport has a specific limitation compared to tcp and ipc:
        the server must issue a bind before any client issues a connect.
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

    def __init__(self, context, sock_type, **kwargs):
        identity = None
        if 'identity' in kwargs:
            identity = kwargs.pop('identity')
        super(Socket, self).__init__(context, sock_type, **kwargs)
        if identity is not None:
            self.identity = identity

    def signal(self, status=0):
        assert status < 0x7fffffff
        self.send(struct.pack("I", status))

    def wait(self, timeout=-1):
        self.setsockopt(zmq.RCVTIMEO, timeout)
        message = self.recv()
        assert Socket.is_signal(message)
        return struct.unpack('I', message)[0]

    def send_to_client(self, client, uuid, message):
        self.send_multipart([
            client,
            Socket.DELIMITER_FRAME,
            uuid,
            Socket.DELIMITER_FRAME,
            bytes(json.dumps(message).encode('utf-8'))
        ])

    def recv_from_client(self):
        frames = self.recv_multipart()
        assert len(frames) == 5
        assert frames[1] == Socket.DELIMITER_FRAME
        assert frames[3] == Socket.DELIMITER_FRAME
        return Response(
            json.loads(frames[4].decode('utf-8')),
            client=frames[0],
            uuid=frames[2]
        )

    def send_to_worker(self, worker, client, uuid, message):
        self.send_multipart([
            worker,
            Socket.DELIMITER_FRAME,
            client,
            Socket.DELIMITER_FRAME,
            uuid,
            Socket.DELIMITER_FRAME,
            bytes(json.dumps(message).encode('utf-8'))
        ])

    def recv_from_worker(self):
        frames = self.recv_multipart()
        assert len(frames) == 7
        assert frames[1] == Socket.DELIMITER_FRAME
        assert frames[3] == Socket.DELIMITER_FRAME
        assert frames[5] == Socket.DELIMITER_FRAME
        return Response(
            json.loads(frames[6].decode('utf-8')),
            worker=frames[0],
            client=frames[2],
            uuid=frames[4]
        )

    def send_to_service(self, uuid, message):
        self.send_multipart([
            uuid,
            Socket.DELIMITER_FRAME,
            bytes(json.dumps(message).encode('utf-8'))
        ])

    def recv_from_service(self):
        frames = self.recv_multipart()
        assert len(frames) == 3
        assert frames[1] == Socket.DELIMITER_FRAME
        return Response(
            json.loads(frames[2].decode('utf-8')),
            uuid=frames[0]
        )


class RouteSocket(Socket):

    def wait(self, timeout=-1):
        self.setsockopt(zmq.RCVTIMEO, timeout)
        sender, delimiter, message = self.recv_multipart()
        assert Socket.is_signal(message)
        return struct.unpack('I', message)[0]

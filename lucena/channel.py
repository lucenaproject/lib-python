# -*- coding: utf-8 -*-
import json

import zmq

from lucena import READY_MESSAGE, VOID_FRAME


class Channel(object):
    """
    Base class for all Channels.
    Channels are socket wrappers and some helper functions to implement the
    communication between Client <-> Service <-> Worker.
    """
    DELIMITER_FRAME = b''
    WORKER_ENDPOINT = "inproc://worker"

    def __init__(self, context, socket_type, endpoint, identity=None):
        self.context = context
        self.socket = None
        self.socket_type = socket_type
        self.endpoint = endpoint
        self.identity = identity
        self.open()

    def __del__(self):
        self.close()

    def _after_open(self):
        pass

    def _before_close(self):
        pass

    def open(self):
        if self.socket is None:
            self.socket = self.context.socket(self.socket_type)
            if self.identity:
                self.socket.identity = self.identity
            self._after_open()

    def close(self):
        if self.socket:
            self._before_close()
            self.socket.close()
            self.socket = None

    def recv(self):
        raise NotImplementedError("Implement me in a subclass")

    def send(self, **kwargs):
        raise NotImplementedError("Implement me in a subclass")


class WorkerChannel(Channel):
    """
    This Channel allows Workers to send replies to Clients through a
    Service router.

    Client <--> Service:ServiceWorkerChannel <--> WorkerChannel:Worker
    """
    def __init__(self, context, identity=None):
        super(WorkerChannel, self).__init__(
            context,
            zmq.REQ,
            Channel.WORKER_ENDPOINT,
            identity
        )

    def _after_open(self):
        self.socket.connect(self.endpoint)
        self.send(VOID_FRAME, READY_MESSAGE)

    def send(self, client, message):
        self.socket.send_multipart([
            client,
            Channel.DELIMITER_FRAME,
            bytes(json.dumps(message).encode('utf-8'))
        ])

    def recv(self):
        frames = self.socket.recv_multipart()
        assert len(frames) == 3
        assert frames[1] == Channel.DELIMITER_FRAME
        client = frames[0]
        message = json.loads(frames[2].decode('utf-8'))
        return client, message


class ServiceWorkerChannel(Channel):
    """
    This Channel routes messages between Workers and Clients through a Service
    router. When Channel closes, a STOP signal is sent to all registered workers.

    Client <--> Service:ServiceWorkerChannel <--> WorkerChannel:Worker
    """
    def __init__(self, context):
        super(ServiceWorkerChannel, self).__init__(
            context,
            zmq.ROUTER,
            Channel.WORKER_ENDPOINT
        )

    def _after_open(self):
        self.socket.bind(self.endpoint)

    def send(self, worker, client, message):
        self.socket.send_multipart([
            worker,
            Channel.DELIMITER_FRAME,
            client,
            Channel.DELIMITER_FRAME,
            bytes(json.dumps(message).encode('utf-8'))
        ])

    def recv(self):
        frames = self.socket.recv_multipart()
        assert len(frames) == 5
        assert frames[1] == Channel.DELIMITER_FRAME
        assert frames[3] == Channel.DELIMITER_FRAME
        worker = frames[0]
        client = frames[2]
        message = json.loads(frames[4].decode('utf-8'))
        return worker, client, message


class ServiceClientChannel(Channel):
    """
    This Channel routes messages between Workers and Clients through a
    Service router.

    Client:ClientChannel <--> ServiceClientChannel:Service <--> Worker
    """
    def __init__(self, context, endpoint):
        super(ServiceClientChannel, self).__init__(
            context,
            zmq.ROUTER,
            endpoint
        )

    def _after_open(self):
        self.socket.bind(self.endpoint)

    def send(self, client, message):
        self.socket.send_multipart([
            client,
            Channel.DELIMITER_FRAME,
            bytes(json.dumps(message).encode('utf-8'))
        ])

    def recv(self):
        frames = self.socket.recv_multipart()
        assert len(frames) == 3
        assert frames[1] == Channel.DELIMITER_FRAME
        client = frames[0]
        message = json.loads(frames[2].decode('utf-8'))
        return client, message

# -*- coding: utf-8 -*-
import json
import threading

import zmq


class Channel(object):
    """
    Base class for all Channels.
    Channels are socket wrappers and some helper functions to implement the
    communication between Client <-> Service <-> Worker.
    """
    DELIMITER_FRAME = b''
    VOID_FRAME = b'<void>'
    READY_MESSAGE = {"$signal": "READY"}
    STOP_MESSAGE = {"$signal": "STOP"}
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
    Service router. It's just a normal socket wrapper that sends a
    READY message when the channel is opened.

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
        self.send(Channel.VOID_FRAME, Channel.READY_MESSAGE)

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
    router. Notice that plug_worker() starts a worker in a new Thread.
    When Channel closes, a STOP signal is sent to all registered workers.

    Client <--> Service:ServiceWorkerChannel <--> WorkerChannel:Worker
    """
    def __init__(self, context):
        super(ServiceWorkerChannel, self).__init__(
            context,
            zmq.ROUTER,
            Channel.WORKER_ENDPOINT
        )
        self.workers = []

    def _after_open(self):
        self.socket.bind(self.endpoint)

    def _before_close(self):
        while self.workers:
            worker = self.workers.pop()
            self.send(worker, Channel.VOID_FRAME, Channel.STOP_MESSAGE)

    def _plug_worker(self, worker_class, identity):
        worker = worker_class()
        worker.plug(self.context, identity)

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

    def plug_worker(self, worker_class):
        worker_identity = "worker-{}".format(len(self.workers)).encode("utf-8")
        thread = threading.Thread(
            target=self._plug_worker,
            args=(worker_class, worker_identity,)
        )
        thread.start()
        worker, client, message = self.recv()
        assert worker == worker_identity
        assert client == Channel.VOID_FRAME
        assert message == Channel.READY_MESSAGE
        self.workers.append(worker_identity)
        return worker_identity


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

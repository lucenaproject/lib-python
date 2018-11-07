# -*- coding: utf-8 -*-
import collections
import re
import threading
import zmq

from lucena.exceptions import WorkerAlreadyStarted, WorkerNotStarted, \
    LookupHandlerError
from lucena.io2.socket import Socket
from lucena.message_handler import MessageHandler


class Worker(object):

    RunningWorker = collections.namedtuple(
        'RunningWorker',
        ['worker', 'thread']
    )

    PollHandler = collections.namedtuple(
        'PollHandler',
        ['socket', 'flags', 'handler']
    )

    class Controller(object):

        def __init__(self, *args, **kwargs):
            self.context = zmq.Context.instance()
            self.args = args
            self.kwargs = kwargs
            self.poller = zmq.Poller()
            self.running_workers = None
            self.control_socket = Socket(self.context, zmq.ROUTER)
            self.control_socket.bind(Socket.inproc_unique_endpoint())

        def is_started(self):
            return self.running_workers is not None

        def start(self, number_of_workers=1):
            if self.is_started():
                raise WorkerAlreadyStarted()
            if not isinstance(number_of_workers, int) or number_of_workers < 1:
                raise ValueError("Parameter number_of_workers must be a positive integer.")
            self.running_workers = {}
            for i in range(number_of_workers):
                worker = Worker(*self.args, **self.kwargs)
                thread = threading.Thread(
                    target=worker,
                    daemon=False,
                    kwargs={
                        'endpoint': self.control_socket.last_endpoint,
                        'index': i
                    }
                )
                thread.start()
                identity, client, message = self.recv()
                assert identity == worker.identity(i)
                assert client == b'$controller'
                assert message == {"$signal": "ready"}
                self.running_workers[identity] = Worker.RunningWorker(worker, thread)
            return list(self.running_workers.keys())

        def stop(self, timeout=None):
            for worker_id, running_worker in self.running_workers.items():
                self.send(worker_id, b'$controller', {'$signal': 'stop'})
                _worker_id, client, message = self.recv()
                assert _worker_id == worker_id
                assert client == b'$controller'
                assert message == {'$signal': 'stop', '$rep': 'OK'}
                running_worker.thread.join(timeout=timeout)
            self.running_workers = None

        def send(self, worker_id, client_id, message):
            if not self.is_started():
                raise WorkerNotStarted()
            return self.control_socket.send_to_worker(worker_id, client_id, message)

        def recv(self):
            # TODO: Add timeout
            if not self.is_started():
                raise WorkerNotStarted()
            worker, client, message = self.control_socket.recv_from_worker()
            return worker, client, message

    # Worker implementation.

    def __init__(self, *args, **kwargs):
        self.context = zmq.Context.instance()
        self.poller = zmq.Poller()
        self.poll_handlers = []
        self.message_handlers = []
        self.bind_handler({}, self.handler_default)
        self.bind_handler({'$signal': 'stop'}, self.handler_stop)
        self.bind_handler({'$req': 'eval'}, self.handler_eval)
        self.stop_signal = False
        self.control_socket = None

    def __call__(self, endpoint, index):
        self._before_start(self.identity(index))
        self.control_socket.connect(endpoint)
        self.control_socket.send_to_client(b'$controller', {"$signal": "ready"})
        while not self.stop_signal:
            self._handle_poll()
        self._before_stop()

    def _add_poll_handler(self, socket, flags, handler):
        poll_handler = self.PollHandler(socket, flags, handler)
        self.poll_handlers.append(poll_handler)

    def _before_start(self, identity):
        self.poll_handlers = []
        self.stop_signal = False
        self.control_socket = Socket(self.context, zmq.REQ, identity=identity)
        self._add_poll_handler(
            self.control_socket,
            zmq.POLLIN if not self.stop_signal else 0,
            self._handle_ctrl_socket
        )

    def _before_stop(self):
        self.control_socket.close()

    def _handle_poll(self):
        for poll_handler in self.poll_handlers:
            self.poller.register(
                poll_handler.socket,
                poll_handler.flags
            )
        sockets = dict(self.poller.poll(1))
        for poll_handler in self.poll_handlers:
            if poll_handler.socket in sockets:
                poll_handler.handler()

    def _handle_ctrl_socket(self):
        client, message = self.control_socket.recv_from_client()
        response = self.resolve(message)
        self.control_socket.send_to_client(client, response)

    @classmethod
    def identity(cls, index=0):
        id1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', cls.__name__)
        id2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', id1).lower()
        return '{}#{}'.format(id2, index).encode('utf8')

    @staticmethod
    def handler_default(message):
        response = {}
        response.update(message)
        response.update({"$rep": None, "$error": "No handler match"})
        return response

    def handler_eval(self, message):
        response = {}
        response.update(message)
        attr = getattr(self, message.get('$attr'))
        response.update({'$rep': attr})
        return response

    def handler_stop(self, message):
        response = {}
        response.update(message)
        response.update({'$rep': 'OK'})
        self.stop_signal = True
        return response

    def bind_handler(self, message, handler):
        self.message_handlers.append(MessageHandler(message, handler))
        self.message_handlers.sort()

    def unbind_handler(self, message):
        for message_handler in self.message_handlers:
            if message_handler.message == message:
                self.message_handlers.remove(message_handler)
                return
        raise LookupHandlerError("No handler for {}".format(message))

    def get_handler_for(self, message):
        for message_handler in self.message_handlers:
            if message_handler.match_in(message):
                return message_handler.handler
        raise LookupHandlerError("No handler for {}".format(message))

    def resolve(self, message):
        handler = self.get_handler_for(message)
        return handler(message)


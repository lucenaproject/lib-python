# -*- coding: utf-8 -*-
import collections
import threading
import zmq
from uuid import uuid4

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
        def __init__(self, **kwargs):
            self.context = zmq.Context.instance()
            self.default_timeout = kwargs.get('default_timeout')
            self.kwargs = kwargs
            self.running_workers = None
            self.control_socket = Socket(self.context, zmq.ROUTER)
            self.control_socket.bind(Socket.inproc_unique_endpoint())
            self.control_socket.setsockopt(zmq.ROUTER_MANDATORY, 1)

        def is_started(self):
            return self.running_workers is not None

        def start(self, number_of_workers=1):
            if self.is_started():
                raise WorkerAlreadyStarted()
            if not isinstance(number_of_workers, int) or number_of_workers < 1:
                raise ValueError(
                    "Parameter number_of_workers must be a positive integer."
                )
            self.running_workers = {}
            for i in range(number_of_workers):
                worker = self.kwargs.get('worker_factory', Worker)(
                    **self.kwargs
                )
                identity = '$worker#{}'.format(i).encode('utf8')
                thread = threading.Thread(
                    target=worker,
                    daemon=False,
                    kwargs={
                        'endpoint': self.control_socket.last_endpoint,
                        'identity': identity
                    }
                )
                thread.start()
                self.wait_for_signal('ready', identity)
                self.running_workers[identity] = Worker.RunningWorker(
                    worker,
                    thread
                )
            return list(self.running_workers.keys())

        def stop(self, timeout=None):
            for worker_id, running_worker in self.running_workers.items():
                # TODO: Remove this and try with Poll
                # TODO: Keep the sequence (REQ1, REP1), (REQ2, REP2), ...
                for _ in range(2):
                    self.send(
                        worker_id,
                        b'$controller',
                        b'$uuuuuuid',
                        {'$signal': 'stop'}
                    )
                    response = self.recv()
                    if not worker_id == worker_id:
                        continue
                    if not response.client == b'$controller':
                        continue
                    if not response.message == {'$signal': 'stop', '$rep': 'OK'}:
                        continue
                    running_worker.thread.join(timeout=timeout)
                    break
            self.running_workers = None

        def send(self, worker_id, client_id, uuid, message):
            if not self.is_started():
                raise WorkerNotStarted()
            return self.control_socket.send_to_worker(
                worker_id,
                client_id,
                uuid,
                message
            )

        def recv(self, timeout=None):
            if not self.is_started():
                raise WorkerNotStarted()
            return self.control_socket.recv_from_worker()

        def wait_for_signal(self, signal, worker=None):
            response = self.control_socket.recv_from_worker()
            if worker is not None:
                assert response.worker == worker
            assert response.client == b'$controller'
            assert response.message == {"$signal": signal}

    # Worker implementation.

    def __init__(self, **kwargs):
        self.identity = None
        self.control_socket = None
        self.stop_signal = False
        self.default_timeout = kwargs.get('default_timeout')
        self.poll_handlers = []
        self.message_handlers = []
        self.context = zmq.Context.instance()
        self.poller = zmq.Poller()
        self.bind_handler({}, self.handler_default)
        self.bind_handler({'$signal': 'stop'}, self.handler_stop)
        self.bind_handler({'$req': 'eval'}, self.handler_eval)

    def __call__(self, endpoint, identity):
        self.identity = identity
        self._before_start()
        for poll_handler in self.poll_handlers:
            self.poller.register(
                poll_handler.socket,
                poll_handler.flags
            )
        self._signal_ready(endpoint)
        while not self.stop_signal:
            self._handle_poll()
        self._before_stop()
        print("EXIT {}".format(self.identity))

    def _add_poll_handler(self, socket, flags, handler):
        poll_handler = self.PollHandler(socket, flags, handler)
        self.poll_handlers.append(poll_handler)

    def _before_start(self):
        self.poll_handlers = []
        self.stop_signal = False
        self.control_socket = Socket(
            self.context,
            zmq.REQ,
            identity=self.identity
        )
        self._add_poll_handler(
            self.control_socket,
            zmq.POLLIN,
            self._handle_ctrl_socket
        )

    def _before_stop(self):
        self.control_socket.close()

    def _handle_poll(self):
        sockets = dict(self.poller.poll(10))
        for poll_handler in self.poll_handlers:
            if poll_handler.socket in sockets:
                poll_handler.handler()

    def _handle_ctrl_socket(self):
        response = self.control_socket.recv_from_client()
        self.control_socket.send_to_client(
            response.client,
            response.uuid,
            self.resolve(response.message)
        )

    def _signal_ready(self, endpoint):
        self.control_socket.connect(endpoint)
        self.control_socket.send_to_client(
            b'$controller',
            b'$uuid',
            {"$signal": "ready"}
        )

    @staticmethod
    def gen_uuid():
        return uuid4().bytes

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
        print("{} - {}".format(self.identity, message))
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


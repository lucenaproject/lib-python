# -*- coding: utf-8 -*-
import tempfile
import threading

import zmq

from lucena.exceptions import ServiceAlreadyStarted, ServiceNotStarted
from lucena.io2.socket import Socket
from lucena.worker import Worker


class Service(Worker):

    class Controller(object):

        def __init__(self, *args, **kwargs):
            self.context = zmq.Context.instance()
            self.args = args
            self.kwargs = kwargs
            self.poller = zmq.Poller()
            self.service_identity = Service.identity()
            self.service_thread = None
            self.control_socket = Socket(self.context, zmq.ROUTER)
            self.control_socket.bind(Socket.inproc_unique_endpoint())

        def is_started(self):
            return self.service_thread is not None

        def start(self):
            if self.service_thread is not None:
                raise ServiceAlreadyStarted()
            service = Service(*self.args, **self.kwargs)
            self.service_thread = threading.Thread(
                target=service,
                daemon=False,
                kwargs={
                    'endpoint': self.control_socket.last_endpoint,
                    'index': 0
                }
            )
            self.service_thread.start()
            message = self.recv()
            assert message == {"$signal": "ready"}

        def stop(self, timeout=None):
            self.send({'$signal': 'stop'})
            message = self.recv()
            assert message == {'$signal': 'stop', '$rep': 'OK'}
            self.service_thread.join(timeout=timeout)
            self.service_thread = None

        def send(self, message):
            if not self.is_started():
                raise ServiceNotStarted()
            return self.control_socket.send_to_worker(
                self.service_identity,
                b'$controller',
                message
            )

        def recv(self):
            # TODO: Add timeout
            if not self.is_started():
                raise ServiceNotStarted()
            worker, client, message = self.control_socket.recv_from_worker()
            assert client == b'$controller'
            assert worker == self.service_identity
            return message

    # Service implementation.

    def __init__(self, worker_factory, endpoint=None, number_of_workers=1):
        # http://zguide.zeromq.org/page:all#Getting-the-Context-Right
        # You should create and use exactly one context in your process.
        super(Service, self).__init__()
        self.worker_factory = worker_factory
        self.endpoint = endpoint if endpoint is not None \
            else "ipc://{}.ipc".format(tempfile.NamedTemporaryFile().name)
        self.number_of_workers = number_of_workers
        self.socket = None
        self.worker_controller = None
        self.worker_ready_ids = None
        self.total_client_requests = 0

    def _before_start(self, identity):
        super(Service, self)._before_start(identity)
        self.worker_ready_ids = []
        self.socket = Socket(self.context, zmq.ROUTER)
        self.socket.bind(self.endpoint)
        self.worker_controller = Worker.Controller(self.worker_factory)
        self.worker_ready_ids = self.worker_controller.start(self.number_of_workers)
        self._add_poll_handler(
            self.socket,
            zmq.POLLIN if self.worker_ready_ids else 0,
            self._handle_socket
        )
        self._add_poll_handler(
            self.worker_controller.control_socket,
            zmq.POLLIN,
            self._handle_worker_controller
        )

    def _before_stop(self):
        super(Service, self)._before_stop()
        self.socket.close()
        self.worker_controller.stop()

    def _handle_socket(self):
        assert len(self.worker_ready_ids) > 0
        client, request = self.socket.recv_from_client()
        worker_name = self.worker_ready_ids.pop(0)
        self.worker_controller.send(worker_name, client, request)
        self.total_client_requests += 1

    def _handle_worker_controller(self):
        worker_id, client, reply = self.worker_controller.recv()
        self.worker_ready_ids.append(worker_id)
        self.socket.send_to_client(client, reply)

    def pending_workers(self):
        return self.worker_ready_ids is not None and \
               len(self.worker_ready_ids) < self.number_of_workers


def create_service(worker_factory=None, endpoint=None, number_of_workers=1):
    return Service.Controller(
        worker_factory,
        endpoint=endpoint,
        number_of_workers=number_of_workers
    )

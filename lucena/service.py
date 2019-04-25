# -*- coding: utf-8 -*-
import tempfile
import threading

import zmq

from lucena.exceptions import ServiceAlreadyStarted, ServiceNotStarted
from lucena.io2.socket import Socket
from lucena.worker import Worker


class Service(Worker):

    class Controller(Worker.Controller):

        def __init__(self, **kwargs):
            super(Service.Controller, self).__init__(**kwargs)
            self.service_thread = None

        def is_started(self):
            return self.service_thread is not None

        def start(self, **kwargs):
            if self.service_thread is not None:
                raise ServiceAlreadyStarted()
            service = Service(**self.kwargs)
            self.service_thread = threading.Thread(
                target=service,
                daemon=False,
                kwargs={
                    'endpoint': self.control_socket.last_endpoint,
                    'identity': b'$service'
                }
            )
            self.service_thread.start()
            self.wait_for_signal('ready', b'$service')

        def stop(self, timeout=None):
            response = self.resolve({'$signal': 'stop'})
            assert response == {'$signal': 'stop', '$rep': 'OK'}
            self.service_thread.join(timeout=timeout)
            self.service_thread = None

        def resolve(self, message, timeout=None):
            if not self.is_started():
                raise ServiceNotStarted()
            self.send(b'$service', b'$controller', b'$uuid', message)
            worker, client, uuid, message = self.recv()
            return message

    # Service implementation.

    def __init__(self, service_name=None, worker_factory=None, endpoint=None,
                 number_of_workers=1, default_timeout=None):
        # http://zguide.zeromq.org/page:all#Getting-the-Context-Right
        # You should create and use exactly one context in your process.
        super(Service, self).__init__(default_timeout=default_timeout)
        if service_name is None:
            service_name = self.__class__.__name__
        self.service_name = service_name
        if worker_factory is None:
            worker_factory = Worker
        self.worker_factory = worker_factory
        self.endpoint = endpoint if endpoint is not None \
            else "ipc://{}.ipc".format(tempfile.NamedTemporaryFile().name)
        self.number_of_workers = number_of_workers
        self.socket = None
        self.worker_controller = None
        self.worker_ready_ids = None
        self.total_client_requests = 0

    def _before_start(self):
        super(Service, self)._before_start()
        self.worker_ready_ids = []
        self.socket = Socket(self.context, zmq.ROUTER)
        self.socket.bind(self.endpoint)
        self.worker_controller = Worker.Controller(
            worker_factory=self.worker_factory
        )
        self.worker_ready_ids = self.worker_controller.start(
            self.number_of_workers
        )
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
        response = self.socket.recv_from_client()
        worker_name = self.worker_ready_ids.pop(0)
        self.worker_controller.send(
            worker_name,
            response.client,
            response.uuid,
            response.message
        )
        self.total_client_requests += 1

    def _handle_worker_controller(self):
        worker_id, client, uuid, message = self.worker_controller.recv()
        self.worker_ready_ids.append(worker_id)
        # TODO: Verify if client is still waiting the reply (timeout happens)
        self.socket.send_to_client(client, uuid, message)

    @property
    def pending_workers(self):
        return self.worker_ready_ids is not None and \
               len(self.worker_ready_ids) < self.number_of_workers


def create_service(service_name, worker_factory=None, endpoint=None,
                   number_of_workers=1):
    return Service.Controller(
        service_name=service_name,
        worker_factory=worker_factory,
        endpoint=endpoint,
        number_of_workers=number_of_workers,
        default_timeout=None
    )

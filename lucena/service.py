# -*- coding: utf-8 -*-
import tempfile

import zmq

from lucena.controller import Controller, NewController
from lucena.io2.socket import Socket


class Service(object):

    def __init__(self, worker_factory, endpoint=None, number_of_workers=1):
        # http://zguide.zeromq.org/page:all#Getting-the-Context-Right
        # You should create and use exactly one context in your process.
        self.context = zmq.Context.instance()
        self.poller = zmq.Poller()
        self.worker_factory = worker_factory
        self.endpoint = endpoint if endpoint is not None \
            else "ipc://{}.ipc".format(tempfile.NamedTemporaryFile().name)
        self.number_of_workers = number_of_workers
        self.socket = None
        self.control_socket = None
        self.proxy_socket = None
        self.worker_controllers = None
        self.worker_ids = None
        self.worker_ready_ids = None
        self.signal_stop = False
        self.total_client_requests = 0

    def _start_worker(self, identity=None):
        worker = self.worker_factory()
        worker.start(self.context, self.proxy_socket.last_endpoint, identity)

    def _plug(self, control_socket, number_of_workers):
        # Init worker queues
        self.worker_controllers = []
        self.worker_ids = []
        self.worker_ready_ids = []
        # Init sockets
        self.socket = Socket(self.context, zmq.ROUTER)
        self.socket.bind(self.endpoint)
        self.proxy_socket = Socket(self.context, zmq.ROUTER)
        self.proxy_socket.bind(Socket.inproc_unique_endpoint())
        self.control_socket = control_socket
        self.control_socket.signal(Socket.SIGNAL_READY)
        # Init workers
        for i in range(number_of_workers):
            worker_id = 'worker#{}'.format(i).encode('utf8')
            worker = self.worker_factory()
            controller = NewController(worker)
            self.worker_controllers.append(controller)
            self.worker_ids.append(worker_id)
            self.worker_ready_ids.append(worker_id)
            controller.start(
                context=self.context,
                endpoint=self.proxy_socket.last_endpoint,
                identity=worker_id
            )

    def _unplug(self):
        self.socket.close()
        while self.worker_controllers:
            worker = self.worker_controllers.pop()
            worker.stop()
        self.proxy_socket.close()
        self.control_socket.close()

    def _handle_poll(self):
        self.poller.register(
            self.control_socket,
            zmq.POLLIN
        )
        self.poller.register(
            self.socket,
            zmq.POLLIN if self.worker_ready_ids and not self.signal_stop else 0
        )
        self.poller.register(
            self.proxy_socket,
            zmq.POLLIN
        )
        return dict(self.poller.poll(.1))

    def _handle_control_socket(self):
        signal = self.control_socket.wait(timeout=10)
        self.signal_stop = self.signal_stop or signal == Socket.SIGNAL_STOP

    def _handle_proxy_socket(self):
        worker_id, client, reply = self.proxy_socket.recv_from_worker()
        self.worker_ready_ids.append(worker_id)
        self.socket.send_to_client(client, reply)

    def _handle_socket(self):
        assert len(self.worker_ready_ids) > 0
        client, request = self.socket.recv_from_client()
        worker_name = self.worker_ready_ids.pop(0)
        self.proxy_socket.send_to_worker(worker_name, client, request)
        self.total_client_requests += 1

    def controller_loop(self, control_socket):
        self._plug(control_socket, self.number_of_workers)
        while not self.signal_stop or self.pending_workers():
            sockets = self._handle_poll()
            if self.control_socket in sockets:
                self._handle_control_socket()
            if self.socket in sockets:
                self._handle_socket()
            if self.proxy_socket in sockets:
                self._handle_proxy_socket()
        self._unplug()

    def pending_workers(self):
        return self.worker_ready_ids is not None and \
               len(self.worker_ready_ids) < self.number_of_workers


def create_service(worker_factory=None, endpoint=None, number_of_workers=1):
    service = Service(worker_factory, endpoint, number_of_workers)
    controller = Controller(service)
    return controller


def main():
    from lucena.worker import Worker
    service = create_service(Worker)
    service.start()


if __name__ == '__main__':
    main()

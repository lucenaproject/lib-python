# -*- coding: utf-8 -*-
import tempfile
import threading

import zmq

from lucena import READY_MESSAGE, VOID_FRAME, STOP_MESSAGE
from lucena.channel import ServiceClientChannel
from lucena.controller import Controller
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
        self.control_socket = None
        self.proxy_socket = None
        self.client_channel = ServiceClientChannel(
            self.context,
            self.endpoint
        )
        self.worker_threads = []
        self.worker_ids = []
        self.worker_ready_ids = []
        self.signal_stop = False
        self.total_client_requests = 0

    def __del__(self):
        self.context.term()

    def _start_worker(self, identity=None):
        worker = self.worker_factory()
        worker.start(self.context, self.proxy_socket.last_endpoint, identity)

    def _plug_workers(self, number_of_workers):
        assert self.worker_ids == []
        self.proxy_socket = Socket(self.context, zmq.ROUTER)
        self.proxy_socket.bind(Socket.inproc_unique_endpoint())
        for i in range(number_of_workers):
            worker_id = 'worker#{}'.format(i).encode('utf8')
            thread = threading.Thread(
                daemon=False,
                target=self._start_worker,
                args=(worker_id,)
            )
            thread.start()
            worker_id, client, message = self.proxy_socket.recv_from_worker()
            assert client == VOID_FRAME
            assert message == READY_MESSAGE
            self.worker_threads.append(thread)
            self.worker_ids.append(worker_id)
            self.worker_ready_ids.append(worker_id)

    def _unplug_workers(self):
        while self.worker_ids:
            worker_id = self.worker_ids.pop()
            self.proxy_socket.send_to_worker(worker_id, VOID_FRAME, STOP_MESSAGE)

    def _handle_poll(self):
        self.poller.register(
            self.control_socket,
            zmq.POLLIN
        )
        self.poller.register(
            self.client_channel.socket,
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
        self.client_channel.send(client, reply)

    def _handle_client_channel(self):
        assert len(self.worker_ready_ids) > 0
        client, request = self.client_channel.recv()
        worker_name = self.worker_ready_ids.pop(0)
        self.proxy_socket.send_to_worker(worker_name, client, request)
        self.total_client_requests += 1

    def controller_loop(self, control_socket):
        self.control_socket = control_socket
        control_socket.signal(Socket.SIGNAL_READY)
        self._plug_workers(self.number_of_workers)
        while not self.signal_stop or self.pending_workers():
            sockets = self._handle_poll()
            if self.control_socket in sockets:
                self._handle_control_socket()
            if self.client_channel.socket in sockets:
                self._handle_client_channel()
            if self.proxy_socket in sockets:
                self._handle_proxy_socket()
        self._unplug_workers()
        self.control_socket.close()
        self.proxy_socket.close()
        self.client_channel.close()

    def pending_workers(self):
        return len(self.worker_ready_ids) < self.number_of_workers


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

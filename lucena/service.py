# -*- coding: utf-8 -*-
import threading

import zmq

from lucena.channel import ServiceClientChannel, ServiceWorkerChannel


class Service(object):
    def __init__(self, worker_factory, number_of_workers=4):
        # http://zguide.zeromq.org/page:all#Getting-the-Context-Right
        # You should create and use exactly one context in your process.
        self.context = zmq.Context.instance()
        self.worker_factory = worker_factory
        self.client_channel = ServiceClientChannel(
            self.context,
            "ipc://frontend.ipc"
        )
        self.poller = zmq.Poller()
        self.worker_channel = None
        self.worker_threads = []
        self.number_of_workers = number_of_workers
        self.ready_workers = []
        self.stop_flag = False
        self.total_client_requests = 0

    def __del__(self):
        self.context.term()

    def _start_worker(self, worker_name):
        worker = self.worker_factory(worker_name, service=self)
        worker.start()

    def _init_workers(self, number_of_workers):
        assert self.worker_channel is None
        self.worker_channel = ServiceWorkerChannel(self.context)
        for i in range(number_of_workers):
            worker_name = 'worker-{}'.format(i).encode("utf-8")
            thread = threading.Thread(
                target=self._start_worker,
                args=(worker_name,)
            )
            self.worker_threads.append(thread)
            thread.start()
            self.worker_channel.plug_worker_by_name(worker_name)
            self.ready_workers.append(worker_name)

    def _update_client_channel_poll(self):
        if self.ready_workers and not self.stop_flag:
            self.poller.register(self.client_channel.socket, zmq.POLLIN)
        else:
            if self.client_channel.socket in self.poller:
                self.poller.unregister(self.client_channel.socket)

    def _update_worker_channel_poll(self):
        if not (self.worker_channel.socket in self.poller):
            self.poller.register(self.worker_channel.socket, zmq.POLLIN)

    def _handle_poll(self):
        self._update_client_channel_poll()
        self._update_worker_channel_poll()
        return dict(self.poller.poll(.1))

    def _handle_worker_channel(self):
        worker_name, client, reply = self.worker_channel.recv()
        self.ready_workers.append(worker_name)
        self.client_channel.send(client, reply)

    def _handle_client_channel(self):
        assert len(self.ready_workers) > 0
        client, request = self.client_channel.recv()
        worker_name = self.ready_workers.pop(0)
        self.worker_channel.send(worker_name, client, request)
        self.total_client_requests += 1

    def _io_loop(self):
        while True:
            sockets = self._handle_poll()
            if self.client_channel.socket in sockets:
                self._handle_client_channel()
            if self.worker_channel.socket in sockets:
                self._handle_worker_channel()
            if self.stop_flag and not self.pending_workers():
                break
        self.client_channel.close()
        self.worker_channel.close()

    def pending_workers(self):
        return len(self.ready_workers) < self.number_of_workers

    def start(self):
        self._init_workers(self.number_of_workers)
        threading.Thread(target=self._io_loop).start()

    def stop(self):
        self.stop_flag = True
        while self.pending_workers():
            pass
        print("Service STOP")

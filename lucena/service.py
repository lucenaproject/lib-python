# -*- coding: utf-8 -*-
import threading

import zmq

from lucena.channel import ServiceClientChannel, ServiceWorkerChannel
from lucena.worker import Worker


class Service(object):
    def __init__(self, worker, number_of_workers=4):
        self.context = zmq.Context.instance()
        self.worker = worker
        self.client_channel = ServiceClientChannel(
            self.context,
            "ipc://frontend.ipc"
        )
        self.poller = zmq.Poller()
        self.worker_channel = ServiceWorkerChannel(self.context)
        self.number_of_workers = number_of_workers
        self.ready_workers = []
        self.stop_flag = False

    def __del__(self):
        self.context.term()

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
        worker, client, reply = self.worker_channel.recv()
        self.ready_workers.append(worker)
        self.client_channel.send(client, reply)

    def _handle_client_channel(self):
        assert len(self.ready_workers) > 0
        client, request = self.client_channel.recv()
        worker = self.ready_workers.pop(0)
        self.worker_channel.send(worker, client, request)

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
        for _ in range(self.number_of_workers):
            worker = self.worker_channel.plug_worker(self.worker)
            self.ready_workers.append(worker)
        threading.Thread(target=self._io_loop).start()

    def stop(self):
        self.stop_flag = True


def client_task(ident):
    """Basic request-reply client using REQ socket."""
    socket = zmq.Context().socket(zmq.REQ)
    socket.identity = u"Client-{}".format(ident).encode("ascii")
    socket.connect("ipc://frontend.ipc")

    # Send request, get reply
    socket.send(b'{"$req": "HELLO"}')
    reply = socket.recv()
    print("{}: {}".format(
        socket.identity.decode("ascii"),
        reply.decode("ascii"))
    )


# Start background tasks
def start(task, *args):
    import multiprocessing
    process = multiprocessing.Process(target=task, args=args)
    process.start()


def main2():
    for i in range(10):
        start(client_task, i)
    s = Service(worker=Worker)
    s.start()
    import time
    time.sleep(1)
    s.stop()


if __name__ == '__main__':
    main2()

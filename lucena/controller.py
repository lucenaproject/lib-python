# -*- coding: utf-8 -*-
import collections
import threading

import zmq

from lucena.io2.socket import Socket, RouteSocket


class Controller(object):

    def __init__(self, slave):
        self.context = zmq.Context.instance()
        self.slave = slave
        self.thread = None
        self.master_socket = None

    def start(self, **kwargs):
        self.master_socket, slave_socket = Socket.socket_pair(self.context)
        self.thread = threading.Thread(
            target=self.slave.controller_loop,
            daemon=False,
            args=(slave_socket,),
            kwargs=kwargs
        )
        self.thread.start()
        signal = self.master_socket.wait()
        assert signal == Socket.SIGNAL_READY

    def stop(self, timeout=None):
        self.master_socket.signal(Socket.SIGNAL_STOP)
        self.thread.join(timeout=timeout)
        self.master_socket.close()
        self.thread = None


class WorkerController(object):

    RunningWorker = collections.namedtuple(
        'RunningWorker',
        ['worker', 'thread']
    )

    def __init__(self, proxy_socket):
        self.proxy_socket = proxy_socket
        self.poller = zmq.Poller()
        self.running_workers = {}

    def start(self, worker, worker_id):
        assert worker_id not in self.running_workers
        thread = threading.Thread(
            target=worker.controller_loop,
            daemon=False,
            kwargs={
                'endpoint': self.proxy_socket.last_endpoint,
                'identity': worker_id
            }
        )
        self.running_workers[worker_id] = self.RunningWorker(worker, thread)
        thread.start()
        _worker_id, client, message = self.proxy_socket.recv_from_worker()
        assert _worker_id == worker_id
        assert client == b'$controller'
        assert message == {"$signal": "ready"}

    def stop(self, timeout=None):
        for worker_id, running_worker in self.running_workers.items():
            self.proxy_socket.send_to_worker(
                worker_id,
                b'$controller', {'$signal': 'stop'}
            )
            _worker_id, client, message = self.proxy_socket.recv_from_worker()
            assert(_worker_id == worker_id)
            assert(client == b'$controller')
            assert(message == {'$signal': 'stop', '$rep': 'OK'})
            running_worker.thread.join(timeout=timeout)
        self.running_workers = {}


# class NewController(object):
#
#     def __init__(self):
#         self.context = zmq.Context.instance()
#         self.thread = None
#         self.master_socket = None
#         self.workers = {}
#         self.ready_workers = []
#         self.control_socket = RouteSocket(self.context, zmq.ROUTER)
#         self.control_socket.bind(Socket.inproc_unique_endpoint())
#
#     def start(self, worker, identity):
#         if identity in self.workers:
#             raise ValueError("Identity {} already used.")
#         self.workers[identity] = worker
#         self.thread = threading.Thread(
#             target=worker.plug_controller,
#             daemon=False,
#             kwargs={
#                 'endpoint': self.control_socket.last_endpoint,
#                 'context': self.context,
#                 'identity':identity
#             }
#         )
#         self.thread.start()
#         signal = self.control_socket.wait()
#         assert signal == Socket.SIGNAL_READY
#         self.ready_workers.append(identity)
#
#     def stop(self, timeout=None):
#         self.master_socket.signal(Socket.SIGNAL_STOP)
#         self.thread.join(timeout=timeout)
#         self.master_socket.close()
#         self.thread = None
#
#     def send(self, worker, client, message):
#         self.control_socket.send_to_worker(worker, client, message)
#
#     def recv(self):
#         return self.control_socket.recv_from_worker()
#
#     def ready_worker(self):
#         return len(self.ready_workers) > 0

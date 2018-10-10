# -*- coding: utf-8 -*-
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


class NewController(object):

    def __init__(self, slave, slave_id, proxy_socket):
        self.context = zmq.Context.instance()
        self.slave = slave
        self.slave_id = slave_id
        self.proxy_socket = proxy_socket
        self.thread = None
        self.master_socket = None

    def start(self, **kwargs):
        # TODO: Implement signal in self.proxy_socket and remove control_socket from Worker.
        # TODO: Support multiple workers in NewController.
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
        self.proxy_socket.send_to_worker(self.slave_id, b'$controller', {'$signal': 'stop'})
        # worker, client, message = self.proxy_socket.recv_from_worker()
        # assert(worker == self.slave_id)
        # assert(client == b'$controller')
        # assert(message == {'$signal': 'stop', '$rep': 'OK'})
        self.thread.join(timeout=timeout)
        self.master_socket.close()
        self.thread = None


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

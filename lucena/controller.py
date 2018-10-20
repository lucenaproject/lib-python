# -*- coding: utf-8 -*-
import collections
import threading

import zmq

from lucena.io2.socket import Socket


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


class Controller2(object):

    RunningWorker = collections.namedtuple(
        'RunningWorker',
        ['worker', 'thread']
    )

    def __init__(self, worker_factory, *worker_args, **worker_kwargs):
        self.context = zmq.Context.instance()
        self.worker_factory = worker_factory
        self.worker_args = worker_args
        self.worker_kwargs = worker_kwargs
        self.poller = zmq.Poller()
        self.running_workers = {}
        self.control_socket = Socket(self.context, zmq.ROUTER)
        self.control_socket.bind(Socket.inproc_unique_endpoint())

    def start(self, number_of_workers=1):
        for i in range(number_of_workers):
            worker = self.worker_factory()
            worker_id = 'worker#{}'.format(i).encode('utf8')
            thread = threading.Thread(
                target=worker.controller_loop,
                daemon=False,
                kwargs={
                    'endpoint': self.control_socket.last_endpoint,
                    'identity': worker_id
                }
            )
            self.running_workers[worker_id] = self.RunningWorker(worker, thread)
            thread.start()
            _worker_id, client, message = self.control_socket.recv_from_worker()
            assert _worker_id == worker_id
            assert client == b'$controller'
            assert message == {"$signal": "ready"}
        return list(self.running_workers.keys())

    def stop(self, timeout=None):
        for worker_id, running_worker in self.running_workers.items():
            self.control_socket.send_to_worker(
                worker_id,
                b'$controller', {'$signal': 'stop'}
            )
            _worker_id, client, message = self.control_socket.recv_from_worker()
            assert(_worker_id == worker_id)
            assert(client == b'$controller')
            assert(message == {'$signal': 'stop', '$rep': 'OK'})
            running_worker.thread.join(timeout=timeout)
        self.running_workers = {}

    def message_queued(self, timeout=0.01):
        self.poller.register(
            self.control_socket,
            zmq.POLLIN
        )
        return bool(self.poller.poll(timeout))

    def send(self, worker, client, message):
        return self.control_socket.send_to_worker(worker, client, message)

    def recv(self):
        return self.control_socket.recv_from_worker()

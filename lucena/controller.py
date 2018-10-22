# -*- coding: utf-8 -*-
import collections
import re
import threading

import zmq

from lucena.io2.socket import Socket


class Controller(object):

    RunningSlave = collections.namedtuple(
        'RunningSlave',
        ['slave', 'thread']
    )

    def __init__(self, slave, *args, **kwargs):
        self.context = zmq.Context.instance()
        self.slave = slave
        self.args = args
        self.kwargs = kwargs
        self.poller = zmq.Poller()
        self.running_slaves = {}
        self.control_socket = Socket(self.context, zmq.ROUTER)
        self.control_socket.bind(Socket.inproc_unique_endpoint())

    def _start(self, number_of_slaves=1):
        for i in range(number_of_slaves):
            slave = self.slave(*self.args, **self.kwargs)
            slave_id = self.identity_for(i)
            thread = threading.Thread(
                target=slave.controller_loop,
                daemon=False,
                kwargs={
                    'endpoint': self.control_socket.last_endpoint,
                    'identity': slave_id
                }
            )
            self.running_slaves[slave_id] = self.RunningSlave(slave, thread)
            thread.start()
            _slave_id, client, message = self.control_socket.recv_from_worker()
            assert _slave_id == slave_id
            assert client == b'$controller'
            assert message == {"$signal": "ready"}
        return list(self.running_slaves.keys())

    def _stop(self, timeout=None):
        for slave_id, running_worker in self.running_slaves.items():
            self.control_socket.send_to_worker(
                slave_id,
                b'$controller', {'$signal': 'stop'}
            )
            _worker_id, client, message = self.control_socket.recv_from_worker()
            assert(_worker_id == slave_id)
            assert(client == b'$controller')
            assert(message == {'$signal': 'stop', '$rep': 'OK'})
            running_worker.thread.join(timeout=timeout)
        self.running_slaves = {}

    def identity_for(self, index):
        id1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', self.slave.__name__)
        id2 = re.sub('([a-z0-9])([A-Z])', r'\1_\2', id1).lower()
        return '{}#{}'.format(id2, index).encode('utf8')

    def start(self, **kwargs):
        number_of_slaves = kwargs.get('number_of_slaves')
        assert number_of_slaves is not None
        return self._start(number_of_slaves)

    def stop(self, **kwargs):
        timeout = kwargs.get('timeout')
        return self._stop(timeout)

    def send(self, message, **kwargs):
        worker = kwargs.get('worker')
        client = kwargs.get('client')
        assert worker is not None
        assert client is not None
        return self.control_socket.send_to_worker(worker, client, message)

    def recv(self):
        return self.control_socket.recv_from_worker()

    def message_queued(self, timeout=0.01):
        self.poller.register(
            self.control_socket,
            zmq.POLLIN
        )
        return bool(self.poller.poll(timeout))

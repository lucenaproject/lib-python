# -*- coding: utf-8 -*-
import collections
import threading

import zmq

from lucena.io2.socket import Socket
from lucena.message_handler import MessageHandler


class Worker(object):

    def __init__(self):
        self.context = zmq.Context.instance()
        self.poller = zmq.Poller()
        self.message_handlers = []
        self.bind_handler({}, self.default_handler)
        self.bind_handler({'$signal': 'stop'}, self.stop_handler)
        self.stop_signal = False
        self.control_socket = None

    def _handle_poll(self):
        self.poller.register(
            self.control_socket,
            zmq.POLLIN if not self.stop_signal else 0
        )
        return dict(self.poller.poll(.1))

    def _handle_ctrl_socket(self):
        client, message = self.control_socket.recv_from_client()
        response = self.resolve(message)
        self.control_socket.send_to_client(client, response)

    @staticmethod
    def default_handler(message):
        response = {}
        response.update(message)
        response.update({"$rep": None, "$error": "No handler match"})
        return response

    def stop_handler(self, message):
        response = {}
        response.update(message)
        response.update({'$rep': 'OK'})
        self.stop_signal = True
        return response

    def bind_handler(self, message, handler):
        self.message_handlers.append(MessageHandler(message, handler))
        self.message_handlers.sort()

    def get_handler_for(self, message):
        for message_handler in self.message_handlers:
            if message_handler.match_in(message):
                return message_handler.handler
        raise LookupError("No handler for {}".format(message))

    def resolve(self, message):
        handler = self.get_handler_for(message)
        return handler(message)

    def controller_loop(self, endpoint, identity=None):
        self.control_socket = Socket(self.context, zmq.REQ, identity=identity)
        self.control_socket.connect(endpoint)
        self.control_socket.send_to_client(b'$controller', {"$signal": "ready"})
        while not self.stop_signal:
            sockets = self._handle_poll()
            if self.control_socket in sockets:
                self._handle_ctrl_socket()


class WorkerController(object):

    RunningWorker = collections.namedtuple(
        'RunningWorker',
        ['worker', 'thread']
    )

    def __init__(self, worker_factory):
        self.context = zmq.Context.instance()
        self.worker_factory = worker_factory
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


class MathWorker(Worker):
    def __init__(self):
        super(MathWorker, self).__init__()
        self.bind_handler({'$req': 'sum'}, self.sum)
        self.bind_handler({'$req': 'multiply'}, self.multiply)

    @staticmethod
    def sum(message):
        result = message.get('a') + message.get('b')
        return {'$rep': result}

    @staticmethod
    def multiply(message):
        result = message.get('a') * message.get('b')
        return {'$rep': result}


if __name__ == '__main__':
    def main():
        worker = MathWorker()
        response_message = worker.resolve({
            "$service": "math",
            "$req": "sum",
            "a": 100,
            "b": 20
        })
        print(response_message)
    main()

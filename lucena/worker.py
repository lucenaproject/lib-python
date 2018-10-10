# -*- coding: utf-8 -*-
import zmq

from lucena.io2.socket import Socket
from lucena.message_handler import MessageHandler


class Worker(MessageHandler):
    def __init__(self):
        self.socket = None
        super(Worker, self).__init__()

    def controller_loop(self, control_socket, context, endpoint, identity=None):
        self.socket = Socket(context, zmq.REP, identity=identity)
        self.socket.connect(endpoint)
        self.control_socket = control_socket
        self.control_socket.signal(Socket.SIGNAL_READY)
        while not self.stop_signal:
            sockets = self._handle_poll()
            if self.control_socket in sockets:
                self._handle_control_socket()
            if self.socket in sockets:
                self._handle_socket()

    def _handle_poll(self):
        self.poller.register(
            self.control_socket,
            zmq.POLLIN
        )
        self.poller.register(
            self.socket,
            zmq.POLLIN if not self.stop_signal else 0
        )
        return dict(self.poller.poll(.1))

    def _handle_control_socket(self):
        signal = self.control_socket.wait(timeout=10)
        self.stop_signal = self.stop_signal or signal == Socket.SIGNAL_STOP

    def _handle_socket(self):
        client, message = self.socket.recv_from_client()
        response = self.resolve(message)
        self.socket.send_to_client(client, response)


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
    worker = MathWorker()
    response_message = worker.resolve({
        "$service": "math",
        "$req": "sum",
        "a": 100,
        "b": 20
    })
    print(response_message)

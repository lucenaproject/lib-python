# -*- coding: utf-8 -*-
import zmq

from lucena.io2.socket import Socket
from lucena.message_handler import MessageHandler


class Worker(MessageHandler):

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

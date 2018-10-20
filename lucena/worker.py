# -*- coding: utf-8 -*-
import zmq

from lucena.io2.socket import Socket
from lucena.message_handler import MessageHandler


class Worker(object):

    def __init__(self, *args, **kwargs):
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

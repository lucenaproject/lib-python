# -*- coding: utf-8 -*-
import zmq

from lucena import STOP_MESSAGE, READY_MESSAGE, VOID_FRAME
from lucena.io2.socket import Socket
from lucena.message_handler import MessageHandler


class Worker(object):
    def __init__(self):
        self.message_handlers = []
        self.bind_handler({}, self.default_handler)

    @staticmethod
    def default_handler(message):
        response = {}
        response.update(message)
        response.update({"$rep": None, "$error": "No handler match"})
        return response

    def bind_handler(self, message, handler):
        self.message_handlers.append(MessageHandler(message, handler))
        self.message_handlers.sort()

    def bind_remote_handler(self, message, handler_endpoint):
        pass

    def get_handler_for(self, message):
        for message_handler in self.message_handlers:
            if message_handler.match_in(message):
                return message_handler.handler
        raise LookupError("No handler for {}".format(message))

    def resolve(self, message):
        handler = self.get_handler_for(message)
        return handler(message)

    def start(self, context, endpoint, identity=None):
        socket = Socket(context, zmq.REQ, identity=identity)
        socket.connect(endpoint)
        socket.send_to_client(VOID_FRAME, READY_MESSAGE)
        while True:
            client, message = socket.recv_from_client()
            if message == STOP_MESSAGE:
                break
            response = self.resolve(message)
            socket.send_to_client(client, response)


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

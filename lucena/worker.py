# -*- coding: utf-8 -*-
import zmq

from lucena.io2.socket import Socket
from lucena.message_handler import MessageHandler


class Worker(MessageHandler):
    pass


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

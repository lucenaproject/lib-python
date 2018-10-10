# -*- coding: utf-8 -*-
import json

import zmq

from lucena.io2.socket import Socket


class MessageHandlerPair(object):
    """
    A message is basically a request (REQ) from a client, a handler is a
    function that resolves the request and returns a reply (REP).
    This class maps a message with a callable function (the handler).

    The handler evaluation order is important and it's determined by the
    following rules (implemented in the __lt__ method):

    1. More properties win.
        {a:1, b:2} wins over {a:1} as it has more properties.

    2. If same number of properties, alphabetical order win.
        {a:1, b:2} wins over {a:1, c:3} as b comes before c alphabetically.
        {a:1, b:2, d:4} wins over {a:1, c:3, d:4} as b comes before c.

    3. If same properties, local handlers win.
        The local handler {a:1, b:2} wins over the remote one {a:1, b:2}.
    """
    def __init__(self, message, handler):
        self.message = message
        self.handler = handler
        self.key = json.dumps(message, sort_keys=True)

    @property
    def is_local(self):
        return True

    def __lt__(self, other):
        """
        This function implements the precedence order of message handlers:
          1. More properties win.
          2. If same number of properties, alphabetical order win.
          3. If same properties, local handlers win.
        """
        if len(self.message) > len(other.message):
            return True
        if len(self.message) == len(other.message) and self.key < other.key:
            return True
        if self.message == other.message and self.is_local:
            return True
        return False

    def __str__(self):
        return self.key

    def match_in(self, message):
        try:
            for key, value in self.message.items():
                assert (message[key] == value)
            return True
        except (AssertionError, KeyError):
            return False


class MessageHandler(object):

    def __init__(self):
        self.poller = zmq.Poller()
        self.message_handlers = []
        self.bind_handler({}, self.default_handler)
        self.stop_signal = False
        self.control_socket = None

    def _handle_poll(self):
        self.poller.register(
            self.control_socket,
            zmq.POLLIN
        )
        return dict(self.poller.poll(.1))

    def _handle_control_socket(self):
        client, message = self.control_socket.recv_from_client()
        response = self.resolve(message)
        self.control_socket.send_to_client(client, response)

    @staticmethod
    def default_handler(message):
        response = {}
        response.update(message)
        response.update({"$rep": None, "$error": "No handler match"})
        return response

    def bind_handler(self, message, handler):
        self.message_handlers.append(MessageHandlerPair(message, handler))
        self.message_handlers.sort()

    def get_handler_for(self, message):
        for message_handler in self.message_handlers:
            if message_handler.match_in(message):
                return message_handler.handler
        raise LookupError("No handler for {}".format(message))

    def resolve(self, message):
        handler = self.get_handler_for(message)
        return handler(message)

    def plug_controller(self, endpoint, context, identity=None):
        self.control_socket = Socket(context, zmq.REQ, identity=identity)
        self.control_socket.connect(endpoint)
        self.control_socket.signal(Socket.SIGNAL_READY)
        while not self.stop_signal:
            sockets = self._handle_poll()
            if self.control_socket in sockets:
                self._handle_control_socket()

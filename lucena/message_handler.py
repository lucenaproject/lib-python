# -*- coding: utf-8 -*-
import json


class MessageHandler(object):
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

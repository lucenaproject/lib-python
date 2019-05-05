# -*- coding: utf-8 -*-
import logging
import time

import zmq

import lucena.majordomo.MDP as MDP
from lucena.exceptions import LookupHandlerError
from lucena.message_handler import MessageHandler


class Worker(object):
    """
    Implements the MDP/Worker spec at http:#rfc.zeromq.org/spec:7.
    """
    HEARTBEAT_LIVENESS = 3
    heartbeat_at = 0  # When to send HEARTBEAT (relative to time.time())
    liveness = 0  # How many attempts left
    heartbeat = 2500  # Heartbeat delay, msecs
    reconnect = 2500  # Reconnect delay, msecs

    # Internal state
    expect_reply = False  # False only at start

    timeout = 2500  # poller timeout

    # Return address, if any
    reply_to = None

    def __init__(self, broker_endpoint):
        self.broker_endpoint = broker_endpoint
        self.socket = None
        self.message_handlers = []
        self.stop_signal = False
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.bind_handler({}, self.handler_default)
        self.bind_handler({'$signal': 'stop'}, self.handler_stop)
        self.bind_handler({'$req': 'eval'}, self.handler_eval)
        self.reconnect_to_broker()

    def reconnect_to_broker(self):
        if self.socket:
            self.poller.unregister(self.socket)
            self.socket.close()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.linger = 0
        self.socket.connect(self.broker_endpoint)
        self.poller.register(self.socket, zmq.POLLIN)
        logging.info("connecting to broker at %s", self.broker_endpoint)

        # Register service with broker
        self.send_to_broker(MDP.W_READY, b'$echo', [])

        # If liveness hits zero, queue is considered disconnected
        self.liveness = self.HEARTBEAT_LIVENESS
        self.heartbeat_at = time.time() + 1e-3 * self.heartbeat

    def send_to_broker(self, command, option=None, message=None):
        """
        Send message to broker.
        If no msg is provided, creates one internally
        """
        message_parts = [b'', MDP.W_WORKER, command]
        if option is not None:
            message_parts.append(option)
        if message is None:
            message = []
        if not isinstance(message, list):
            message = [message]
        message_parts.extend(message)
        logging.debug("[WORKER] send: %s", message_parts)
        self.socket.send_multipart(message_parts)

    def recv(self, reply=None):
        """
        Send reply, if any, to broker and wait for next request.
        """
        # Format and send the reply if we were provided one
        assert reply is not None or not self.expect_reply

        if reply is not None:
            assert self.reply_to is not None
            reply = [self.reply_to, b''] + reply
            self.send_to_broker(MDP.W_REPLY, message=reply)

        self.expect_reply = True

        while True:
            # Poll socket for a reply, with timeout

            items = self.poller.poll(self.timeout)

            if items:
                msg = self.socket.recv_multipart()
                logging.debug("received message from broker: %s", msg)

                self.liveness = self.HEARTBEAT_LIVENESS
                assert len(msg) >= 3

                empty = msg.pop(0)
                assert empty == b''

                header = msg.pop(0)
                assert header == MDP.W_WORKER

                command = msg.pop(0)
                if command == MDP.W_REQUEST:
                    # We should pop and save as many addresses as there are
                    # up to a null part, but for now, just save one…
                    self.reply_to = msg.pop(0)
                    # pop empty
                    empty = msg.pop(0)
                    assert empty == b''

                    return msg  # We have a request to process
                elif command == MDP.W_HEARTBEAT:
                    # Do nothing for heartbeats
                    pass
                elif command == MDP.W_DISCONNECT:
                    self.reconnect_to_broker()
                else:
                    logging.error("invalid input message: ")
                    print(msg)

            else:
                self.liveness -= 1
                if self.liveness == 0:
                    logging.warning("disconnected from broker - retrying...")
                    try:
                        time.sleep(1e-3 * self.reconnect)
                    except KeyboardInterrupt:
                        break
                    self.reconnect_to_broker()

            # Send HEARTBEAT if it's time
            if time.time() > self.heartbeat_at:
                self.send_to_broker(MDP.W_HEARTBEAT)
                self.heartbeat_at = time.time() + 1e-3 * self.heartbeat

        logging.warning("interrupt received, killing worker")
        return None

    def destroy(self):
        self.context.destroy(0)

    @staticmethod
    def handler_default(message):
        response = {}
        response.update(message)
        response.update({"$rep": None, "$error": "No handler match"})
        return response

    def handler_eval(self, message):
        response = {}
        response.update(message)
        attr = getattr(self, message.get('$attr'))
        response.update({'$rep': attr})
        return response

    def handler_stop(self, message):
        response = {}
        response.update(message)
        response.update({'$rep': 'OK'})
        self.stop_signal = True
        return response

    def bind_handler(self, message, handler):
        self.message_handlers.append(MessageHandler(message, handler))
        self.message_handlers.sort()

    def unbind_handler(self, message):
        for message_handler in self.message_handlers:
            if message_handler.message == message:
                self.message_handlers.remove(message_handler)
                return
        raise LookupHandlerError("No handler for {}".format(message))

    def get_handler_for(self, message):
        for message_handler in self.message_handlers:
            if message_handler.match_in(message):
                return message_handler.handler
        raise LookupHandlerError("No handler for {}".format(message))

    def resolve(self, message):
        handler = self.get_handler_for(message)
        return handler(message)

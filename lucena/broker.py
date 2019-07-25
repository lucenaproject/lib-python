# -*- coding: utf-8 -*-

import json
import logging
import time
from binascii import hexlify

import zmq

from lucena.majordomo import MDP
from lucena.message_handler import MessageDispatcher


class Broker(object):
    """
    Majordomo Protocol broker
    http:#rfc.zeromq.org/spec:7 and spec:8
    """

    INTERNAL_SERVICE_PREFIX = "mmi."
    HEARTBEAT_LIVENESS = 3
    HEARTBEAT_INTERVAL = 2500
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    class Message(object):
        def __init__(self, sender=None, header=None, payload=None):
            if payload is None:
                payload = []
            assert isinstance(payload, list)
            self.sender = sender
            self.header = header
            self.payload = payload

    class Service(object):
        def __init__(self, name):
            self.name = name
            self.client_requests = []
            self.idle_workers = []

    class Worker(object):
        def __init__(self, identity, address, lifetime):
            self.identity = identity
            self.address = address
            self.expiry = time.time() + 1e-3 * lifetime

    def __init__(self):
        self.services = {}
        self.workers = {}
        self.idle_workers = []
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.socket = self.context.socket(zmq.ROUTER)
        self.message_dispatcher = MessageDispatcher()
        self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL
        # Init worker state.
        self.socket.linger = 0
        self.poller.register(self.socket, zmq.POLLIN)
        # Init internal handlers.
        self.message_dispatcher.bind_handler(
            {'$req': '$service'},
            self.handler_service
        )

    def recv(self):
        frames = self.socket.recv_multipart()
        logging.debug("[BROKER] recv: %s", frames)
        assert len(frames) >= 3
        assert frames[1] == b''
        message = Broker.Message(
            sender=frames[0],
            header=frames[2],
            payload=frames[3:]
        )
        return message

    def mediate(self):
        """
        Main broker work happens here
        """
        while True:
            try:
                items = self.poller.poll(self.HEARTBEAT_INTERVAL)
            except KeyboardInterrupt:
                break
            if items:
                message = self.recv()
                if MDP.C_CLIENT == message.header:
                    self.process_client(message)
                elif MDP.W_WORKER == message.header:
                    self.process_worker(message)
                else:
                    logging.error("invalid message: %s", message)
            self.purge_workers()
            self.send_heartbeats()

    def destroy(self):
        """
        Disconnect all workers, destroy context.
        """
        while self.workers:
            self.delete_worker(self.workers.values()[0], True)
        self.context.destroy(0)

    def process_client(self, message):
        """
        Process a request coming from a client.
        """
        payload = json.loads(message.payload[0])  # TODO: Mover a self.recv
        response = self.message_dispatcher.resolve(payload)
        self.socket.send_multipart([
            message.sender,
            b'',
            MDP.C_CLIENT,
            json.dumps(response).encode('utf-8')
        ])

        # service = message.payload[0]
        # if service.startswith(self.INTERNAL_SERVICE_PREFIX):
        #     self.service_internal(
        #         service,
        #         [message.sender, b''] + message.payload
        #     )
        # else:
        #     self.dispatch(
        #         self.require_service(service),
        #         [message.sender, b''] + message.payload
        #     )

    def process_worker(self, message):
        """
        Process message sent to us by a worker.
        """
        assert len(message.payload) >= 1
        command = message.payload[0]
        worker_ready = hexlify(message.sender) in self.workers
        worker = self.require_worker(message.sender)

        if MDP.W_READY == command:
            assert len(message.payload) >= 2
            service = message.payload[1].decode('utf-8')
            # Not first command in session or Reserved service name
            if worker_ready or service.startswith(self.INTERNAL_SERVICE_PREFIX):
                self.delete_worker(worker, True)
            else:
                worker.service = self.require_service(service)
                self.worker_waiting(worker)

        elif MDP.W_REPLY == command:
            if worker_ready:
                client = message.payload[1]
                assert message.payload[2] == b''
                self.socket.send_multipart(
                    [client, b'', MDP.C_CLIENT, worker.service.name] + message.payload[3:]
                )
                self.worker_waiting(worker)
            else:
                self.delete_worker(worker, True)

        elif MDP.W_HEARTBEAT == command:
            if worker_ready:
                worker.expiry = time.time() + 1e-3 * self.HEARTBEAT_EXPIRY
            else:
                self.delete_worker(worker, True)

        elif MDP.W_DISCONNECT == command:
            self.delete_worker(worker, False)
        else:
            logging.error("invalid message: %s", message)

    def delete_worker(self, worker, disconnect):
        """
        Deletes worker from all data structures, and deletes worker.
        """
        assert worker is not None
        if disconnect:
            self.send_to_worker(worker, MDP.W_DISCONNECT, None, None)
        if worker.service is not None:
            worker.service.idle_workers.remove(worker)
        self.workers.pop(worker.identity)

    def require_worker(self, address):
        """
        Finds the worker (creates if necessary).
        """
        assert (address is not None)
        identity = hexlify(address)
        worker = self.workers.get(identity)
        if worker is None:
            worker = Broker.Worker(identity, address, self.HEARTBEAT_EXPIRY)
            self.workers[identity] = worker
            logging.debug("[BROKER] registering new worker: %s", identity)
        return worker

    def require_service(self, name):
        """
        Locates the service (creates if necessary).
        """
        assert (name is not None)
        service = self.services.get(name)
        if service is None:
            service = Broker.Service(name)
            self.services[name] = service
        return service

    def bind(self, endpoint):
        """
        Bind broker to endpoint, can call this multiple times.
        We use a single socket for both clients and workers.
        """
        self.socket.bind(endpoint)
        logging.info("MDP broker/0.1.1 is active at %s", endpoint)

    def service_internal(self, service, message):
        """
        Handle internal service according to 8/MMI specification
        """
        return_code = b"501"
        if b"mmi.service" == service:
            name = message[-1]
            return_code = b"200" if name in self.services else b"404"
        message[-1] = return_code

        # insert the protocol header and service name after
        # the routing envelope ([client, ''])
        message = message[:2] + [MDP.C_CLIENT, service] + message[2:]
        self.socket.send_multipart(message)

    def send_heartbeats(self):
        """
        Send heartbeats to idle workers if it's time
        """
        if time.time() > self.heartbeat_at:
            for worker in self.idle_workers:
                self.send_to_worker(worker, MDP.W_HEARTBEAT, None, None)
            self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL

    def purge_workers(self):
        """
        Look for & kill expired workers.
        Workers are oldest to most recent, so we stop at the first alive worker.
        """
        while self.idle_workers:
            w = self.idle_workers[0]
            if w.expiry < time.time():
                logging.debug("deleting expired worker: %s", w.identity)
                self.delete_worker(w, False)
                self.idle_workers.pop(0)
            else:
                break

    def worker_waiting(self, worker):
        """
        This worker is now waiting for work.
        """
        # Queue to broker and service waiting lists
        self.idle_workers.append(worker)
        worker.service.idle_workers.append(worker)
        worker.expiry = time.time() + 1e-3 * self.HEARTBEAT_EXPIRY
        self.dispatch(worker.service, None)

    def dispatch(self, service, message):
        """
        Dispatch requests to waiting workers as possible
        """
        assert service is not None
        if message is not None:  # Queue message if any
            service.client_requests.append(message)
        self.purge_workers()
        # If there is client requests and idle workers, dispatch messages
        while service.idle_workers and service.client_requests:
            message = service.client_requests.pop(0)
            worker = service.idle_workers.pop(0)
            self.idle_workers.remove(worker)
            self.send_to_worker(worker, MDP.W_REQUEST, None, message)

    def send_to_worker(self, worker, command, option, message=None):
        """
        Send message to worker.
        If message is provided, sends that message.
        """
        if message is None:
            message = []
        elif not isinstance(message, list):
            message = [message]

        # Stack routing and protocol envelopes to start of message
        # and routing envelope
        if option is not None:
            message = [option] + message
        message = [worker.address, b'', MDP.W_WORKER, command] + message
        logging.debug("[BROKER] send: %s", message)
        self.socket.send_multipart(message)

    def handler_service(self, message):
        service = message.get('$param')
        message.update({"$rep": 200 if service in self.services else 404})
        return message

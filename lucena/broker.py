# -*- coding: utf-8 -*-

import logging
import time
from binascii import hexlify

import zmq

import lucena.majordomo.MDP as MDP


class Broker(object):
    """
    Majordomo Protocol broker
    http:#rfc.zeromq.org/spec:7 and spec:8
    """

    class Service(object):
        name = None  # Service name
        requests = None  # List of client requests
        waiting = None  # List of waiting workers

        def __init__(self, name):
            self.name = name
            self.requests = []
            self.waiting = []

    class Worker(object):
        identity = None  # hex Identity of worker
        address = None  # Address to route to
        service = None  # Owning service, if known
        expiry = None  # expires at this point, unless heartbeat

        def __init__(self, identity, address, lifetime):
            self.identity = identity
            self.address = address
            self.expiry = time.time() + 1e-3 * lifetime

    # We'd normally pull these from config data
    INTERNAL_SERVICE_PREFIX = b"mmi."
    HEARTBEAT_LIVENESS = 3
    HEARTBEAT_INTERVAL = 2500  # msecs
    HEARTBEAT_EXPIRY = HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

    # ---------------------------------------------------------------------

    context = None  # Our context
    services = None  # known services
    workers = None  # known workers
    waiting = None  # idle workers

    # ---------------------------------------------------------------------

    def __init__(self):
        self.services = {}
        self.workers = {}
        self.waiting = []
        self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.ROUTER)
        self.socket.linger = 0
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    # ---------------------------------------------------------------------

    def mediate(self):
        """
        Main broker work happens here
        """
        while True:
            try:
                items = self.poller.poll(self.HEARTBEAT_INTERVAL)
            except KeyboardInterrupt:
                break  # Interrupted
            if items:
                msg = self.socket.recv_multipart()
                logging.debug("received message: %s", msg)

                sender = msg.pop(0)
                empty = msg.pop(0)
                assert empty == b''
                header = msg.pop(0)

                if MDP.C_CLIENT == header:
                    self.process_client(sender, msg)
                elif MDP.W_WORKER == header:
                    self.process_worker(sender, msg)
                else:
                    logging.error("invalid message: %s", msg)
            self.purge_workers()
            self.send_heartbeats()

    def destroy(self):
        """
        Disconnect all workers, destroy context.
        """
        while self.workers:
            self.delete_worker(self.workers.values()[0], True)
        self.context.destroy(0)

    def process_client(self, sender, message):
        """
        Process a request coming from a client.
        """
        assert len(message) >= 2  # Service name + body
        service = message.pop(0)
        # Set reply return address to client sender
        message = [sender, b''] + message
        if service.startswith(self.INTERNAL_SERVICE_PREFIX):
            self.service_internal(service, message)
        else:
            self.dispatch(self.require_service(service), message)

    def process_worker(self, sender, message):
        """
        Process message sent to us by a worker.
        """
        assert len(message) >= 1  # At least, command
        command = message.pop(0)
        worker_ready = hexlify(sender) in self.workers
        worker = self.require_worker(sender)

        if MDP.W_READY == command:
            assert len(message) >= 1  # At least, a service name
            service = message.pop(0)
            # Not first command in session or Reserved service name
            if worker_ready or service.startswith(self.INTERNAL_SERVICE_PREFIX):
                self.delete_worker(worker, True)
            else:
                # Attach worker to service and mark as idle
                worker.service = self.require_service(service)
                self.worker_waiting(worker)

        elif MDP.W_REPLY == command:
            if worker_ready:
                # Remove & save client return envelope and insert the
                # protocol header and service name, then rewrap envelope.
                client = message.pop(0)
                empty = message.pop(0)  # ?
                message = [client, b'', MDP.C_CLIENT, worker.service.name] + message
                self.socket.send_multipart(message)
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
            worker.service.waiting.remove(worker)
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
            logging.debug("registering new worker: %s", identity)
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

    def service_internal(self, service, msg):
        """
        Handle internal service according to 8/MMI specification
        """
        return_code = "501"
        if "mmi.service" == service:
            name = msg[-1]
            return_code = "200" if name in self.services else "404"
        msg[-1] = return_code

        # insert the protocol header and service name after
        # the routing envelope ([client, ''])
        msg = msg[:2] + [MDP.C_CLIENT, service] + msg[2:]
        self.socket.send_multipart(msg)

    def send_heartbeats(self):
        """
        Send heartbeats to idle workers if it's time
        """
        if time.time() > self.heartbeat_at:
            for worker in self.waiting:
                self.send_to_worker(worker, MDP.W_HEARTBEAT, None, None)
            self.heartbeat_at = time.time() + 1e-3 * self.HEARTBEAT_INTERVAL

    def purge_workers(self):
        """
        Look for & kill expired workers.
        Workers are oldest to most recent, so we stop at the first alive worker.
        """
        while self.waiting:
            w = self.waiting[0]
            if w.expiry < time.time():
                logging.debug("deleting expired worker: %s", w.identity)
                self.delete_worker(w, False)
                self.waiting.pop(0)
            else:
                break

    def worker_waiting(self, worker):
        """
        This worker is now waiting for work.
        """
        # Queue to broker and service waiting lists
        self.waiting.append(worker)
        worker.service.waiting.append(worker)
        worker.expiry = time.time() + 1e-3 * self.HEARTBEAT_EXPIRY
        self.dispatch(worker.service, None)

    def dispatch(self, service, message):
        """
        Dispatch requests to waiting workers as possible
        """
        assert (service is not None)
        if message is not None:  # Queue message if any
            service.requests.append(message)
        self.purge_workers()
        # If there is client requests and idle workers, dispatch messages
        while service.waiting and service.requests:
            message = service.requests.pop(0)
            worker = service.waiting.pop(0)
            self.waiting.remove(worker)
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
        logging.debug("sending %r to worker: %s", command, message)
        self.socket.send_multipart(message)

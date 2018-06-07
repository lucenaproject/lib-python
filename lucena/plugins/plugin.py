# -*- coding: utf-8 -*-
import json
import logging
import time
import threading

import zmq

from lucena.io2.networking import create_pipe


logger = logging.getLogger(__name__)


class Plugin(object):
    def __init__(self, zmq_context):
        self.zmq_context = zmq_context
        self.poller = zmq.Poller()
        self.socket, self.worker_socket = create_pipe(self.zmq_context)
        self.signal = threading.Event()
        self.thread = None

    def start(self):
        if self.thread:
            raise RuntimeError("Worker already started.")
        self.thread = threading.Thread(target=self._start_thread)
        self.thread.daemon = False
        self.thread.start()
        self.signal.wait()

    def stop(self):
        if self.thread is None:
            logger.warning("Worker already stopped.")
            return
        self.socket.set(zmq.SNDTIMEO, 0)
        self.socket.send_unicode("$TERM")
        self.signal.wait()

    def _run(self):
        raise NotImplementedError("Implement me in a subclass")

    def _start_thread(self):
        self.signal.set()
        self._run()
        # At this point the worker has finished
        # and we can clean up everything.
        self.socket.close()
        self.worker_socket.close()
        self.socket = None
        self.worker_socket = None
        self.thread = None
        self.signal.set()

    def send(self, *args, **kwargs):
        return self.socket.send(*args, **kwargs)

    def send_unicode(self, *args, **kwargs):
        return self.socket.send_unicode(*args, **kwargs)

    def send_multipart(self, *args, **kwargs):
        return self.socket.send_multipart(*args, **kwargs)

    def send_json(self, *args, **kwargs):
        return self.socket.send_json(*args, **kwargs)

    def recv(self, *args, **kwargs):
        return self.socket.recv(*args, **kwargs)

    def recv_unicode(self, *args, **kwargs):
        return self.socket.recv_unicode(*args, **kwargs)

    def recv_multipart(self, *args, **kwargs):
        return self.socket.recv_multipart(*args, **kwargs)

    def handle_pipe(self):
        #  Get just the commands off the pipe
        request = self.pipe.recv_multipart()
        try:
            json_request = json.loads(request[0].decode('utf-8'))
            command = json_request.get('command')
        except Exception:
            command = request.pop(0).decode('UTF-8')

        if not command:
            return -1  # Interrupted

        elif command == "CONFIGURE":
            port = json_request.get('port')
            self.configure(port)
        elif command == "PUBLISH":
            self.transmit = request.pop(0)
            if self.interval == 0:
                self.interval = INTERVAL_DFLT
            # Start broadcasting immediately
            self.ping_at = time.time()
        elif command == "SILENCE":
            self.transmit = None
        elif command == "SUBSCRIBE":
            self.filter = json_request.get('filter')
        elif command == "UNSUBSCRIBE":
            self.filter = None
        elif command == "$TERM":
            self.terminated = True
        else:
            logger.error("zbeacon: - invalid command: {0}".format(command))

    def run(self):
        # Signal actor successfully initialized
        self.pipe.signal()
        self.poller = zmq.Poller()
        self.poller.register(self.pipe, zmq.POLLIN)
        self.poller.register(self.udp_socket, zmq.POLLIN)

        while not self.terminated:
            timeout = 1
            if self.transmit:
                timeout = self.ping_at - time.time()
                if timeout < 0:
                    timeout = 0
            # Poll on API pipe and on UDP socket
            items = dict(self.poller.poll(timeout * 1000))
            if self.pipe in items and items[self.pipe] == zmq.POLLIN:
                self.handle_pipe()
            if self.udp_socket.fileno() in items \
                    and items[self.udp_socket.fileno()] == zmq.POLLIN:
                self.handle_udp()

            if self.transmit and time.time() >= self.ping_at:
                self.send_beacon()
                self.ping_at = time.time() + self.interval

# -*- coding: utf-8 -*-
import random
import threading

import zmq

from lucena.io2.socket import Socket


class Controller(object):
    def __init__(self, slave):
        self.context = zmq.Context.instance()
        self.slave = slave
        self.thread = None
        self.master_socket = None

    def start(self, **kwargs):
        assert self.thread is None
        self.master_socket, slave_socket = Controller._create_pipe(self.context)
        self.thread = threading.Thread(
            target=self.slave.controller_loop,
            daemon=False,
            args=(slave_socket,),
            kwargs=kwargs
        )
        self.thread.start()
        signal = self.master_socket.wait()
        assert signal == Socket.SIGNAL_READY

    def stop(self):
        self.master_socket.signal(Socket.SIGNAL_STOP)

    @staticmethod
    def _create_pipe(context, hwm=1000, max_retries=20):
        """
        http://zguide.zeromq.org/page:all#Unicast-Transports
        The inter-thread transport, inproc, is a connected signaling transport.
        It is much faster than tcp or ipc. This transport has a specific limitation
        compared to tcp and ipc: the server must issue a bind before
        any client issues a connect.
        """
        master_socket = Socket(context, zmq.PAIR)
        slave_socket = Socket(context, zmq.PAIR)
        master_socket.set_hwm(hwm)
        slave_socket.set_hwm(hwm)
        # close immediately on shutdown
        master_socket.setsockopt(zmq.LINGER, 0)
        slave_socket.setsockopt(zmq.LINGER, 0)
        for i in range(max_retries):
            try:
                endpoint = "inproc://pipe-{:04x}-{:04x}".format(
                    random.randint(0, 0x10000),
                    random.randint(0, 0x10000)
                )
                master_socket.bind(endpoint)
                slave_socket.connect(endpoint)
                return master_socket, slave_socket
            except zmq.ZMQError:
                if i == max_retries - 1:
                    raise
                continue


if __name__ == '__main__':
    ctx = zmq.Context()
    Controller._create_pipe(ctx, 1000)

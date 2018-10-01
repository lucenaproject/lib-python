# -*- coding: utf-8 -*-
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
        self.master_socket, slave_socket = Socket.socket_pair(self.context)
        self.thread = threading.Thread(
            target=self.slave.controller_loop,
            daemon=False,
            args=(slave_socket,),
            kwargs=kwargs
        )
        self.thread.start()
        signal = self.master_socket.wait()
        assert signal == Socket.SIGNAL_READY

    def stop(self, timeout=None):
        self.master_socket.signal(Socket.SIGNAL_STOP)
        self.thread.join(timeout=timeout)
        self.master_socket.close()
        self.thread = None

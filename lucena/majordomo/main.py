# -*- coding: utf-8 -*-
import logging
import time
from multiprocessing import Process

from lucena.broker import Broker
from lucena.worker2 import Worker
from lucena.client2 import Client


def worker_main():
    worker = Worker("tcp://localhost:5555")
    reply = None
    while True:
        request = worker.recv(reply)
        if request is None:
            break  # Worker was interrupted
        reply = request  # Echo is complex… :-)


def broker_main():
    """create and start new broker"""
    broker = Broker()
    broker.bind("tcp://*:5555")
    broker.mediate()


def client_main():
    client = Client("tcp://localhost:5555")
    requests = 10
    for i in range(requests):
        try:
            client.send(b"mmi.service", b"$echo")
        except KeyboardInterrupt:
            print("send interrupted, aborting")
            return
    time.sleep(1)
    count = 0
    while count < requests:
        try:
            reply = client.recv()
            logging.debug('[CLIENT] recv: %s', reply)
        except KeyboardInterrupt:
            break
        else:
            # also break on failure to reply:
            if reply is None:
                break
        count += 1
    logging.info("%i requests/replies processed" % count)


def main():
    p_broker = Process(target=broker_main)
    p_broker.start()

    p_worker = Process(target=worker_main)
    p_worker.start()

    p_client = Process(target=client_main)
    p_client.start()

    p_worker.join()
    p_broker.join()
    p_client.join()


if __name__ == '__main__':
    main()

import zmq
import logging

from time import sleep
from random import uniform


logging.basicConfig(level=logging.INFO)


class Worker:
    def __init__(self, stock, value, port):
        self.stock = stock
        self.value = value
        self.broker = port
        context = zmq.Context.instance()
        self.socket = context.socket(zmq.DEALER)
        logging.info(f'Connecting {stock} socket to the broker...')
        self.socket.connect(f'tcp://localhost:{port}')

    def run(self):
        logging.info(f'Starting {self.stock} stocks worker...')
        while True:
            rate = b'{%b} %d' % (self.stock.encode('utf-8'), self.value)
            self.socket.send(rate)

            self.value = (self.value + uniform(-(self.value * 0.2), self.value * 0.2))
            sleep(uniform(0.3, 0.6))

    def close(self):
        logging.info(f'Closing {self.stock} socket...')
        self.socket.close()


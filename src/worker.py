import sys
import zmq
import logging

from time import time, sleep
from random import uniform


class Worker:
    def __init__(self, stock, value, port=4321, max_reconnects=5):
        self.stock = stock
        self.value = value
        self.broker = port
        
        self.socket = None
        self.poller = None

        self.heartbeat_interval = 2
        self.heartbeat_liveness = 3
        self.heartbeat_at = time() + self.heartbeat_interval
        self.max_reconnects = 5

        self.set_logger()
        self.start()

    def run(self):
        self.logger.info(f'Starting worker...')
        liveness = self.heartbeat_liveness
        reconnects = 0

        while True:
            socks = dict(self.poller.poll(self.heartbeat_interval*1000))
            # Received a message from the broker
            if socks.get(self.socket) == zmq.POLLIN:
                frames = self.socket.recv_multipart()
                # Broker heartbeat message
                if len(frames) == 1 and frames[0] == b'HEARTBEAT':
                    reconnects = 0
                    liveness = self.heartbeat_liveness
            # Regular workflow
            else:
                liveness -= 1
                # Heartbeat liveness has depleted
                if liveness == 0:
                    self.logger.warning(f'Heartbeat failure, can\'t reach broker!')
                    interval = uniform(1.0, 5.0)
                    self.logger.warning(f'Reconnecting in {interval:02.2f} seconds...')
                    sleep(interval)
                    # Attempt reconnection
                    self.reconnect()
                    reconnects += 1
                    liveness = self.heartbeat_liveness
                    if reconnects > self.max_reconnects:
                        self.logger.warning(f'Connection to the broker has been lost!')
                        self.stop()
                        break
                # Send current stock rate and calculate a new value
                else:
                    self.logger.info('Sending current rates to the broker...')
                    rate = b'{%b} %.2f' % (self.stock.encode('utf-8'), self.value)
                    self.socket.send(rate)
                    self.value = (self.value + uniform(-(self.value * 0.2), self.value * 0.2))
            # Send a heartbeat to the broker
            if time() > self.heartbeat_at:
                heartbeat_at = time() + self.heartbeat_interval
                self.logger.info(f'Sending a heartbeat to the broker...')
                self.socket.send(b'HEARTBEAT')
            sleep(uniform(0.5, 0.9))

    def start(self):
        context = zmq.Context.instance()
        self.socket = context.socket(zmq.DEALER)
        self.socket.setsockopt(zmq.IDENTITY, bytes(self.stock.encode('utf-8')))
        self.logger.info(f'Connecting to the broker...')
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        self.socket.connect(f'tcp://localhost:{self.broker}')

    def stop(self):
        self.logger.info(f'Closing socket...')
        self.socket.close()
	
    def reconnect(self):
        self.logger.info(f'Reconnecting to broker...')
        self.poller.unregister(self.socket)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.stop()
        self.start()

    def set_logger(self):
        self.logger = logging.getLogger(self.stock)
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())
        handler = self.logger.handlers[0]
        fmt = logging.Formatter(f'[%(name)s] %(levelname)s: %(message)s')
        handler.setFormatter(fmt)


def main(argv):
    worker = Worker(argv[0], float(argv[1]))
    worker.run()

if __name__ == '__main__':
    main(sys.argv[1:])

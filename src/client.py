import zmq
import logging

from random import randint

class Client:
    def __init__(self, port=4322, retries=3, timeout=2000):
        self.id = b'C%d' % randint(0,10000)
        self.retries = retries
        self.timeout = timeout
        self.broker = port
        
        self.poller = None
        self.socket = None
        self.queues = []

        self.set_logger()
        self.start()

    def set_logger(self):
        self.logger = logging.getLogger(self.id.decode('utf-8'))
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(logging.StreamHandler())
        handler = self.logger.handlers[0]
        fmt = logging.Formatter(f'[%(name)s] %(levelname)s: %(message)s')
        handler.setFormatter(fmt)

    def start(self):
        context = zmq.Context.instance()
        self.socket = context.socket(zmq.DEALER)
        self.socket.connect(f'tcp://localhost:{self.broker}')
        self.socket.setsockopt(zmq.IDENTITY, self.id)
        self.logger.info(f'Connecting to the broker...')
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)

    def stop(self):
        self.logger.info(f'Closing socket...')
        self.socket.close()
	
    def register(self, queue):
        request = [b'REGISTER', bytes(queue.encode('utf-8'))]
        self.logger.info(f'Sending register request for queue [{queue}]...')
        self.socket.send_multipart(request)
        response = self.socket.recv_multipart()
        self.logger.info(f'Got response from broker: {response[0]}')
        if b'OK' in response:
            self.queues.append(queue)

    def consume(self):
        while True:
            if len(self.queues) == 0:
                self.logger.info('Nothing to consume! Exiting...')
                self.stop()
                break
            socks = dict(self.poller.poll(self.timeout))
            if socks.get(self.socket) == zmq.POLLIN:
               msg = self.socket.recv()
               self.logger.info(f'New message: {msg}')


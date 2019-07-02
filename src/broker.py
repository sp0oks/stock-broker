import zmq
import logging

from time import time
from threading import Thread

logging.basicConfig(level=logging.INFO)


class Broker:
    def __init__(self, wport=4321, cport=4322):
        self.workers = {}
        self.clients = []
        self.worker_port = wport
        self.client_port = cport
        
        self.poller = None
        self.backend = None
        self.frontend = None

        self.heartbeat_interval = 2
        self.heartbeat_at = time() + self.heartbeat_interval
        
        logger = logging.getLogger().handlers[0]
        fmt = logging.Formatter('[Broker] %(levelname)s: %(message)s')
        logger.setFormatter(fmt)

        self.start()

    def start(self):
        context = zmq.Context.instance()
        # Router for workers
        logging.info(f'Listening to workers in port {self.worker_port}.')
        self.backend = context.socket(zmq.ROUTER)
        self.backend.bind(f'tcp://*:{self.worker_port}')

        # Router for clients
        logging.info(f'Listening to workers in port {self.client_port}.')
        self.frontend = context.socket(zmq.ROUTER)
        self.frontend.bind(f'tcp://*:{self.client_port}')

        # Register poller to both routers
        self.poller = zmq.Poller()
        self.poller.register(self.backend, zmq.POLLIN)
        self.poller.register(self.frontend, zmq.POLLIN)

    def relay(self, frames):
        # Gets the frames received from a worker and relays the content to its clients
        address = frames[0]
        content = frames[1]
        
        logging.info(f'Relaying message from {address} to clients...')
        for client in self.workers[address]:
            message = [client, content]
            self.frontend.send_multipart(message)

    def run(self):
        while True:
            socks = dict(self.poller.poll(self.heartbeat_interval * 1000))
            # Received a message from a worker 
            if socks.get(self.backend) == zmq.POLLIN:
                frames = self.backend.recv_multipart()
                if not frames:
                    logging.info('Received an empty message!')
                else:
                    address = frames[0]
                    # Register new worker connecting to the broker
                    if address not in self.workers:
                        self.workers[address] = []
                    # Relay new message to all clients registered to this service
                    if len(self.workers[address]) > 0 and b'HEARTBEAT' not in frames:
                        self.relay(frames)
                    if b'HEARTBEAT' not in frames:
                        logging.info(f'Received {frames[1]} from {address}')
            # Received a message from a client
            if socks.get(self.frontend) == zmq.POLLIN:
                frames = self.frontend.recv_multipart()
                address = frames[0]
                # Register new client connecting to the broker
                if address not in self.clients:
                    self.clients.append(address)
                logging.info(f'New request from client: {frames[1:]}')
                # If a request to register to a queue shows up, try to register the client to the queue it asks for
                if b'REGISTER' in frames:
                    if self.workers.get(frames[2]) is not None:
                        self.workers[frames[2]].append(address)
                        response = [address, b'OK']
                    else:
                        response = [address, b'BAD REQUEST']
                    self.frontend.send_multipart(response)
            # Send heartbeat to idle workers
            if time() > self.heartbeat_at:
                for worker in self.workers.keys():
                    msg = [worker, b'HEARTBEAT']
                    self.backend.send_multipart(msg)
                self.heartbeat_at = time() + self.heartbeat_interval 

def main():
    broker = Broker()
    try:
        broker.run()
    except Exception as e:
        logging.error(e)
        logging.info('Broker has been stopped.')

if __name__ == "__main__":
    main()

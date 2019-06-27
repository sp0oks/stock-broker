import zmq
import logging

from time import time


logging.basicConfig(level=logging.INFO)


class Broker:
    def __init__(self, wport=4321, cport=4322):
        context = zmq.Context.instance()
        self.workers = []
        self.worker_port = wport
        self.client_port = cport
        self.poller = zmq.Poller()

        self.heartbeat_interval = 5
        self.heartbeat_liveness = 3
        self.heartbeat_at = time() + self.heartbeat_interval
        
        logger = logging.getLogger().handlers[0]
        fmt = logging.Formatter('[Broker] %(levelname)s: %(message)s')
        logger.setFormatter(fmt)

        # Router for workers
        logging.info(f'Listening to workers in port {wport}.')
        self.backend = context.socket(zmq.ROUTER)
        self.backend.bind(f'tcp://*:{wport}')

        # Register poller to workers
        self.poller.register(self.backend, zmq.POLLIN)

    def run(self):
        while True:
            socks = dict(self.poller.poll(self.heartbeat_interval * 1000))
            # Received a message from a worker 
            if socks.get(self.backend) == zmq.POLLIN:
                frames = self.backend.recv_multipart()
                if not frames:
                    logging.info(f'Received an empty message!')
                else:
                    address = frames[0]
                    if address not in self.workers:
                        self.workers.append(address)
                    logging.info(f'Received {frames[1]} from {address}')
                    
            # Send heartbeat to idle workers
            if time() > self.heartbeat_at:
                for worker in self.workers:
                    msg = [worker, b'HEARTBEAT']
                    self.backend.send_multipart(msg)
                self.heartbeat_at = time() + self.heartbeat_interval 

def main():
    broker = Broker()
    try:
        broker.run()
    except:
        logging.info('Broker has been stopped.')

if __name__ == "__main__":
    main()

import zmq

from threading import Thread
from worker import Worker

class Broker:
    def __init__(self, workers, wport=4321, cport=4322):
        context = zmq.Context.instance()
        self.worker_port = wport
        self.client_port = cport
        
        # Router for workers
        self.backend = context.socket(zmq.ROUTER)
        self.backend.bind(f'tcp://*:{wport}')


from worker import Worker
from client import Client
from threading import Thread

def main():
    for i in range(5):
        print(f'Connecting worker #{i}...')
        w = Worker(str(i), 0.5)
        Thread(target=w.run).start()
    
    #c = Client()
    #c.register('COCA')
    #c.consume()
    
if __name__ == '__main__':
    main()

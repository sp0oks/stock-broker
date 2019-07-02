from client import Client
from threading import Thread
from random import randint

def main():
    for i in range(20):
        print(f'Connecting client #{i}...')
        c = Client()
        c.register(str(randint(0,19)))
        Thread(target=c.consume).start()
       
if __name__ == '__main__':
    main()

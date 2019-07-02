from worker import Worker
from threading import Thread
from random import uniform

def main():
    for i in range(20):
        print(f'Connecting worker #{i}...')
        w = Worker(str(i), uniform(0.5, 3.0))
        Thread(target=w.run).start()
       
if __name__ == '__main__':
    main()

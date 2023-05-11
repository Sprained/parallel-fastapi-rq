from redis import Redis
from rq import Worker, Queue, Connection
import time

redis_conn = Redis(host="localhost", port=6379)

def initializer():
    with Connection(redis_conn):
        while True:
            queues = [queue for queue in Queue.all() if queue.name.endswith('_db')]
            worker = Worker(queues=queues)
            worker.work(burst=True)
            time.sleep(3600)


if __name__ == '__main__':
    initializer()
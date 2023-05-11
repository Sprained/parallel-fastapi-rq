from fastapi import FastAPI
from redis import Redis
from rq import Worker, Queue, Connection
from ratelimit import sleep_and_retry, limits

app = FastAPI()

redis_conn = Redis(host="localhost", port=6379)

queues_db = {
    "client1": Queue(name="client1_db", connection=redis_conn),
    "client2": Queue(name="client2_db", connection=redis_conn),
}

queues_lead = {
    "client1": Queue(name="client1_lead", connection=redis_conn),
    "client2": Queue(name="client2_lead", connection=redis_conn),
}


@limits(calls=1, period=60)  # Limite de 5 chamadas por minuto
@sleep_and_retry
async def read_queue(data):
    print(data)

@app.get("/")
async def initial():
    if not "client7" in queues_db:
        queues_db["client7"] = Queue(name="client7_db", connection=redis_conn)

    if not "client3" in queues_lead:
        queues_lead["client3"] = Queue(name="client3_lead", connection=redis_conn)

    queues_db["client7"].enqueue(read_queue, args=["oi2"])

    return "Hellow"

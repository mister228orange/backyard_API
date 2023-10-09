import asyncio

from fastapi import FastAPI
import redis
import json
from celery import Celery
import requests

app = FastAPI()

# Set up Redis connection
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Configure Celery
celery = Celery(
    'celery_app',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0',
)

# Celery task to fetch and store data in Redis


async def get_pair_data(pair):
    response = await requests.get(f'https://www.bestchange.ru/{pair}.html')
    data = response.html
    return data

@celery.task
async def fetch_and_store_data():
    tasks = [get_pair_data(pos) for pos in cfg.positions]
    result = await asyncio.gather(*tasks)
    write_tasks = [
        lambda x: redis_client.set()
        for pos in result
    ]
    await redis_client.set('xchange_data', json.dumps(data))

# Endpoint to retrieve data from Redis
@app.get("/{xchange_pair}/{xchange_service}")
async def get_xchange_data(xchange_pair, xchange_service: str):
    # Retrieve data from Redis
    data = redis_client.get(f'https://www.bestchange.ru/{xchange_pair}.html')
    if data:
        return json.loads(data)
    else:
        return {"message": "Data not found"}

if __name__ == "__main__":
    # Start Celery worker
    celery.worker_main(['worker', '--loglevel=info'])

    # Start the FastAPI application
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

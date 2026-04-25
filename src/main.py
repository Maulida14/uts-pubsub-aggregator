from fastapi import FastAPI
from typing import List
from src.models import Event
from src.queue import event_queue
from src.consumer import consumer, processed_events
from src.dedup_store import init_db, get_stat
import asyncio
import time

app = FastAPI()
start_time = time.time()

@app.on_event("startup")
async def startup():
    init_db()
    asyncio.create_task(consumer())

@app.post("/publish")
async def publish(events: List[Event]):
    for e in events:
        await event_queue.put(e)
    return {"status": "accepted", "count": len(events)}

@app.get("/events")
def get_events(topic: str = None):
    if topic:
        return processed_events.get(topic, [])
    
    all_events = []
    for t in processed_events.values():
        all_events.extend(t)
    return all_events

@app.get("/stats")
def get_stats():
    return {
        "received": get_stat("received"),
        "unique_processed": get_stat("unique_processed"),
        "duplicate_dropped": get_stat("duplicate_dropped"),
        "topics": list(processed_events.keys()),
        "uptime": time.time() - start_time
    }
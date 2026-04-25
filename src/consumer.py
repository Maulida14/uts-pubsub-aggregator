from src.queue import event_queue
from src.dedup_store import is_duplicate, store_event, update_stat

processed_events = {}

async def consumer():
    while True:
        event = await event_queue.get()

        update_stat("received")

        if is_duplicate(event.topic, event.event_id):
            update_stat("duplicate_dropped")
            continue

        store_event(event.topic, event.event_id)
        update_stat("unique_processed")

        if event.topic not in processed_events:
            processed_events[event.topic] = []

        processed_events[event.topic].append(event.dict())
import requests
import random
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

URL = "http://localhost:8080/publish"

TOTAL_EVENTS = 5000
DUPLICATE_RATIO = 0.2 

generated_ids = []

def generate_event(event_id):
    return {
        "topic": "stress.test",
        "event_id": event_id,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "stress-script",
        "payload": {
            "value": random.randint(1, 1000)
        }
    }

def send_batch(batch, index):
    try:
        res = requests.post(URL, json=batch)
        print(f"[Batch {index}] Status: {res.status_code} | Events: {len(batch)}")
        return res.status_code
    except Exception as e:
        print(f"[Batch {index}] Error:", e)

def main():
    events = []

    # generate event unik
    for _ in range(TOTAL_EVENTS):
        eid = str(uuid.uuid4())
        generated_ids.append(eid)
        events.append(generate_event(eid))

    # tambah duplikat
    duplicate_count = int(TOTAL_EVENTS * DUPLICATE_RATIO)
    for _ in range(duplicate_count):
        dup_id = random.choice(generated_ids)
        events.append(generate_event(dup_id))

    print(f"Total events (termasuk duplikat): {len(events)}")

    # batching
    BATCH_SIZE = 100
    batches = [events[i:i+BATCH_SIZE] for i in range(0, len(events), BATCH_SIZE)]

    # kirim paralel + progress
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(send_batch, batch, idx+1)
            for idx, batch in enumerate(batches)
        ]

        for future in as_completed(futures):
            pass 

    print("Selesai kirim semua event")

if __name__ == "__main__":
    main()
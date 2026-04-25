# UTS Sistem Terdistribusi - Pub-Sub Log Aggregator

## Cara Menjalankan

Build:
docker build -t uts-aggregator .

Run:
docker run -p 8080:8080 uts-aggregator

## Endpoint

POST /publish
GET /events
GET /stats

## Fitur
- Pub-Sub model (in-memory queue)
- Idempotent consumer
- Deduplication (SQLite persistent)
- At-least-once simulation

## Test
pytest
# uts-pubsub-aggregator
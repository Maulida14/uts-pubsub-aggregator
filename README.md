# Pub-Sub Log Aggregator dengan Idempotent Consumer dan Deduplication

## 📌 Deskripsi

Proyek ini merupakan implementasi layanan **Pub-Sub Log Aggregator** berbasis Python menggunakan FastAPI. Sistem menerima event dari publisher, memprosesnya melalui consumer, serta menerapkan mekanisme **idempotency** dan **deduplication** untuk memastikan event yang sama tidak diproses berulang kali.

Seluruh sistem berjalan dalam container Docker dengan penyimpanan data lokal menggunakan SQLite untuk menjaga persistensi.

---

## 🎯 Fitur Utama

* Publish event melalui endpoint HTTP
* Idempotent consumer (tidak memproses ulang event yang sama)
* Deduplication berbasis `(topic, event_id)`
* At-least-once delivery simulation
* Penyimpanan persisten (SQLite)
* Statistik sistem (`/stats`)
* Stress test hingga ≥ 5000 event (dengan duplikasi)
* Dockerized (reproducible environment)

---

## 🏗️ Arsitektur Sistem

Publisher → API (/publish) → Queue (async) → Consumer → SQLite (dedup + stats)

Penjelasan:

* **Publisher** mengirim event
* **API** menerima dan memasukkan ke queue
* **Consumer** memproses event secara asynchronous
* **Dedup Store (SQLite)** memastikan idempotency
* **Stats** mencatat performa sistem

---

## 📦 Struktur Project

```
UTS_aggregator/
│
├── src/
│   ├── main.py
│   ├── consumer.py
│   ├── queue.py
│   ├── models.py
│   ├── dedup_store.py
│   └── config.py
│
├── scripts/
│   └── stress_test.py
│
├── tests/
│   ├── test_api.py
│   ├── test_dedup.py
│   └── test_persistence.py
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
└── report.md
```

---

## ⚙️ Cara Menjalankan

### 🔹 Build Docker Image

```
docker build -t uts-aggregator .
```

### 🔹 Run Container (dengan volume)

```
docker run -p 8080:8080 -v aggregator_data:/app/data --name aggregator uts-aggregator
```

---

## 🔗 Endpoint API

### 1. Publish Event

```
POST /publish
```

Contoh:

```
[
  {
    "topic": "test",
    "event_id": "1",
    "timestamp": "2024-01-01T00:00:00",
    "source": "demo",
    "payload": {"msg": "halo"}
  }
]
```

---

### 2. Get Events

```
GET /events?topic=test
```

---

### 3. Get Statistics

```
GET /stats
```

Contoh output:

```
{
  "received": 6000,
  "unique_processed": 5000,
  "duplicate_dropped": 1000,
  "uptime": 120.5
}
```

---

## 🧪 Stress Test

Jalankan:

```
python scripts/stress_test.py
```

Spesifikasi:

* Total event: 5000+
* Duplikasi: ≥20%
* Batch processing + concurrency

---

## 🐳 Docker Compose 

```
docker-compose up --build
```

## 🔁 Demonstrasi Idempotency

1. Kirim event dengan `event_id` tertentu
2. Kirim ulang event yang sama
3. Sistem hanya memproses sekali
4. Event duplikat akan dihitung pada `duplicate_dropped`

---

## 📊 Evaluasi Sistem

* Throughput: jumlah event yang diproses
* Latency: waktu respon API
* Duplicate rate: jumlah event duplikat
* Konsistensi: dijaga melalui idempotency dan deduplication

---

## 🎥 Video Demo

(Link YouTube di sini)

---

## 📚 Referensi

Tanenbaum, A. S., & van Steen, M. (2017). *Distributed systems: Principles and paradigms* (Edisi ke-2). Pearson.

Narkhede, N., Shapira, G., & Palino, T. (2021). *Kafka: The definitive guide* (Edisi ke-2). O’Reilly Media.

Kleppmann, M. (2017). *Designing data-intensive applications: The big ideas behind reliable, scalable, and maintainable systems*. O’Reilly Media.

---

## 📝 Catatan

* Data disimpan menggunakan SQLite pada Docker volume (`/app/data`)
* Sistem mendukung restart tanpa kehilangan data deduplication
* Statistik disimpan secara persisten

---

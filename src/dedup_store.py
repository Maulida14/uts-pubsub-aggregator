import os
import sqlite3
from src.config import DB_PATH

def init_db():
    db_dir = os.path.dirname(DB_PATH)

    os.makedirs(db_dir, exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            topic TEXT,
            event_id TEXT,
            PRIMARY KEY (topic, event_id)
        )
        """)

        conn.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            key TEXT PRIMARY KEY,
            value INTEGER
        )
        """)

def is_duplicate(topic, event_id):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "SELECT 1 FROM processed_events WHERE topic=? AND event_id=?",
            (topic, event_id)
        )
        return cur.fetchone() is not None

def store_event(topic, event_id):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_events VALUES (?, ?)",
            (topic, event_id)
        )

def update_stat(key, inc=1):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        INSERT INTO stats (key, value)
        VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value = value + ?
        """, (key, inc, inc))

def get_stat(key):
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute("SELECT value FROM stats WHERE key=?", (key,))
        row = cur.fetchone()
        return row[0] if row else 0
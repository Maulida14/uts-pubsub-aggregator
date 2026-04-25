from src.dedup_store import init_db, is_duplicate, store_event

def test_dedup():
    init_db()
    topic = "test"
    eid = "123"

    assert not is_duplicate(topic, eid)
    store_event(topic, eid)
    assert is_duplicate(topic, eid)
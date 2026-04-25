from src.dedup_store import update_stat, get_stat

def test_stats():
    update_stat("received")
    assert get_stat("received") >= 1
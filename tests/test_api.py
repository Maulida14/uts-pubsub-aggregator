from fastapi.testclient import TestClient
from src.main import app

client = TestClient(app)

def test_publish():
    res = client.post("/publish", json=[{
        "topic": "test",
        "event_id": "1",
        "timestamp": "2024",
        "source": "x",
        "payload": {}
    }])
    assert res.status_code == 200
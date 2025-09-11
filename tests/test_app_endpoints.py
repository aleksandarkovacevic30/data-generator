# tests/test_app_endpoints.py
import json
import types
import time

import app as backend  # the FastAPI module

def test_status_and_config_roundtrip(client):
    r = client.get("/status")
    assert r.status_code == 200
    st = r.json()
    assert "domain" in st and "running" in st

    # Switch domain to beverage
    r = client.post("/config", json={"domain": "beverage", "interval_seconds": 2, "batch_size": 2})
    assert r.status_code == 200 and r.json()["ok"] is True

    r = client.get("/config")
    cfg = r.json()
    assert cfg["domain"] == "beverage"
    assert cfg["interval_seconds"] == 2
    assert cfg["batch_size"] == 2

def test_generate_now_and_recent_flow(client):
    # Ensure we have some rows
    r = client.post("/generate-now", json={})
    assert r.status_code == 200
    assert r.json()["emitted"] >= 1

    r = client.get("/recent?n=5")
    assert r.status_code == 200
    items = r.json()
    assert isinstance(items, list) and len(items) >= 1
    assert "record" in items[0] and "timestamp" in items[0]

def test_download_sanitized_removes_internal_keys(client):
    # Generate a small batch
    client.post("/config", json={"batch_size": 3})
    client.post("/generate-now", json={})

    # Download sanitized
    r = client.get("/download?limit=5&sanitized=true")
    assert r.status_code == 200
    data = json.loads(r.text)
    assert isinstance(data, list)
    for rec in data:
        assert all(not k.startswith("_") for k in rec.keys())

def test_switch_domains_and_generate_company_then_beverage(client):
    # Switch to company and generate
    client.post("/config", json={"domain": "company", "batch_size": 2})
    client.post("/generate-now", json={})
    r1 = client.get("/recent?n=2").json()
    assert any(x["record"]["domain"] == "company" for x in r1)

    # Switch to beverage and generate
    client.post("/config", json={"domain": "beverage", "batch_size": 2})
    client.post("/generate-now", json={})
    r2 = client.get("/recent?n=4").json()
    assert any(x["record"]["domain"] == "beverage" for x in r2)

def test_sink_failure_sets_curl_on_status(client, monkeypatch):
    """
    We don't call the network. Instead, stub the sink.send() to simulate a failure that
    populates the send_state fields (last_send_ok, last_send_error, last_send_curl).
    """
    def fake_send(batch, state):
        state.last_send_ok = False
        state.last_send_error = "HTTP 403 Forbidden: token invalid"
        state.last_send_curl = "curl -X POST 'https://example.test/ingest' -H 'Authorization: Bearer ***' -H 'Content-Type: application/json' -d '[]'"

    # Patch the actual sink used by the app
    monkeypatch.setattr(backend.producer.sink, "send", fake_send, raising=True)

    # Trigger a send via generate-now
    r = client.post("/generate-now", json={})
    assert r.status_code == 200

    st = client.get("/status").json()
    assert st["last_send_ok"] is False
    assert "Forbidden" in (st["last_send_error"] or "")
    assert "curl -X POST" in (st["last_send_curl"] or "")

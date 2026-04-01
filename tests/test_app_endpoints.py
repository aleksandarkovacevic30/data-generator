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
    Simulate a sink failure without touching the real network.
    Patches app.sink_send so the /generate-now endpoint sees a failed send,
    then verifies /status reflects last_send_ok=False with a cURL hint.
    """
    # Configure active domain first (before patching)
    client.post("/config", json={"domain": "company"})

    def _fake_sink_send(batch, cfg, *, domain=None, force=False):
        backend.patch_state({
            "last_send_ok":     False,
            "last_send_error":  "simulated connection refused",
            "last_send_status": 0,
            "last_send_curl":   "curl -i -X POST 'http://fake.test/ingest' -H 'Authorization: Bearer tok'",
            "last_row":         batch[-1] if batch else None,
        })
        return {"ok": False, "error": "simulated connection refused"}

    monkeypatch.setattr(backend, "sink_send", _fake_sink_send)

    r = client.post("/generate-now", json={})
    assert r.status_code == 200

    st = client.get("/status").json()
    assert st["last_send_ok"] is False
    assert "refused" in (st["last_send_error"] or "")
    assert "curl" in (st["last_send_curl"] or "")

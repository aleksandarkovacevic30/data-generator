#!/usr/bin/env python3
"""
runner.py — background generation loop (separate process from app.py).

Reads config.json on every tick, generates a batch for the active domain,
and optionally POSTs it to the configured sink. Writes a heartbeat so that
app.py can report runner_alive in /status.

Domains are auto-discovered via the same domains/ package used by app.py.
"""

import os
import json
import time
from typing import Any, Dict, List

import httpx
import domains as domain_registry

BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH    = os.path.join(BASE_DIR, "config.json")
STATE_PATH     = os.path.join(BASE_DIR, "state.json")
HEARTBEAT_PATH = "/tmp/mdg_runner.heartbeat"
SEND_LOG_PATH  = os.path.join(BASE_DIR, "send_log.jsonl")

# Discover domains once at startup
_DOMAINS: Dict[str, Any] = domain_registry.discover_all()
print(f"[runner] Discovered domains: {sorted(_DOMAINS.keys())}")

DEFAULT_CONFIG: Dict[str, Any] = {
    "running": False, "domain": "company", "interval_seconds": 2, "batch_size": 10,
    "sink": {"enabled": False, "url": "", "bearer": "", "mode": "batch",
             "timeout": 10.0, "max_retries": 1, "trust_env": False},
    "scenarios": {},
}


# -------------------------- Helpers --------------------------
def _read_json(path: str, default: Dict[str, Any]) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return dict(default)
    except Exception as e:
        print(f"[runner] WARN: failed to read {path}: {e}")
        return dict(default)


def _write_json(path: str, data: Dict[str, Any]) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def load_config() -> Dict[str, Any]:
    cfg = _read_json(CONFIG_PATH, DEFAULT_CONFIG)
    for k, v in DEFAULT_CONFIG.items():
        cfg.setdefault(k, v)
    cfg.setdefault("sink", {}).setdefault("enabled", False)
    return cfg


def save_state(patch: Dict[str, Any]) -> None:
    st = _read_json(STATE_PATH, {})
    st.update(patch)
    st["last_updated_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _write_json(STATE_PATH, st)


def heartbeat() -> None:
    try:
        with open(HEARTBEAT_PATH, "w") as f:
            f.write(str(time.time()))
    except Exception:
        pass


def append_send_log(entry: dict) -> None:
    try:
        entry = dict(entry)
        entry["ts"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        if "resp_body" in entry and entry["resp_body"]:
            entry["resp_body"] = str(entry["resp_body"])[:4000]
        with open(SEND_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        print("[runner] append_send_log failed:", e)


# -------------------------- Generation --------------------------
def generate_batch(domain: str, batch_size: int, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    mod = _DOMAINS.get(domain)
    if mod is None:
        raise RuntimeError(f"Domain '{domain}' not available. Known: {sorted(_DOMAINS.keys())}")
    return mod.make_generator(cfg).generate_batch(int(batch_size))


def sanitize_for_send(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {k: v for k, v in rec.items() if not k.startswith("_")}


def build_curl(url: str, bearer: str, payload: Any) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return (
        "curl -i -X POST " + repr(url)
        + " -H 'Authorization: Bearer " + bearer + "'"
        + " -H 'Content-Type: application/json'"
        + " --data-binary " + repr(data)
    )


# -------------------------- Sink --------------------------
def send_to_sink(records: List[Dict[str, Any]], sink_cfg: Dict[str, Any]) -> Dict[str, Any]:
    url         = sink_cfg.get("url") or ""
    bearer      = sink_cfg.get("bearer") or ""
    mode        = sink_cfg.get("mode", "batch")
    timeout     = float(sink_cfg.get("timeout", 10.0))
    max_retries = int(sink_cfg.get("max_retries", 1))
    trust_env   = bool(sink_cfg.get("trust_env", False))

    if not url or not bearer:
        return {"ok": False, "error": "Missing sink url/bearer"}

    payload_batch = [sanitize_for_send(r) for r in records]
    headers = {"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"}

    start    = time.time()
    last_exc = None
    with httpx.Client(
        timeout=httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0),
        follow_redirects=True, trust_env=trust_env,
    ) as c:
        for _ in range(1 + max_retries):
            try:
                if mode == "batch":
                    resp = c.post(url, headers=headers, json=payload_batch)
                else:
                    resp = None
                    for rec in payload_batch:
                        resp = c.post(url, headers=headers, json=[rec])
                        if resp.status_code >= 400:
                            break

                if resp is not None and resp.status_code < 400:
                    append_send_log({
                        "source": "runner", "domain": sink_cfg.get("domain") or "unknown",
                        "count": len(payload_batch), "mode": mode, "ok": True,
                        "status": resp.status_code,
                        "duration_s": round(time.time() - start, 3),
                        "url": url, "resp_body": resp.text,
                    })
                    return {"ok": True, "status": resp.status_code, "body": resp.text}

                status = getattr(resp, "status_code", 0) if resp else 0
                curl   = build_curl(url, bearer, payload_batch if mode == "batch" else [payload_batch[0]])
                append_send_log({
                    "source": "runner", "domain": sink_cfg.get("domain") or "unknown",
                    "count": len(payload_batch), "mode": mode, "ok": False,
                    "status": status, "duration_s": round(time.time() - start, 3),
                    "url": url, "resp_body": getattr(resp, "text", ""), "error": f"HTTP {status}",
                })
                return {"ok": False, "status": status, "error": f"HTTP {status}",
                        "body": getattr(resp, "text", ""), "curl": curl}
            except Exception as e:
                last_exc = e
                time.sleep(0.25)

    curl = build_curl(url, bearer, payload_batch if mode == "batch" else [payload_batch[0]])
    append_send_log({
        "source": "runner", "domain": sink_cfg.get("domain") or "unknown",
        "count": len(payload_batch), "mode": mode, "ok": False, "status": 0,
        "duration_s": round(time.time() - start, 3),
        "url": url, "error": str(last_exc or "unknown error"),
    })
    return {"ok": False, "error": str(last_exc or "unknown error"), "curl": curl}


# -------------------------- Main loop --------------------------
def main():
    print("[runner] starting")
    while True:
        try:
            cfg      = load_config()
            interval = max(0.2, float(cfg.get("interval_seconds", 2.0)))

            if cfg.get("running", False):
                domain     = (cfg.get("domain") or "company").strip().lower()
                batch_size = int(cfg.get("batch_size", 10))

                try:
                    batch = generate_batch(domain, batch_size, cfg)
                except Exception as e:
                    save_state({"last_send_ok": False, "last_send_error": f"generation failed: {e}"})
                    heartbeat()
                    time.sleep(interval)
                    continue

                sink_cfg = cfg.get("sink") or {}
                if sink_cfg.get("enabled", False):
                    result = send_to_sink(batch, sink_cfg)
                    if result.get("ok"):
                        save_state({
                            "last_send_ok": True, "last_send_error": None,
                            "last_send_status": result.get("status"),
                            "last_send_curl": None,
                            "last_row": batch[-1] if batch else None,
                            "last_send_body": result.get("body"),
                        })
                    else:
                        save_state({
                            "last_send_ok": False, "last_send_error": result.get("error"),
                            "last_send_status": result.get("status"),
                            "last_send_curl": result.get("curl"),
                            "last_send_body": result.get("body"),
                            "last_row": batch[-1] if batch else None,
                        })
                else:
                    save_state({
                        "last_send_ok": None, "last_send_error": None,
                        "last_send_status": None, "last_send_curl": None,
                        "last_row": batch[-1] if batch else None,
                    })

            heartbeat()
        except Exception as e:
            print("[runner] tick failure:", e)

        time.sleep(max(0.2, float(load_config().get("interval_seconds", 2.0))))


if __name__ == "__main__":
    main()

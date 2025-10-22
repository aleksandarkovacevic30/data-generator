# app.py — Master Data Generator (web backend only; runner.py does the loop)
# - Per-domain sinks (company, beverage) + legacy global sink fallback
# - Domain-specific config endpoints: /config/domain/{domain} (GET/POST)
# - Robust /generate-now (accepts GET with query or POST with/without JSON)
# - Status exposes runner heartbeat + last send info (incl. curl to reproduce)
# - UI routes: /ui, /ui/company, /ui/beverage (served from local folders)

import os
import json
import time
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from collections import deque

from fastapi import FastAPI, Request, Body, Query, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse, Response, RedirectResponse
import httpx

# -------------------------- Paths / Globals --------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(BASE_DIR, "config.json")
STATE_PATH  = os.path.join(BASE_DIR, "state.json")
HEARTBEAT_PATH = "/tmp/mdg_runner.heartbeat"

# UI directories (keep next to app.py)
UI_MAIN     = os.path.join(BASE_DIR, "ui_main")
UI_COMPANY  = os.path.join(BASE_DIR, "ui_company")
UI_BEVERAGE = os.path.join(BASE_DIR, "ui_beverage")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("mdg.app")

app = FastAPI(title="Master Data Generator (Web)", version="2.0.0")

# Wide-open CORS (tighten if you need to)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# Keep a small recent buffer in-memory for /recent and /download
RECENT_MAX = 500
_recent = deque(maxlen=RECENT_MAX)


SEND_LOG_PATH = os.path.join(BASE_DIR, "send_log.jsonl")

def append_send_log(entry: dict):
    try:
        entry = dict(entry)
        entry["ts"] = datetime.now(timezone.utc).isoformat()
        # keep it small
        if "resp_body" in entry and entry["resp_body"]:
            entry["resp_body"] = str(entry["resp_body"])[:4000]
        with open(SEND_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception as e:
        log.warning("append_send_log failed: %s", e)


# -------------------------- Defaults / Utilities --------------------------
DEFAULT_SINK = {
    "enabled": False,
    "url": "",
    "bearer": "",
    "mode": "batch",
    "timeout": 10.0,
    "max_retries": 1,
    "trust_env": False,
    "verify_tls": False     # <— NEW: set False to skip certificate verification
}

DEFAULT_CONFIG: Dict[str, Any] = {
    "running": False,
    "domain": "company",           # active domain: 'company' | 'beverage'
    "interval_seconds": 2,
    "batch_size": 10,
    "scenarios": {},               # shared scenario knobs (per generator interprets its keys)
    "company_params": {},          # domain-specific knobs
    "beverage_params": {},
    # Legacy global sink (kept for backward compatibility)
    "sink": dict(DEFAULT_SINK),
    # NEW: per-domain sinks
    "sinks": {
        "company":  dict(DEFAULT_SINK),
        "beverage": dict(DEFAULT_SINK),
    },
}

DEFAULT_CONFIG.update({
    "sources": {"company": "synthetic", "beverage": "synthetic"},  # synthetic | gleif
    "gleif": {
        "csv_path": "",        # absolute path to a GLEIF CSV / CSV.GZ / ZIP (Golden Copy)
        "guess_websites": False
    }
})


def _safe_read_json(path: str, default: Any):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return default
    except Exception as e:
        log.warning("Failed to read %s: %s", path, e)
        return default

def _atomic_write(path: str, data: Any):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def _deep_merge(dst: dict, src: dict):
    for k, v in (src or {}).items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)
        else:
            dst[k] = v

def get_config() -> Dict[str, Any]:
    cfg = _safe_read_json(CONFIG_PATH, dict(DEFAULT_CONFIG))

    # Backfill top-level defaults
    for k, v in DEFAULT_CONFIG.items():
        if k not in cfg:
            cfg[k] = v

    # Backfill legacy sink keys
    sink = cfg.get("sink") or {}
    for k, v in DEFAULT_SINK.items():
        sink.setdefault(k, v)
    cfg["sink"] = sink

    # Backfill per-domain sinks, ensure complete keys
    sinks = cfg.get("sinks") or {}
    if "company" not in sinks:
        sinks["company"] = dict(DEFAULT_SINK)
    if "beverage" not in sinks:
        sinks["beverage"] = dict(DEFAULT_SINK)
    for d in list(sinks.keys()):
        for k, v in DEFAULT_SINK.items():
            sinks[d].setdefault(k, v)
    cfg["sinks"] = sinks

    return cfg

def set_config(patch: Dict[str, Any]) -> Dict[str, Any]:
    cfg = get_config()
    _deep_merge(cfg, patch or {})
    _atomic_write(CONFIG_PATH, cfg)
    return cfg

def patch_state(patch: Dict[str, Any]):
    st = _safe_read_json(STATE_PATH, {})
    st.update(patch)
    st["last_updated_utc"] = datetime.now(timezone.utc).isoformat()
    _atomic_write(STATE_PATH, st)
    return st

def heartbeat_age() -> Optional[float]:
    try:
        with open(HEARTBEAT_PATH, "r", encoding="utf-8") as f:
            ts = float(f.read().strip())
        return max(0.0, time.time() - ts)
    except Exception:
        return None

def list_domains() -> List[str]:
    out = []
    try:
        from generator.domains.company import CompanyGenerator  # noqa: F401
        out.append("company")
    except Exception:
        pass
    try:
        from generator.domains.beverage import BeverageGenerator  # noqa: F401
        out.append("beverage")
    except Exception:
        pass
    return sorted(set(out))

def sanitize_for_send(record: Dict[str, Any]) -> Dict[str, Any]:
    """Strip internal keys; guarantee _issues/_source never leave."""
    return {k: v for k, v in record.items() if not k.startswith("_")}

def build_curl(url: str, bearer: str, payload: Any) -> str:
    data = json.dumps(payload, ensure_ascii=False)
    return (
        "curl -i -X POST "
        + repr(url)
        + " -H 'Authorization: Bearer " + bearer + "'"
        + " -H 'Content-Type: application/json'"
        + " --data-binary " + repr(data)
    )

# -------------------------- Generators (dynamic) --------------------------
def _make_generator(domain: str, cfg: Dict[str, Any]):
    d = (domain or "company").strip().lower()
    if d == "company":
        src = ((cfg.get("sources") or {}).get("company") or "synthetic").lower()
        if src == "gleif":
            from generator.domains.company_gleif import CompanyFromGLEIFGenerator
            csv_path = ((cfg.get("gleif") or {}).get("csv_path") or "").strip()
            guess = bool((cfg.get("gleif") or {}).get("guess_websites", False))
            g = CompanyFromGLEIFGenerator(csv_path=csv_path, guess_websites=guess)
            # If you still want to apply your existing "issue scenarios" on top,
            # do it *after* generation (in generate_batch wrapper) or let your sink/agent handle fixes.
            return g
        else:
            from generator.domains.company import CompanyGenerator
            g = CompanyGenerator()
            scen = cfg.get("scenarios") or {}
            if scen:
                try:
                   g.set_scenarios(scen)
                except Exception:
                    pass
            params = cfg.get("company_params") or {}
            for k, v in params.items():
                if hasattr(g, k):
                    setattr(g, k, v)
            return g

    if d == "beverage":
        from generator.domains.beverage import BeverageGenerator
        g = BeverageGenerator()
        scen = cfg.get("scenarios") or {}
        if scen:
            try:
                g.set_scenarios(scen)
            except Exception as e:
                log.warning("Beverage.set_scenarios failed: %s", e)
        params = cfg.get("beverage_params") or {}
        for k, v in params.items():
            if hasattr(g, k):
                setattr(g, k, v)
        return g

    raise ValueError(f"Unsupported domain: {domain}")

def generate_batch(batch_size: int, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    g = _make_generator(cfg.get("domain", "company"), cfg)
    n = max(1, int(batch_size))
    return g.generate_batch(n)

# -------------------------- Sink sending --------------------------
def sink_send(
    batch: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    *,
    domain: Optional[str] = None,
    force: bool = False
) -> Dict[str, Any]:
    d = (domain or cfg.get("domain") or "company").strip().lower()
    # Prefer per-domain sink; fallback to legacy 'sink'
    sink = (cfg.get("sinks") or {}).get(d) or (cfg.get("sink") or {})
    enabled = bool(sink.get("enabled")) or bool(force)

    if not enabled:
        # still update last_row for UI
        st = patch_state({
            "last_send_ok": None,
            "last_send_error": None,
            "last_send_status": None,
            "last_send_curl": None,
            "last_row": batch[-1] if batch else None,
        })
        return {"ok": True, "note": "sink disabled", "state": st}

    url = sink.get("url") or ""
    bearer = sink.get("bearer") or ""
    mode = sink.get("mode", "batch")
    timeout = float(sink.get("timeout", 10.0))
    max_retries = int(sink.get("max_retries", 1))
    trust_env = bool(sink.get("trust_env", False))

    if not url or not bearer:
        st = patch_state({
            "last_send_ok": False,
            "last_send_error": "Missing sink url/bearer",
            "last_send_status": 0,
            "last_send_curl": build_curl(url or "<missing>", bearer or "<missing>", []),
            "last_row": batch[-1] if batch else None,
        })
        return {"ok": False, "error": "Missing sink url/bearer", "state": st}

    payload_batch = [sanitize_for_send(r) for r in batch]
    headers = {"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"}

    start = time.time()
    last_exc = None
    TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)
    verify_tls = bool(sink.get("verify_tls", True))
    verify: Any
    if not verify_tls:
        verify = False
    else:
        verify = True    
    with httpx.Client(timeout=TIMEOUT, follow_redirects=True, trust_env=trust_env, verify=verify) as c:
        for _ in range(1 + max_retries):
            try:
                if mode == "batch":
                    resp = c.post(url, headers=headers, json=payload_batch)
                else:
                    resp = None
                    for rec in payload_batch:
                        resp = c.post(url, headers=headers, json=[rec])  # send one by one
                        if resp.status_code >= 400:
                            break

                if resp is not None and resp.status_code < 400:
                    st = patch_state({
                        "last_send_ok": True,
                        "last_send_error": None,
                        "last_send_status": resp.status_code,
                        "last_send_curl": None,
                        "last_row": batch[-1] if batch else None,
                        "last_send_body": (resp.text or "")[:4000],
                    })
                    append_send_log({
                        "source": "app", "domain": d, "count": len(payload_batch), "mode": mode,
                        "ok": True, "status": resp.status_code, "duration_s": round(time.time() - start, 3),
                        "url": url, "resp_body": resp.text
                    })

                    return {"ok": True, "status": resp.status_code, "state": st}
                else:
                    status = getattr(resp, "status_code", 0) if resp is not None else 0
                    body = getattr(resp, "text", "")
                    curl = build_curl(url, bearer, payload_batch if mode == "batch" else [payload_batch[0]])
                    st = patch_state({
                        "last_send_ok": False,
                        "last_send_error": f"HTTP {status}",
                        "last_send_status": status,
                        "last_send_curl": curl,
                        "last_row": batch[-1] if batch else None,
                        "last_send_body": body,
                    })
                    append_send_log({
                        "source": "app", "domain": d, "count": len(payload_batch), "mode": mode,
                        "ok": False, "status": status, "duration_s": round(time.time() - start, 3),
                        "url": url, "resp_body": body, "error": f"HTTP {status}"
                    })

                    return {"ok": False, "status": status, "error": f"HTTP {status}", "state": st}
            except Exception as e:
                last_exc = e
                time.sleep(0.25)

    curl = build_curl(url, bearer, payload_batch if mode == "batch" else [payload_batch[0]])
    st = patch_state({
        "last_send_ok": False,
        "last_send_error": str(last_exc or "unknown error"),
        "last_send_status": 0,
        "last_send_curl": curl,
        "last_row": batch[-1] if batch else None,
    })
    append_send_log({
        "source": "app", "domain": d, "count": len(payload_batch), "mode": mode,
        "ok": False, "status": 0, "duration_s": round(time.time() - start, 3),
        "url": url, "error": str(last_exc or "unknown error")
    })

    return {"ok": False, "error": str(last_exc or "unknown error"), "state": st}

# -------------------------- API: Status / Config / Control --------------------------
@app.get("/status")
def status():
    cfg = get_config()
    st  = _safe_read_json(STATE_PATH, {})
    hb  = heartbeat_age()
    active_domain = cfg.get("domain", "company")
    active_sink = (cfg.get("sinks") or {}).get(active_domain) or cfg.get("sink") or {}
    return JSONResponse({
        "ok": True,
        "running": bool(cfg.get("running", False)),
        "domain": active_domain,
        "interval_seconds": cfg.get("interval_seconds", 2),
        "batch_size": cfg.get("batch_size", 10),
        "runner_alive": (hb is not None and hb < 20.0),
        "runner_heartbeat_age_seconds": hb,
        "domains_available": list_domains(),
        # active sink snapshot
        "active_sink_enabled": active_sink.get("enabled", False),
        "active_sink_url": active_sink.get("url", ""),
        # last send info
        "last_send_ok": st.get("last_send_ok"),
        "last_send_error": st.get("last_send_error"),
        "last_send_status": st.get("last_send_status"),
        "last_send_curl": st.get("last_send_curl"),
        "last_send_body": st.get("last_send_body"),
        "last_row": st.get("last_row"),
    })

@app.get("/config")
def read_config():
    return JSONResponse(get_config())

@app.post("/config")
def write_config(payload: Dict[str, Any] = Body(...)):
    cfg = set_config(payload or {})
    return JSONResponse({"ok": True, "config": cfg})

@app.post("/start")
def start():
    cfg = set_config({"running": True})
    return JSONResponse({"ok": True, "config": cfg, "note": "Runner will pick this up on next tick."})

@app.post("/stop")
def stop():
    cfg = set_config({"running": False})
    return JSONResponse({"ok": True, "config": cfg, "note": "Runner will stop after current tick."})


@app.post("/generate-download")
@app.get("/generate-download")
async def generate_download(request: Request):
    """Generate a fresh batch and return it as a downloadable JSON file.
       No sink sending. Records are sanitized (_issues/_source stripped)."""
    cfg = get_config()
    batch_size = int(cfg.get("batch_size", 10))

    # Accept n/count from query or JSON body
    try:
        qp_n = request.query_params.get("n") or request.query_params.get("count")
        if qp_n is not None:
            batch_size = max(1, int(qp_n))
    except Exception:
        pass
    try:
        body = await request.json()
        if isinstance(body, dict):
            if "n" in body:
                batch_size = max(1, int(body["n"]))
            if "count" in body:
                batch_size = max(1, int(body["count"]))
    except Exception:
        pass  # no JSON is fine

    # Generate without sending
    try:
        batch = generate_batch(batch_size, cfg)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"generation failed: {e!r}"}, status_code=500)

    # Sanitize and return as attachment
    rows = [sanitize_for_send(r) for r in batch]
    data = json.dumps(rows, ensure_ascii=False, indent=2)
    domain = (cfg.get("domain") or "company").lower()
    fname = f"mdg_{domain}_{int(time.time())}_N{len(rows)}.json"
    return Response(
        content=data,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'}
    )


# ---------- Domain-scoped configuration ----------
def _validate_domain_or_404(domain: str):
    d = (domain or "").strip().lower()
    if d not in list_domains():
        return None
    return d

@app.get("/config/domain/{domain}")
def read_domain_config(domain: str = Path(..., description="e.g. company, beverage")):
    d = _validate_domain_or_404(domain)
    if not d:
        return JSONResponse({"detail": f"Unknown domain '{domain}'"}, status_code=404)
    cfg = get_config()
    return {
        "domain": d,
        "running": cfg.get("running", False),
        "interval_seconds": cfg.get("interval_seconds", 2),
        "batch_size": cfg.get("batch_size", 10),
        "scenarios": cfg.get("scenarios", {}),
        "params": cfg.get(f"{d}_params", {}),
        "sink": (cfg.get("sinks") or {}).get(d) or cfg.get("sink") or {},
    }

@app.post("/config/domain/{domain}")
def write_domain_config(
    payload: Dict[str, Any] = Body(...),
    domain: str = Path(..., description="e.g. company, beverage")
):
    d = _validate_domain_or_404(domain)
    if not d:
        return JSONResponse({"detail": f"Unknown domain '{domain}'"}, status_code=404)
    patch: Dict[str, Any] = {}

    # Domain params
    if "params" in payload:
        patch[f"{d}_params"] = payload["params"]

    # Scenarios are shared (each generator only reads the keys it understands)
    if "scenarios" in payload:
        patch["scenarios"] = payload["scenarios"]

    # Per-domain sink
    if "sink" in payload:
        patch.setdefault("sinks", {})
        patch["sinks"][d] = payload["sink"]

    # Common timing knobs
    if "interval_seconds" in payload:
        patch["interval_seconds"] = payload["interval_seconds"]
    if "batch_size" in payload:
        patch["batch_size"] = payload["batch_size"]

    # Running flag + active domain selection if asked
    if "running" in payload:
        patch["running"] = payload["running"]
    if payload.get("domain") == d:
        patch["domain"] = d

    cfg = set_config(patch)
    return {"ok": True, "config": cfg}

# -------------------------- Generate Now / Recent / Download --------------------------
@app.post("/generate-now")
@app.get("/generate-now")
async def generate_now(request: Request):
    cfg = get_config()
    batch_size = int(cfg.get("batch_size", 10))
    force_send = False

    # Query params (GET or POST)
    try:
        qp_n = request.query_params.get("n") or request.query_params.get("count")
        qp_force = request.query_params.get("force_send")
        if qp_n is not None:
            batch_size = max(1, int(qp_n))
        if qp_force is not None:
            force_send = qp_force.lower() in ("1", "true", "yes", "on")
    except Exception:
        pass

    # Optional JSON body
    try:
        body = await request.json()
        if isinstance(body, dict):
            if "n" in body:
                batch_size = max(1, int(body.get("n")))
            if "count" in body:
                batch_size = max(1, int(body.get("count")))
            if "force_send" in body:
                force_send = bool(body.get("force_send"))
    except Exception:
        pass  # no/invalid JSON is fine

    # Generate
    try:
        batch = generate_batch(batch_size, cfg)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"generation failed: {e!r}"}, status_code=500)

    # Keep recent for UI/exports
    ts = datetime.now(timezone.utc).isoformat()
    for rec in batch:
        _recent.append({"timestamp": ts, "record": rec})

    # Optional immediate send (respects per-domain sink)
    result = sink_send(batch, cfg, domain=cfg.get("domain", "company"), force=bool(force_send))
    return JSONResponse({"ok": True, "emitted": len(batch), "send_result": result})



@app.get("/send-log")
def get_send_log(limit: int = Query(50, ge=1, le=1000)):
    try:
        if not os.path.isfile(SEND_LOG_PATH):
            return []
        with open(SEND_LOG_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()[-limit:]
        return [json.loads(x) for x in lines]
    except Exception as e:
        return JSONResponse({"detail": f"read failed: {e}"}, status_code=500)

@app.post("/send-log/clear")
def clear_send_log():
    try:
        if os.path.isfile(SEND_LOG_PATH):
            os.remove(SEND_LOG_PATH)
        return {"ok": True}
    except Exception as e:
        return JSONResponse({"detail": f"clear failed: {e}"}, status_code=500)



@app.get("/recent")
def recent(n: int = Query(50, ge=1, le=RECENT_MAX)):
    out = list(_recent)[-n:]
    return JSONResponse(out)

@app.get("/download")
def download(limit: int = Query(100, ge=1, le=RECENT_MAX), sanitized: bool = Query(True)):
    out = list(_recent)[-limit:]
    rows = [r["record"] for r in out]
    if sanitized:
        rows = [sanitize_for_send(r) for r in rows]
    data = json.dumps(rows, ensure_ascii=False, indent=2)
    return Response(
        content=data,
        media_type="application/json",
        headers={"Content-Disposition": f'attachment; filename="mdg_export_{int(time.time())}.json"'}
    )

# -------------------------- Egress Check --------------------------
@app.get("/egress-check")
def egress_check(url: str):
    try:
        with httpx.Client(timeout=10.0, follow_redirects=True, trust_env=False) as c:
            r = c.head(url)
        return {"ok": True, "status": r.status_code, "final_url": str(r.url)}
    except Exception as e:
        return {"ok": False, "error": str(e)}

# -------------------------- UI routes --------------------------
def _safe_file(path: str, label: str):
    if os.path.isfile(path):
        return FileResponse(path)
    return JSONResponse({"detail": f"{label} not found", "path": path}, status_code=404)

@app.get("/ui", include_in_schema=False)
@app.get("/ui/", include_in_schema=False)
def ui_main():
    return _safe_file(os.path.join(UI_MAIN, "index.html"), "ui_main/index.html")

@app.get("/ui/company", include_in_schema=False)
@app.get("/ui/company/", include_in_schema=False)
def ui_company():
    return _safe_file(os.path.join(UI_COMPANY, "index.html"), "ui_company/index.html")

@app.get("/ui/beverage", include_in_schema=False)
@app.get("/ui/beverage/", include_in_schema=False)
def ui_beverage():
    return _safe_file(os.path.join(UI_BEVERAGE, "index.html"), "ui_beverage/index.html")

@app.get("/", include_in_schema=False)
def root():
    idx = os.path.join(UI_MAIN, "index.html")
    if os.path.isfile(idx):
        return RedirectResponse(url="/ui")
    return JSONResponse({"ok": True, "message": "MDG backend is running. Add a UI under /ui."})

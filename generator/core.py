# generator/core.py
# Core abstractions + plumbing for the synthetic data generator.
# - Domain plugin interface (DomainGenerator)
# - Domain registry for hot-swapping domains
# - Metrics aggregator
# - Recent buffer for cross-record heuristics (e.g., making duplicates)
# - CluedIn sink (HTTP sender) with cURL reproduction for ANY failure
# - Sanitization utilities (drop keys starting with "_")
#
# This module has ZERO FastAPI/UI code. The web app imports and uses it.

from __future__ import annotations

import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx

logger = logging.getLogger("mdg.core")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)


# ----------------------------- Sanitization -----------------------------

def sanitize_outbound_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove any private/internal keys from the record.
    Policy: drop keys starting with "_".
    This guarantees we never ship _issues/_source/etc to sinks.
    """
    return {k: v for k, v in rec.items() if not str(k).startswith("_")}


def sanitize_outbound_batch(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [sanitize_outbound_record(r) for r in records]


# ----------------------------- Recent Buffer -----------------------------

class RecentBuffer:
    """
    Thread-safe rolling buffer of recent generated records.

    Store items as dicts like:
      { "timestamp": <iso>, "record": <dict> }
    """
    def __init__(self, maxlen: int = 2000):
        self._buf: deque = deque(maxlen=maxlen)
        self._lock = threading.Lock()

    def append(self, item: Dict[str, Any]) -> None:
        with self._lock:
            self._buf.append(item)

    def last(self, n: int) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._buf)[-n:]

    def items(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._buf)


# A global buffer shared by the app and (optionally) domain plugins
RECENT = RecentBuffer(maxlen=2000)


# ----------------------------- Metrics -----------------------------

class Metrics:
    """
    Thread-safe counters for quick charts/health.
    """
    def __init__(self, window_size: int = 300):
        self.total_records = 0
        self.total_with_issues = 0
        self.by_issue: Dict[str, int] = {}
        self.window: List[int] = []
        self.window_size = window_size
        self._lock = threading.Lock()

    def bump(self, rec: Dict[str, Any]) -> None:
        has_issues = 1 if (rec.get("_issues") or []) else 0
        with self._lock:
            self.total_records += 1
            self.total_with_issues += has_issues
            for issue in rec.get("_issues", []):
                self.by_issue[issue] = self.by_issue.get(issue, 0) + 1
            self.window.append(has_issues)
            if len(self.window) > self.window_size:
                self.window = self.window[-self.window_size:]

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            tot = max(1, self.total_records)
            pct_recent = (sum(self.window) / len(self.window)) if self.window else 0.0
            top = sorted(self.by_issue.items(), key=lambda kv: kv[1], reverse=True)[:12]
            return {
                "total_records": self.total_records,
                "total_with_issues": self.total_with_issues,
                "pct_bad_overall": round(self.total_with_issues / tot, 4),
                "pct_bad_recent": round(pct_recent, 4),
                "top_issues_labels": [k for k, _ in top],
                "top_issues_values": [v for _, v in top],
            }


# ----------------------------- Domain Abstraction -----------------------------

class DomainGenerator(ABC):
    """
    Base class for a domain plugin.

    A plugin MUST:
      - Provide a stable `name` (e.g., "company", "beverage")
      - Implement `default_scenarios()` returning {scenario_key: probability}
      - Implement `generate_batch(batch_size)` returning a list of records (dicts)
        Each record may include internal keys (like `_issues`), which will be
        stripped by the sink before sending.
    """

    name: str

    @abstractmethod
    def default_scenarios(self) -> Dict[str, float]:
        ...

    @abstractmethod
    def set_scenarios(self, scenarios: Dict[str, float]) -> None:
        """
        Update the scenario probabilities used by this generator.
        The plugin should clamp/validate values to [0, 1].
        """
        ...

    @abstractmethod
    def generate_batch(self, batch_size: int) -> List[Dict[str, Any]]:
        ...

    # Optional: plugins can use RECENT buffer for duplicate/correlation scenarios
    def recent(self) -> List[Dict[str, Any]]:
        return RECENT.items()


class DomainRegistry:
    """
    Registry for domain plugins. The app can swap active domain at runtime.
    """
    def __init__(self):
        self._plugins: Dict[str, DomainGenerator] = {}
        self._lock = threading.Lock()

    def register(self, plugin: DomainGenerator) -> None:
        name = str(plugin.name).lower().strip()
        if not name:
            raise ValueError("Plugin must have a non-empty name")
        with self._lock:
            self._plugins[name] = plugin

    def get(self, name: str) -> DomainGenerator:
        key = str(name).lower().strip()
        with self._lock:
            if key not in self._plugins:
                raise KeyError(f"Domain '{name}' not registered")
            return self._plugins[key]

    def names(self) -> List[str]:
        with self._lock:
            return sorted(self._plugins.keys())


# ----------------------------- Config & State -----------------------------

@dataclass
class SinkConfig:
    send_enabled: bool = False
    cluedin_endpoint: Optional[str] = None
    bearer_token: Optional[str] = None
    send_mode: str = "record"  # "record" | "batch"
    timeout_seconds: float = 6.0
    max_retries: int = 3

    def valid(self) -> bool:
        return bool(self.send_enabled and self.cluedin_endpoint and self.bearer_token)


@dataclass
class GeneratorConfig:
    domain: str = "company"
    interval_seconds: int = 120
    batch_size: int = 1
    # Scenario maps are stored per-domain in the app; plugin instances hold their own effective scenarios.
    sink: SinkConfig = field(default_factory=SinkConfig)


@dataclass
class SendState:
    last_send_ok: Optional[bool] = None
    last_send_error: Optional[str] = None
    last_send_curl: Optional[str] = None
    last_sent_record: Optional[Dict[str, Any]] = None


# ----------------------------- cURL helpers -----------------------------

def _shell_escape_single_quoted(s: str) -> str:
    # POSIX-safe: inside single quotes, escape ' as '\''
    return s.replace("'", "'\\''")


def build_debug_curl(url: str, bearer_token: Optional[str], payload: Any) -> str:
    """
    Build a redacted cURL command for reproducing a POST with JSON.
    Token is redacted intentionally.
    """
    try:
        body = json.dumps(payload, ensure_ascii=False)
    except Exception:
        body = str(payload)
    if len(body) > 4096:
        body = body[:4096] + " …(truncated)…"
    body = _shell_escape_single_quoted(body)
    safe_token = "Bearer *****" if (bearer_token and bearer_token.strip()) else "Bearer <REDACTED>"
    return (
        f"curl -i -X POST '{url}' \\\n"
        f"  -H 'Authorization: {safe_token}' \\\n"
        f"  -H 'Content-Type: application/json' \\\n"
        f"  --data-binary '{body}'"
    )


# ----------------------------- CluedIn Sink -----------------------------

class CluedInSink:
    """
    Synchronous sender, friendly to WSGI hosting.
    - Sends either per-record or batch JSON
    - ALWAYS prepares a cURL reproduction on any failure path
    - Strips private keys from payloads before sending
    """

    def __init__(self, cfg: SinkConfig):
        self.cfg = cfg
        self._lock = threading.Lock()

    def update(self, cfg: SinkConfig) -> None:
        with self._lock:
            self.cfg = cfg

    def send(self, records: List[Dict[str, Any]], state: SendState) -> bool:
        cfg = self.cfg
        if not cfg.valid():
            # Not an error; just "disabled"
            return False

        headers = {
            "Authorization": f"Bearer {cfg.bearer_token}",
            "Content-Type": "application/json",
        }
        sanitized = sanitize_outbound_batch(records)

        def fmt_http_err(exc: httpx.HTTPStatusError) -> str:
            try:
                resp = exc.response
                text = resp.text or ""
                if len(text) > 2048:
                    text = text[:2048] + " …(truncated)…"
                return (
                    f"HTTP {resp.status_code} {resp.reason_phrase} at "
                    f"{resp.request.method} {resp.request.url}\nResponse: {text}"
                )
            except Exception:
                return str(exc)

        def curl_for(payload: Any) -> Optional[str]:
            try:
                return build_debug_curl(cfg.cluedin_endpoint or "", cfg.bearer_token, payload)
            except Exception:
                return None

        last_payload: Any = None

        try:
            with httpx.Client(timeout=cfg.timeout_seconds) as client:
                if cfg.send_mode == "batch":
                    payload = {"records": sanitized}
                    last_payload = payload
                    try:
                        resp = client.post(cfg.cluedin_endpoint or "", headers=headers, json=payload)
                        resp.raise_for_status()
                    except httpx.HTTPStatusError as he:
                        curl = curl_for(last_payload)
                        msg = fmt_http_err(he)
                        logger.error("Error sending batch to CluedIn.\n%s\ncURL:\n%s", msg, curl or "<n/a>")
                        state.last_send_ok = False
                        state.last_send_error = msg
                        state.last_send_curl = curl
                        state.last_sent_record = sanitized[-1] if sanitized else None
                        return False
                else:
                    for rec in sanitized:
                        last_payload = rec
                        try:
                            resp = client.post(cfg.cluedin_endpoint or "", headers=headers, json=rec)
                            resp.raise_for_status()
                        except httpx.HTTPStatusError as he:
                            curl = curl_for(last_payload)
                            msg = fmt_http_err(he)
                            logger.error("Error sending record to CluedIn.\n%s\ncURL:\n%s", msg, curl or "<n/a>")
                            state.last_send_ok = False
                            state.last_send_error = msg
                            state.last_send_curl = curl
                            state.last_sent_record = rec
                            return False

            state.last_send_ok = True
            state.last_send_error = None
            state.last_send_curl = None
            state.last_sent_record = sanitized[-1] if sanitized else None
            return True

        except httpx.RequestError as re:
            curl = curl_for(last_payload if last_payload is not None else ({"records": sanitized} if cfg.send_mode == "batch" else (sanitized[0] if sanitized else {})))
            state.last_send_ok = False
            state.last_send_error = f"Request error: {re!s}"
            state.last_send_curl = curl
            state.last_sent_record = sanitized[-1] if sanitized else None
            logger.error("Request error to CluedIn: %s\ncURL:\n%s", re, curl or "<n/a>")
            return False
        except Exception as e:
            curl = curl_for(last_payload if last_payload is not None else ({"records": sanitized} if cfg.send_mode == "batch" else (sanitized[0] if sanitized else {})))
            state.last_send_ok = False
            state.last_send_error = f"Unhandled error: {e!s}"
            state.last_send_curl = curl
            state.last_sent_record = sanitized[-1] if sanitized else None
            logger.exception("Unhandled error while sending to CluedIn. cURL:\n%s", curl or "<n/a>")
            return False


# ----------------------------- Simple Scheduler -----------------------------

class Producer:
    """
    A small controllable producer loop:
      - calls domain.generate_batch()
      - bumps metrics
      - appends to RECENT
      - optionally sends to sink
    The web app should own a Producer instance and control start/stop.
    """

    def __init__(self, registry: DomainRegistry, metrics: Metrics, sink: Optional[CluedInSink] = None):
        self.registry = registry
        self.metrics = metrics
        self.sink = sink
        self._active_domain = "company"
        self._interval = 120
        self._batch_size = 1
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._state = SendState()
        self._lock = threading.Lock()

    # ---------- getters useful for /status ----------
    @property
    def running(self) -> bool:
        with self._lock:
            return self._running

    @property
    def interval_seconds(self) -> int:
        with self._lock:
            return self._interval

    @property
    def batch_size(self) -> int:
        with self._lock:
            return self._batch_size

    @property
    def domain(self) -> str:
        with self._lock:
            return self._active_domain

    @property
    def send_state(self) -> SendState:
        return self._state

    # ---------- control ----------
    def configure(self, *, domain: Optional[str] = None, interval_seconds: Optional[int] = None, batch_size: Optional[int] = None) -> None:
        with self._lock:
            if domain:
                # Validate exists
                self.registry.get(domain)  # raises if unknown
                self._active_domain = domain
            if interval_seconds is not None and interval_seconds >= 1:
                self._interval = int(interval_seconds)
            if batch_size is not None and 1 <= batch_size <= 100:
                self._batch_size = int(batch_size)

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._running = True
            self._thread = threading.Thread(target=self._loop, name="mdg-producer", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._running = False

    # ---------- core loop ----------
    def _loop(self) -> None:
        while True:
            with self._lock:
                if not self._running:
                    time.sleep(0.25)
                    continue
                domain = self._active_domain
                interval = self._interval
                batch_size = self._batch_size

            plugin = self.registry.get(domain)
            batch = plugin.generate_batch(batch_size)

            ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            for rec in batch:
                self.metrics.bump(rec)
                RECENT.append({"timestamp": ts, "record": rec})

            if self.sink:
                try:
                    self.sink.send(batch, self._state)
                except Exception:
                    # state already populated inside sink
                    pass

            time.sleep(max(1, interval))

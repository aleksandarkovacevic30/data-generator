# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies (activate venv first)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run the web server (dev mode, from project root)
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000

# Run the background loop (separate terminal)
python runner.py

# Run tests
pytest tests/

# Run a single test file
pytest tests/test_app_endpoints.py

# Run a single test
pytest tests/test_company_generator.py::test_company_clean_alignment_no_mismatch
```

## Architecture

This is a **Master Data Generator** — a FastAPI service that generates synthetic (or GLEIF-sourced) data records with configurable data-quality issues, and optionally streams them to an HTTP sink (CluedIn).

### Two-process design

The system runs as **two separate processes** communicating through `config.json` and `state.json` files on disk:

- **`app.py`** — FastAPI web backend. Handles HTTP endpoints, config, UI routing, and on-demand generation via `/generate-now`.
- **`runner.py`** — Standalone polling loop. Reads `config.json` every tick, generates a batch when `running=true`, and POSTs to the sink. Writes a heartbeat to `/tmp/mdg_runner.heartbeat` so `app.py` can report `runner_alive` in `/status`.

The UI's **Start/Stop** buttons set `config.running` via the API; the runner picks it up on its next tick.

### Adding a new domain

Create `domains/<name>/` with three files:

```
domains/
  <name>/
    __init__.py   ← must export DISPLAY_NAME, DESCRIPTION, UI_FILE, make_generator(cfg)
    generator.py  ← the generator class (must have generate_batch(n) -> list[dict])
    ui.html       ← self-contained single-page UI
```

Both `app.py` and `runner.py` call `domains.discover_all()` at startup and pick up any new package that exports `make_generator`. No other files need changing.

**Minimal `__init__.py` example:**
```python
from pathlib import Path
from typing import Any, Dict

DISPLAY_NAME = "MyDomain"
DESCRIPTION  = "One-line description shown on the landing page."
UI_FILE      = Path(__file__).parent / "ui.html"

def make_generator(cfg: Dict[str, Any]):
    from .generator import MyGenerator
    return MyGenerator(cfg)
```

**Minimal generator:**
```python
class MyGenerator:
    name = "mydomain"

    def __init__(self, cfg):
        ...

    def default_scenarios(self) -> dict:
        return {}

    def set_scenarios(self, overrides: dict) -> None:
        pass

    def generate_batch(self, n: int) -> list:
        return [{"domain": "mydomain", "id": i} for i in range(n)]
```

### Domain conventions

- `generate_batch(n)` — returns `list[dict]`. Internal metadata keys (e.g. `_issues`, `_source`, `_ts`) are stripped by `sanitize_for_send()` before records leave the system.
- `default_scenarios()` / `set_scenarios(overrides)` — expose probability knobs (0.0–1.0) that control data-quality issue injection. The UI sliders and `/config/domain/<name>` POST endpoint drive these.
- Records track which scenarios were applied in `_issues: list[str]`.

### Key files

| File | Purpose |
|---|---|
| `app.py` | FastAPI app: auto-discovers domains, serves UIs at `/ui/<name>`, handles config/generation/sink |
| `runner.py` | Standalone polling loop (separate process) |
| `domains/__init__.py` | `discover_all()` — scans `domains/*/` for valid domain packages |
| `domains/company/` | Synthetic company generator + GLEIF CSV variant |
| `domains/beverage/` | Beverage catalog generator |
| `domains/customer/` | Derived from company (renames fields) |
| `domains/vendor/` | Derived from company (renames fields) |
| `domains/promptgen/` | LLM-driven generator (requires `OPENAI_API_KEY`) |
| `generator/core.py` | Base classes: `DomainGenerator`, `DomainRegistry`, `CluedInSink`, `Producer` |
| `generator/llm.py` | OpenAI wrapper used by promptgen |
| `ui/index.html` | Landing page — fetches `/domains` API and renders domain cards dynamically |

### Configuration

Stored in `config.json` (auto-created with defaults). Key shape:

```json
{
  "running": false,
  "domain": "company",
  "interval_seconds": 2,
  "batch_size": 10,
  "scenarios": {},
  "sources": {"company": "synthetic"},
  "gleif": {"csv_path": "", "guess_websites": false},
  "sinks": {
    "<domain>": {"enabled": false, "url": "", "bearer": "", "mode": "batch", "timeout": 10.0}
  }
}
```

`set_config()` uses deep-merge, so partial PATCH requests work. Per-domain sinks (`config.sinks.<domain>`) take priority over the legacy global `config.sink`.

### Key API endpoints

| Endpoint | Purpose |
|---|---|
| `GET /status` | Runner alive, last send result, active sink info |
| `GET /domains` | List auto-discovered domains (consumed by landing page) |
| `GET\|POST /config` | Read or deep-merge-patch global config |
| `GET\|POST /config/domain/{domain}` | Scoped config for one domain |
| `GET\|POST /generate-now` | Generate a batch immediately |
| `GET\|POST /generate-download` | Generate and return as downloadable JSON |
| `GET /recent` | Last N records from in-memory buffer |
| `GET /send-log` | JSONL send history |
| `GET /ui` | Landing page (lists all domains) |
| `GET /ui/{domain}` | Per-domain UI (auto-routed, no hardcoding) |

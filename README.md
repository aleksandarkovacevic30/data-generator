# data-generator
# data-generator


## Local Installation

### 0) Prereqs

* Python 3.10+ installed.
* The repo/folder contains:

  * `app.py`, `runner.py`
  * `generator/` with `__init__.py`, `domains/company.py`, `domains/beverage.py`
  * UI folders: `ui_main/`, `ui_company/`, `ui_beverage/` with `index.html`
  * `requirements.txt` including at least:
    `fastapi uvicorn gunicorn httpx faker pandas pyarrow`

### 1) Create a venv & install deps

```bash
cd /path/to/your/project
python3 -m venv .venv
source .venv/bin/activate  # (Windows: .venv\Scripts\activate)
pip install --upgrade pip
pip install -r requirements.txt
```

### 2) Run the web server (dev mode)

```bash
# from project root (same folder as app.py)
python -m uvicorn app:app --reload --host 0.0.0.0 --port 8000
```

Now open:

* UI: `http://localhost:8000/ui`
* Company page: `http://localhost:8000/ui/company`
* Beverage page: `http://localhost:8000/ui/beverage`

> If you get 404 for UI, your UI folders aren’t where `app.py` expects them. They must be siblings of `app.py` (`ui_main/`, `ui_company/`, `ui_beverage/`).

### 3) Run the background loop (runner)

Open a **second terminal** (keep the web server running in the first one), activate the venv, then:

```bash
cd /path/to/your/project
source .venv/bin/activate
python runner.py
```

You’ll see the tick logs. Leave it running to honor Start/Stop from the UI.

> If you only want to test generation without the loop, skip runner.py and use **Generate Now** in the UI.

### 4) Quick sanity tests

From another shell:

```bash
# server alive?
curl -s http://127.0.0.1:8000/status | python -m json.tool

# generate once (no sink needed)
curl -s -X POST "http://127.0.0.1:8000/generate-now?n=3" | python -m json.tool

# generate & download
curl -OJ "http://127.0.0.1:8000/generate-download?n=5"
```

### 5) Start/Stop the loop via API

```bash
# start (company domain)
curl -s -X POST http://127.0.0.1:8000/config \
  -H 'Content-Type: application/json' \
  -d '{"running":true,"domain":"company","interval_seconds":1,"batch_size":3,"sinks":{"company":{"enabled":false}}}' \
  | python -m json.tool

# stop
curl -s -X POST http://127.0.0.1:8000/stop | python -m json.tool
```

Watch the last row changing:

```bash
watch -n 1 'curl -s http://127.0.0.1:8000/status | jq -r ".runner_alive, .last_row?.company_id // .last_row?.record_id"'
```

### 6) Seeing HTTP responses from sends

We already added logging:

* Latest response (success or error): `GET /status` → `last_send_status`, `last_send_body`, `last_send_error`, `last_send_curl`
* Full history: `GET /send-log?limit=50`

Examples:

```bash
curl -s http://127.0.0.1:8000/status | jq '.last_send_status,.last_send_error,.last_send_curl'
curl -s http://127.0.0.1:8000/send-log?limit=10 | jq .
```

### 7) Running on port 80 (optional, not recommended for dev)

Port 80 requires elevated privileges:

* **Linux quick-and-dirty**: `sudo python -m uvicorn app:app --host 0.0.0.0 --port 80` (not great).
* **Safer**: keep uvicorn on 8000 and put Nginx/Traefik in front.
* **Capability hack (Linux)** to allow binding to 80 without root:

  ```bash
  sudo setcap 'cap_net_bind_service=+ep' $(readlink -f .venv/bin/python)
  python -m uvicorn app:app --host 0.0.0.0 --port 80
  ```

### 8) Common faceplants (and fixes)

* **Clicking Start does nothing** → you didn’t start `runner.py`. It’s separate.
* **UI loads but endpoints 404** → you’re not running from the project root; `app.py` can’t find UI dirs. Run `uvicorn` from the same folder as `app.py`.
* **ImportError: faker/httpx/etc.** → you installed deps outside the venv. Activate the venv and reinstall.
* **Sink enabled and it “hangs”** → your CluedIn endpoint is slow/blocked. Disable sink while testing; check `/status.last_send_curl` to reproduce with curl. Review `/send-log`.

That’s it. If anything fails, paste the exact command and the last 30 lines of output from:

```bash
# web server logs (the terminal running uvicorn shows errors immediately)
# runner logs:
python runner.py   # run in foreground to see tracebacks
```


change to gleif

curl -s -X POST http://127.0.0.1:8000/config \
  -H 'Content-Type: application/json' \
  -d '{
        "sources": {"company": "gleif"},
        "gleif": {
          "csv_path": "/absolute/path/to/lei_golden_copy.csv.gz",
          "guess_websites": true
        }
      }' | jq '.config.sources, .config.gleif'

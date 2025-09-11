# tests/conftest.py
import sys
import os
import random
import pytest
from fastapi.testclient import TestClient

# Ensure the project root (where app.py lives) is on PYTHONPATH
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import app as backend  # noqa: E402


@pytest.fixture(autouse=True)
def _seed():
    # Make random behavior deterministic within each test
    random.seed(42)
    yield
    random.seed(42)


@pytest.fixture(scope="session", autouse=True)
def _tmp_config(monkeypatch, tmp_path_factory):
    """
    Redirect config.json writes to a temp file so tests don't touch your real config.
    """
    cfg_dir = tmp_path_factory.mktemp("cfg")
    cfg_path = cfg_dir / "config.json"
    monkeypatch.setattr(backend, "CONFIG_PATH", str(cfg_path), raising=False)
    yield


@pytest.fixture(scope="session")
def client():
    # Ensure background producer is not running during tests
    try:
        backend.producer.stop()
    except Exception:
        pass
    return TestClient(backend.app)

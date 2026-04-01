# tests/conftest.py
import sys
import os
import random
import pytest
from fastapi.testclient import TestClient

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import app as backend  # noqa: E402


@pytest.fixture(autouse=True)
def _seed():
    random.seed(42)
    yield
    random.seed(42)


@pytest.fixture(scope="session", autouse=True)
def _tmp_config(tmp_path_factory):
    """Redirect config.json writes to a temp file so tests don't touch the real config."""
    cfg_dir  = tmp_path_factory.mktemp("cfg")
    cfg_path = str(cfg_dir / "config.json")
    # Patch directly on the module (no monkeypatch needed for session scope)
    original = backend.CONFIG_PATH
    backend.CONFIG_PATH = cfg_path
    yield
    backend.CONFIG_PATH = original


@pytest.fixture(scope="session")
def client():
    return TestClient(backend.app)

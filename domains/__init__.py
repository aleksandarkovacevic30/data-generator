"""
Domain auto-discovery for the Master Data Generator.

To add a new domain:
  1. Create  domains/<name>/  as a Python package (add __init__.py)
  2. Export from that __init__.py:
       DISPLAY_NAME: str          – human-readable label for the nav
       DESCRIPTION:  str          – one-line blurb shown on the landing page
       UI_FILE:      pathlib.Path – absolute path to the domain's ui.html
       make_generator(cfg: dict)  – factory that returns an object with
                                    .generate_batch(n: int) -> list[dict]
  3. Add generator.py and ui.html alongside __init__.py

That's it — app.py and runner.py call discover_all() at startup and will
pick up the new domain automatically, including its UI route.
"""

import importlib
import pkgutil
import logging
from pathlib import Path
from typing import Any, Dict

log = logging.getLogger("mdg.domains")

_DOMAINS_DIR = Path(__file__).parent


def discover_all() -> Dict[str, Any]:
    """Return {domain_name: module} for every valid domain package."""
    result: Dict[str, Any] = {}
    for _finder, name, ispkg in pkgutil.iter_modules([str(_DOMAINS_DIR)]):
        if not ispkg:
            continue
        try:
            mod = importlib.import_module(f"domains.{name}")
            if callable(getattr(mod, "make_generator", None)):
                result[name] = mod
            else:
                log.debug("Skipping '%s': no make_generator()", name)
        except Exception as exc:
            log.warning("Skipping domain '%s': %s", name, exc)
    return result

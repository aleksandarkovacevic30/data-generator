from pathlib import Path
from typing import Any, Dict

DISPLAY_NAME = "Company"
DESCRIPTION  = "Generate GLEIF/company-like master data."
UI_FILE      = Path(__file__).parent / "ui.html"


def make_generator(cfg: Dict[str, Any]):
    """Return a configured CompanyGenerator or CompanyFromGLEIFGenerator."""
    src = ((cfg.get("sources") or {}).get("company") or "synthetic").lower()
    if src == "gleif":
        from .gleif import CompanyFromGLEIFGenerator
        csv_path  = ((cfg.get("gleif") or {}).get("csv_path") or "").strip()
        guess     = bool((cfg.get("gleif") or {}).get("guess_websites", False))
        return CompanyFromGLEIFGenerator(csv_path=csv_path, guess_websites=guess)

    from .generator import CompanyGenerator
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

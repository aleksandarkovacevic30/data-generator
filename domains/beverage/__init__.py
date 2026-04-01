from pathlib import Path
from typing import Any, Dict

DISPLAY_NAME = "Beverage"
DESCRIPTION  = "Sample beverage catalog data."
UI_FILE      = Path(__file__).parent / "ui.html"


def make_generator(cfg: Dict[str, Any]):
    from .generator import BeverageGenerator
    g = BeverageGenerator()
    scen = cfg.get("scenarios") or {}
    if scen:
        try:
            g.set_scenarios(scen)
        except Exception:
            pass
    params = cfg.get("beverage_params") or {}
    for k, v in params.items():
        if hasattr(g, k):
            setattr(g, k, v)
    return g

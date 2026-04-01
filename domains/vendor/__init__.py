from pathlib import Path
from typing import Any, Dict

DISPLAY_NAME = "Vendor"
DESCRIPTION  = "Vendor records derived from company/GLEIF data."
UI_FILE      = Path(__file__).parent / "ui.html"


def make_generator(cfg: Dict[str, Any]):
    from .generator import VendorGenerator
    return VendorGenerator(cfg)

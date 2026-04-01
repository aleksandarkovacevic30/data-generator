from pathlib import Path
from typing import Any, Dict

DISPLAY_NAME = "PromptGen"
DESCRIPTION  = "Prompt-driven generator (canonical data, near-duplicates, DQ issues)."
UI_FILE      = Path(__file__).parent / "ui.html"


def make_generator(cfg: Dict[str, Any]):
    from .generator import PromptGenerator
    return PromptGenerator(cfg)

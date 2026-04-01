import uuid
import re
import random
from datetime import datetime, timezone
from typing import Dict, Any, List

DEFAULT_SCENARIOS: Dict[str, float] = {
    "abbr_brand":   0.25,
    "abbr_size":    0.25,
    "abbr_pack":    0.25,
    "random_noise": 0.15,
}

BRANDS = [
    ("Coca-Cola",              ["CC", "COKE"]),
    ("Diet Coke",              ["DC", "DT COKE"]),
    ("Sprite",                 ["SPR"]),
    ("Fanta Orange",           ["FTA ORANGE", "FANTA ORG"]),
    ("Barq's Root Beer",       ["BARQS RB", "BARQS ROOTBEER"]),
    ("Schweppes Tonic",        ["SCHW TONIC"]),
    ("Canada Dry Ginger Ale",  ["C DRY GINGER", "C DRY GA"]),
]

SIZES_ML    = [330, 500, 700, 750, 1000, 1500]
PACK_COUNTS = [6, 12, 20, 24]


def _maybe(p: float) -> bool:
    return random.random() < float(p)


def _abbr_size(ml: int) -> str:
    if ml == 500 and _maybe(0.5):
        return "0.5L"
    if ml == 750 and _maybe(0.5):
        return "075L"
    if ml == 330 and _maybe(0.5):
        return "33CL"
    if _maybe(0.3):
        return f"{ml}ML"
    return f"{ml}ml"


def _abbr_pack(n: int) -> str:
    return random.choice([f"{n}X", f"1X{n}", f"{n} PACK"])


def _noisify(s: str) -> str:
    if not s:
        return s
    return re.sub(r"\s+", " ", s.replace("-", " - ").replace("/", " / ")).strip()


class BeverageGenerator:
    name = "beverage"

    def __init__(self) -> None:
        self.scenarios = dict(DEFAULT_SCENARIOS)

    def default_scenarios(self) -> Dict[str, float]:
        return dict(DEFAULT_SCENARIOS)

    def set_scenarios(self, overrides: Dict[str, float]) -> None:
        for k, v in (overrides or {}).items():
            if k in self.scenarios:
                self.scenarios[k] = max(0.0, min(1.0, float(v)))

    def _one(self) -> Dict[str, Any]:
        brand, abbrs = random.choice(BRANDS)
        size_ml = random.choice(SIZES_ML)
        pack    = random.choice(PACK_COUNTS)

        brand_out = abbrs[0] if abbrs and _maybe(self.scenarios["abbr_brand"]) else brand
        size_out  = _abbr_size(size_ml) if _maybe(self.scenarios["abbr_size"]) else f"{size_ml}ml"
        pack_out  = _abbr_pack(pack)    if _maybe(self.scenarios["abbr_pack"]) else f"{pack}x"

        text_tech = f"{size_out} {pack_out} {brand_out}"
        if _maybe(self.scenarios["random_noise"]):
            text_tech = _noisify(text_tech)

        return {
            "domain":    "beverage",
            "record_id": f"BEV_{uuid.uuid4().hex[:10].upper()}",
            "text_tech": text_tech,
            "_issues":   [],
            "_source":   "generator:beverage",
            "_ts":       datetime.now(timezone.utc).isoformat(),
        }

    def generate_batch(self, n: int) -> List[Dict[str, Any]]:
        return [self._one() for _ in range(max(1, int(n)))]

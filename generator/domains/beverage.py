import uuid, random, re
from datetime import datetime, timezone
from typing import Dict, Any, List

DEFAULT_SCENARIOS = {
    "abbr_brand": 0.25,       # e.g., Coca-Cola -> CC
    "abbr_size": 0.25,        # 500ml -> 50cl or .5l
    "abbr_pack": 0.25,        # 24x -> 1X24, etc.
    "random_noise": 0.15,     # stray hyphens/slashes
}

BRANDS = [
    ("Coca-Cola", ["CC", "COKE"]),
    ("Diet Coke", ["DC", "DT COKE"]),
    ("Sprite", ["SPR"]),
    ("Fanta Orange", ["FTA ORANGE","FANTA ORG"]),
    ("Barq's Root Beer", ["BARQS RB","BARQS ROOTBEER"]),
    ("Schweppes Tonic", ["SCHW TONIC"]),
    ("Canada Dry Ginger Ale", ["C DRY GINGER","C DRY GA"]),
]

SIZES_ML = [330, 500, 700, 750, 1000, 1500]  # ml
PACK_COUNTS = [6, 12, 20, 24]

def _maybe(p: float) -> bool: return random.random() < float(p)

def _abbr_brand(name: str, abbrs: List[str]) -> str:
    return random.choice(abbrs) if abbrs and _maybe(1.0) else name

def _abbr_size(ml: int) -> str:
    # convert to a messy equivalent
    if ml == 500 and _maybe(0.5): return "0.5L"
    if ml == 750 and _maybe(0.5): return "075L"
    if ml == 330 and _maybe(0.5): return "33CL"
    if _maybe(0.3): return f"{ml}ML"
    return f"{ml}ml"

def _abbr_pack(n: int) -> str:
    return random.choice([f"{n}X", f"1X{n}", f"{n} PACK"]) if _maybe(1.0) else f"{n}x"

def _noisify(s: str) -> str:
    if not s: return s
    return re.sub(r"\s+", " ", s.replace("-", " - ").replace("/", " / ")).strip()

class BeverageGenerator:
    def __init__(self):
        self.scenarios = dict(DEFAULT_SCENARIOS)

    def set_scenarios(self, overrides: Dict[str, float]):
        for k,v in (overrides or {}).items():
            if k in self.scenarios: self.scenarios[k] = float(v)

    def _one(self) -> Dict[str, Any]:
        brand, abbrs = random.choice(BRANDS)
        size_ml = random.choice(SIZES_ML)
        pack = random.choice(PACK_COUNTS)

        brand_out = brand
        size_out  = f"{size_ml}ml"
        pack_out  = f"{pack}x"

        if _maybe(self.scenarios["abbr_brand"]):
            brand_out = _abbr_brand(brand, abbrs)
        if _maybe(self.scenarios["abbr_size"]):
            size_out = _abbr_size(size_ml)
        if _maybe(self.scenarios["abbr_pack"]):
            pack_out = _abbr_pack(pack)

        text_tech = f"{size_out} {pack_out} {brand_out}"
        if _maybe(self.scenarios["random_noise"]):
            text_tech = _noisify(text_tech)

        return {
            "domain": "beverage",
            "record_id": f"BEV_{uuid.uuid4().hex[:10].upper()}",
            "text_tech": text_tech,
            "_issues": [],
            "_source": "generator:beverage",
            "_ts": datetime.now(timezone.utc).isoformat(),
        }

    def generate_batch(self, n: int) -> List[Dict[str, Any]]:
        return [ self._one() for _ in range(max(1,int(n))) ]

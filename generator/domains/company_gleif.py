# generator/domains/company_gleif.py
# Real company (B2B) generator seeded from a local GLEIF CSV (+ optional website guessing)
# Now with data-quality issue injection and explicit LEI fields.

from __future__ import annotations
import os
import io
import gzip
import zipfile
import random
import re
import string
from typing import Dict, Any, List, Optional

import pandas as pd


# ---------------- I/O helpers ----------------
def _first_present(columns, candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in columns:
            return c
    return None


def _read_table(path: str) -> pd.DataFrame:
    # Accept .csv, .csv.gz, or a single .csv inside a .zip
    if path.lower().endswith(".gz"):
        with gzip.open(path, "rb") as f:
            return pd.read_csv(f, dtype=str, keep_default_na=False)
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path, "r") as z:
            csv_members = [m for m in z.namelist() if m.lower().endswith(".csv")]
            if not csv_members:
                raise ValueError("ZIP has no CSV inside")
            with z.open(csv_members[0], "r") as f:
                data = f.read()
            return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _norm(s: Optional[str]) -> str:
    return (s or "").strip()


# ---------------- Issue helpers ----------------
def _chance(p: float) -> bool:
    try:
        return random.random() < float(p)
    except Exception:
        return False

# 100 city-name variants you can use for fuzzing / normalization tests.
CITY_VARIANTS = {
    "new york": [
        "NYC", "N.Y.C.", "New York City", "Nueva York", "Nova Iorque",
        "Nowy Jork", "Нью-Йорк", "نيويورك", "纽约", "ニューヨーク",
        "Nwe York",  # typo
    ],
    "los angeles": [
        "LA", "L.A.", "Los Ángeles", "City of Angels", "Лос-Анджелес",
        "ロサンゼルス", "洛杉矶", "لوس أنجلِس", "Los Angles",  # typo
        "Los Angelos",  # typo
        "Angels City",  # loose nickname-ish
    ],
    "belgrade": [
        "Beograd", "Белград", "Belgrad", "Belgrado", "Belgrád",
        "Bělehrad", "Beograde",  # typo
        "Belgerade",  # typo
        "Belgrde",  # typo
        "BGD", "B.G.D.",
    ],
    "vienna": [
        "Wien", "Vienne", "Viena", "Bécs", "Wiedeń",
        "Vídeň", "Вена", "Беч", "Beč",
        "Vinna",  # typo
        "Wien City",
    ],
    "zurich": [
        "Zürich", "Zurich", "ZRH", "Züri", "Zurigo",
        "Zurique", "Zúrich", "Цюрих", "苏黎世", "زيورخ",
        "Zuerich",  # ASCII umlaut replacement
    ],
    "geneva": [
        "Genève", "Genf", "Ginevra", "Ginebra", "Женева",
        "جنيف", "日内瓦", "Genava",  # typo
        "Geneeva",  # typo
        "GVA", "G.V.A.",
    ],
    "london": [
        "LON", "Ldn", "L.D.N.", "Londra", "Londres",
        "Лондон", "لندن", "倫敦", "伦敦", "Londen",
        "Londn",  # typo
    ],
    "paris": [
        "PAR", "París", "Parigi", "Париж", "باريس",
        "巴黎", "Παρίσι", "Pariz", "Paree",
        "Pâris",  # typo/diacritic
        "Parsi",  # typo
    ],
    "munich": [
        "München", "Munchen", "MUC", "Minhen", "Monaco di Baviera",
        "Мюнхен", "ميونخ", "慕尼黑", "Monachium", "Mnichov",
        "Munique", "Munic",  # typo
    ],
}
# Total entries = 11*8 (first eight cities) + 12 (Munich) = 100


_ABBREV_COUNTRY_MAP = {    "united states": ["USA",
     "U.S.A.",
     "US",
     "U.S.",
     "United States of America",
     "US of A",
     "America",
     "Estados Unidos",
     "EE.UU.",
     "EUA",
     "États-Unis",
     "Vereinigte Staaten",
     "Vereinigte Staaten von Amerika",
     "VSA",
     "США",
     "Соединённые Штаты",
     "الولايات المتحدة",
     "संयुक्त राज्य अमेरिका",
     "美国",
     "アメリカ合衆国",
     "SAD",
     "Sjedinjene Američke Države",
     "Stati Uniti",
     "Estados Unidos da América",
     "Unitd States"
     ],
    "united kingdom": ["UK",
    "U.K.",
    "Royaume-Uni",
    "Reino Unido",
    "Regno Unito",
    "Vereinigtes Königreich",
    "Великобритания",
    "Уједињено Краљевство",
    "Velika Britanija",
    "英国",
    "イギリス",
    "المملكة المتحدة",
    "Unted Kingdom"],

    "france": ["France",
    "République française",
    "FR",
    "FRA",
    "Francia",
    "França",
    "Frankreich",
    "Fransa",
    "Francuska",
    "Франция",
    "فرنسا",
    "法国",
    "フランス",
    "Frnace"], 

    "germany": ["Germany",
    "Deutschland",
    "BRD",
    "Bundesrepublik Deutschland",
    "DE",
    "DEU",
    "Alemania",
    "Allemagne",
    "Alemanha",
    "Niemcy",
    "Saksa",
    "Njemačka",
    "Немачка",
    "Германия",
    "ألمانيا",
    "德国",
    "ドイツ",
    "Germnay"],

    "austria": ["Austria",
    "Österreich",
    "Oesterreich",
    "Republic of Austria",
    "AT",
    "AUT",
    "Autriche",
    "Austrija",
    "Аустрија",
    "Rakousko",
    "Ausztria",
    "Rakúsko",
    "Avstrija",
    "النمسا",
    "奥地利",
    "オーストリア",
    "Autsria"],

    "switzerland": ["Switzerland",
    "Confoederatio Helvetica",
    "Schweizerische Eidgenossenschaft",
    "Schweiz",
    "Suisse",
    "Svizzera",
    "Svizra",
    "CH",
    "CHE",
    "SUI",
    "Suiza",
    "Suíça",
    "Švajcarska",
    "Швајцарска",
    "Szwajcaria",
    "Elveția",
    "Elveţia",
    "瑞士",
    "スイス",
    "سويسرا",
    "Switzrland"],
    "serbia": ["Serbia",
     "Srbija",
     "Србија",
     "Republika Srbija",
     "Republic of Serbia",
     "RS",
     "SRB",
     "Serbie",
     "Serbien",
     "Serbia (RS)",
     "塞尔维亚",
     "セルビア",
     "صربيا",
     "Serbija"]
}
def _abbrev_country(name: str) -> str:
    if not name:
        return name
    key = name.lower().strip()
    if key in _ABBREV_COUNTRY_MAP:
        return random.choice(_ABBREV_COUNTRY_MAP[key])
    # fallback: 3-letter consonant-heavy abbreviation
    return name


def _abbrev_city(name: str) -> str:
    if not name:
        return name
    key = name.lower().strip()
    if key in CITY_VARIANTS:
        return random.choice(CITY_VARIANTS[key])
    # fallback: 3-letter consonant-heavy abbreviation
    letters = [c for c in key if c.isalpha()]
    if not letters:
        return name
    stripped = "".join([c for c in letters if c not in "aeiou"])
    base = (stripped or "".join(letters))[:3].upper()
    return base or name


def _add_ws_noise(s: str) -> str:
    if not s:
        return s
    patterns = [
        lambda x: f" {x} ",
        lambda x: x.replace(" ", "  ") if " " in x else f"{x}  ",
        lambda x: f"\t{x}",
        lambda x: f"{x}\t",
    ]
    return random.choice(patterns)(s)


def _typo(s: str) -> str:
    if not s or len(s) < 3:
        return s
    i = random.randint(0, len(s) - 2)
    if s[i].isspace() or s[i + 1].isspace():
        return s  # avoid breaking spaces too often
    # swap adjacent chars
    return s[:i] + s[i + 1] + s[i] + s[i + 2:]


def _slugify_for_domain(name: str) -> str:
    s = name.lower()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    s = s.replace("-", "")
    if not s:
        s = "company"
    return s + ".com"


def _invalidate_website(url: str) -> str:
    if not url:
        return url
    variants = [
        lambda u: u.replace("https://", "http:/"),                # missing slash
        lambda u: u.replace("https://", "http://").replace(".", ","),  # bad delimiter
        lambda u: u.replace("https://", ""),                      # no scheme
        lambda u: u + " ",                                        # trailing space
        lambda u: "htps://" + u[8:],                              # misspelled scheme
    ]
    return random.choice(variants)(url)


def _make_email_from_website(url: str) -> Optional[str]:
    # crude "info@domain" based on hostname portion
    if not url:
        return None
    m = re.match(r"^https?://([^/]+)", url.strip())
    host = m.group(1) if m else url.replace("https://", "").replace("http://", "")
    host = host.strip().strip("/")
    if not host or "." not in host:
        return None
    return f"info@{host}"


def _invalidate_email(addr: str) -> str:
    variants = [
        lambda a: a.replace("@", " at "),    # obvious invalid
        lambda a: a.replace(".", ","),       # bad dot
        lambda a: a + " ",                   # space
        lambda a: a.split("@")[0],           # missing domain
    ]
    return random.choice(variants)(addr)


# ---------------- Generator ----------------
class CompanyFromGLEIFGenerator:
    """
    Generate Company (B2B) records from a GLEIF Golden Copy CSV (or gzip/zip).
    Applies data-quality scenarios via set_scenarios({...}).

    Supported scenario keys and typical ranges (0..1):
      - swap_hq_city_country
      - abbrev_city
      - add_whitespace
      - random_typo_name
      - missing_postal
      - invalid_website
      - invalid_email
    """

    def __init__(self, csv_path: str, guess_websites: bool = False):
        if not csv_path or not os.path.isfile(csv_path):
            raise FileNotFoundError(f"GLEIF CSV not found: {csv_path}")
        self.csv_path = csv_path
        self.guess_websites = bool(guess_websites)
        self.scenarios: Dict[str, float] = {
            "swap_hq_city_country": 0.0,
            "abbrev_city": 0.0,
            "add_whitespace": 0.0,
            "random_typo_name": 0.0,
            "missing_postal": 0.0,
            "invalid_website": 0.0,
            "invalid_email": 0.0,
        }

        df = _read_table(csv_path)
        cols = set(df.columns)

        c_lei = _first_present(cols, ["LEI", "Lei"])
        if not c_lei:
            raise ValueError("CSV lacks LEI column")

        c_name = _first_present(cols, [
            "Entity.LegalName", "Entity.LegalName.LegalName",
            "Entity.LegalNameLegal", "Entity.LegalName_(Transliterated)"
        ])

        # Legal/Registered address
        leg = {
            "line1": _first_present(cols, [
                "Entity.LegalAddress.AddressLine1", "Entity.LegalAddress.FirstAddressLine",
                "Entity.LegalAddress.Address1"
            ]),
            "line2": _first_present(cols, [
                "Entity.LegalAddress.AddressLine2", "Entity.LegalAddress.SecondAddressLine",
                "Entity.LegalAddress.Address2"
            ]),
            "line3": _first_present(cols, [
                "Entity.LegalAddress.AddressLine3", "Entity.LegalAddress.ThirdAddressLine",
                "Entity.LegalAddress.Address3"
            ]),
            "city": _first_present(cols, ["Entity.LegalAddress.City"]),
            "region": _first_present(cols, ["Entity.LegalAddress.Region"]),
            "postal": _first_present(cols, ["Entity.LegalAddress.PostalCode"]),
            "country": _first_present(cols, ["Entity.LegalAddress.Country"]),
        }

        # Headquarters address (optional)
        hq = {
            "line1": _first_present(cols, [
                "Entity.HeadquartersAddress.AddressLine1", "Entity.HeadquartersAddress.FirstAddressLine"
            ]),
            "line2": _first_present(cols, [
                "Entity.HeadquartersAddress.AddressLine2", "Entity.HeadquartersAddress.SecondAddressLine"
            ]),
            "line3": _first_present(cols, [
                "Entity.HeadquartersAddress.AddressLine3", "Entity.HeadquartersAddress.ThirdAddressLine"
            ]),
            "city": _first_present(cols, ["Entity.HeadquartersAddress.City"]),
            "region": _first_present(cols, ["Entity.HeadquartersAddress.Region"]),
            "postal": _first_present(cols, ["Entity.HeadquartersAddress.PostalCode"]),
            "country": _first_present(cols, ["Entity.HeadquartersAddress.Country"]),
        }

        keep_cols = [c_lei]
        if c_name: keep_cols.append(c_name)
        for v in list(leg.values()) + list(hq.values()):
            if v and v not in keep_cols:
                keep_cols.append(v)

        df = df[keep_cols].copy()

        rename_map = {}
        if c_name: rename_map[c_name] = "legal_name"
        rename_map[c_lei] = "LEI"
        # legal
        if leg["line1"]:  rename_map[leg["line1"]]  = "leg_line1"
        if leg["line2"]:  rename_map[leg["line2"]]  = "leg_line2"
        if leg["line3"]:  rename_map[leg["line3"]]  = "leg_line3"
        if leg["city"]:   rename_map[leg["city"]]   = "leg_city"
        if leg["region"]: rename_map[leg["region"]] = "leg_region"
        if leg["postal"]: rename_map[leg["postal"]] = "leg_postal"
        if leg["country"]:rename_map[leg["country"]]= "leg_country"
        # HQ
        if hq["line1"]:   rename_map[hq["line1"]]   = "hq_line1"
        if hq["line2"]:   rename_map[hq["line2"]]   = "hq_line2"
        if hq["line3"]:   rename_map[hq["line3"]]   = "hq_line3"
        if hq["city"]:    rename_map[hq["city"]]    = "hq_city"
        if hq["region"]:  rename_map[hq["region"]]  = "hq_region"
        if hq["postal"]:  rename_map[hq["postal"]]  = "hq_postal"
        if hq["country"]: rename_map[hq["country"]] = "hq_country"

        df.rename(columns=rename_map, inplace=True)

        # Keep only non-empty LEI rows
        df = df[df["LEI"].astype(str).str.len() > 0]

        self._df = df.reset_index(drop=True)
        self._n = len(self._df)
        if self._n == 0:
            raise ValueError("GLEIF CSV appears empty after filtering")
        self._idx = list(range(self._n))

    # --- public API ---
    def set_scenarios(self, scenarios: Dict[str, float]):
        # merge/override only known keys
        for k in list(self.scenarios.keys()):
            if k in scenarios:
                try:
                    self.scenarios[k] = max(0.0, min(1.0, float(scenarios[k])))
                except Exception:
                    pass
        return self

    def generate_one(self) -> Dict[str, Any]:
        i = random.choice(self._idx)
        row = self._df.iloc[i]
        rec = self._row_to_record(row)
        issues = self._apply_issues(rec)
        if issues:
            rec["_issues"] = issues
        return rec

    def generate_batch(self, n: int) -> List[Dict[str, Any]]:
        n = max(1, int(n))
        return [self.generate_one() for _ in range(n)]

    # --- record build + issues ---
    def _row_to_record(self, row: pd.Series) -> Dict[str, Any]:
        lei = _norm(row.get("LEI"))
        name = _norm(row.get("legal_name")) or f"Company {lei[:8]}"

        # prefer HQ address when present; fallback to legal
        city     = _norm(row.get("hq_city"))    or _norm(row.get("leg_city"))
        country  = _norm(row.get("hq_country")) or _norm(row.get("leg_country"))
        postal   = _norm(row.get("hq_postal"))  or _norm(row.get("leg_postal"))
        region   = _norm(row.get("hq_region"))  or _norm(row.get("leg_region"))
        line1    = _norm(row.get("hq_line1"))   or _norm(row.get("leg_line1"))
        line2    = _norm(row.get("hq_line2"))   or _norm(row.get("leg_line2"))
        line3    = _norm(row.get("hq_line3"))   or _norm(row.get("leg_line3"))

        website = None
        if self.guess_websites and name:
            website = "https://" + _slugify_for_domain(name)

        # optionally fabricate email from the website (gives invalid_email a target)
        email = _make_email_from_website(website) if website else None

        return {
            "domain": "company",
            "company_id": f"LEI_{lei}",
            "lei": lei,
            "name": name,
            "website": website,               # may be None
            "email": email,                   # may be None
            "hq_address_line1": line1,
            "hq_address_line2": line2,
            "hq_address_line3": line3,
            "hq_city": city,
            "hq_region": region,
            "hq_postal": postal,
            "hq_country": country,
            "created_utc": None,
            "updated_utc": None,
            "_source": "gleif",
        }

    def _apply_issues(self, rec: Dict[str, Any]) -> List[str]:
        issues: List[str] = []
        sc = self.scenarios

        # swap city/country
        if rec.get("hq_city") and rec.get("hq_country") and _chance(sc.get("swap_hq_city_country", 0.0)):
            rec["hq_city"], rec["hq_country"] = rec["hq_country"], rec["hq_city"]
            issues.append("swap_hq_city_country")

        # abbrev city
        if rec.get("hq_city"):
            rec["hq_city"] = _abbrev_city(rec["hq_city"])
            issues.append("abbrev_city")

        # abbrev city
        if rec.get("hq_country"):
            rec["hq_country"] = _abbrev_country(rec["hq_country"])
            issues.append("hq_country")


        # add whitespace (apply to a couple of fields)
        if _chance(sc.get("add_whitespace", 0.0)):
            for k in ("name", "hq_address_line1", "hq_city"):
                if rec.get(k):
                    rec[k] = _add_ws_noise(rec[k])
            issues.append("add_whitespace")

        # random typo in name
        if rec.get("name") and _chance(sc.get("random_typo_name", 0.0)):
            rec["name"] = _typo(rec["name"])
            issues.append("random_typo_name")

        # missing postal
        if rec.get("hq_postal") and _chance(sc.get("missing_postal", 0.0)):
            rec["hq_postal"] = ""
            issues.append("missing_postal")

        # invalid website
        if rec.get("website") and _chance(sc.get("invalid_website", 0.0)):
            rec["website"] = _invalidate_website(rec["website"])
            issues.append("invalid_website")

        # invalid email (fabricate if we have a plausible website-derived email)
        if _chance(sc.get("invalid_email", 0.0)):
            if rec.get("email"):
                rec["email"] = _invalidate_email(rec["email"])
                issues.append("invalid_email")
            else:
                # nothing to break; skip
                pass

        return issues

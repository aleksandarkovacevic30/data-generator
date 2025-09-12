import uuid, re, random
from datetime import datetime, timezone
from typing import Dict, Any, List
from faker import Faker
fake = Faker(); Faker.seed(1337); random.seed(1337)

CITY_TO_COUNTRY = {
    "Belgrade":"Serbia","Belgrade":"Srbija","Belgrade":"Serbien","Belgrade":"RS",
    "New York":"United States","New York":"USA","New York":"U.S.A",
    "London":"United Kingdom","London":"U.K.","London":"UK",
    "Paris":"France","Paris":"Francuska","Paris":"Frankreich","Paris":"FRA",
    "Berlin":"Germany","Berlin":"Nemacka","Berlin":"DE",
    "Zurich":"Switzerland","Zurich":"Svajcarska","Zurich":"CH",
    "San Francisco":"United States","San Francisco":"USA",
    "Tokyo":"Japan","Tokyo":"Nippon","Tokyo":"JP",
    "Sydney":"Australia","Sydney":"AUS",
    "Toronto":"Canada","Toronto":"CA",
    "Vienna":"Austria","Vienna":"Österreich"
}
CITY_ABBREV = {"BGD":"Belgrade","NYC":"New York","NY":"New York","Big Apple":"New York",
               "LDN":"London","PAR":"Paris","BER":"Berlin",
               "ZRH":"Zurich","Cirih":"Zurich","Zürich":"Zurich",
               "SFO":"San Francisco",
               "TKY":"Tokyo","Tokio":"Tokyo",
               "SYD":"Sydney","TOR":"Toronto",
               "Beograd":"Belgrade","Belgrad":"Belgrade",
               "Vienna":"Vienna","Wien":"Vienna","Беч":"Vienna"
               }
CITIES = list(CITY_TO_COUNTRY.keys())

def _maybe(p: float) -> bool: return random.random() < float(p)
def _random_typo(s: str) -> str:
    if not s or len(s)<3: return s
    i = random.randint(0, len(s)-2); return s[:i]+s[i+1:]
def _add_ws(s: str) -> str:
    if not s: return s
    return (" "*random.randint(0,2)) + s + (" "*random.randint(0,2))
def _bad_email(domain: str) -> str: return f"contact.at.{domain}"
def _bad_website(domain: str) -> str: return f"www_{domain}__"

DEFAULT_SCENARIOS = {
    "swap_hq_city_country": 0.10,
    "abbrev_city": 0.10,
    "add_whitespace": 0.10,
    "random_typo_name": 0.07,
    "missing_postal": 0.05,
    "invalid_website": 0.04,
    "invalid_email": 0.04,
}

class CompanyGenerator:
    def __init__(self):
        self.scenarios = dict(DEFAULT_SCENARIOS)

    def set_scenarios(self, overrides: Dict[str, float]):
        for k,v in (overrides or {}).items():
            if k in self.scenarios: self.scenarios[k] = float(v)

    def _pick_city_country(self):
        city = random.choice(CITIES); return city, CITY_TO_COUNTRY[city]

    def _clean_record(self) -> Dict[str, Any]:
        city, country = self._pick_city_country()
        legal_name = fake.company()
        trade_name = re.sub(r'\s+(LLC|Inc\.|Ltd\.|GmbH|AG|S\.A\.)\b', '', legal_name)
        domain = re.sub(r'[^a-z0-9]+', '', legal_name.lower())[:18] or "exampleco"
        website = f"https://{domain}.com"; email = f"info@{domain}.com"
        return {
            "domain": "company",
            "company_id": f"COMP_{uuid.uuid4().hex[:10].upper()}",
            "legal_name": legal_name,
            "trade_name": trade_name,
            "hq_city": city,
            "hq_country": country,
            "address_line": fake.street_address(),
            "postal_code": fake.postcode(),
            "website": website,
            "email": email,
            "phone": fake.phone_number(),
            "registration_number": str(uuid.uuid4())[:12].upper(),
            "industry": fake.job().split(",")[0],
            "employees": random.randint(5, 5000),
            "annual_revenue_usd": round(random.uniform(1e5, 5e8), 2),
            "_issues": [],
            "_source": "generator:company",
            "_ts": datetime.now(timezone.utc).isoformat(),
        }

    def _apply_issues(self, r: Dict[str, Any]) -> Dict[str, Any]:
        scen = self.scenarios
        if _maybe(scen["swap_hq_city_country"]):
            r["hq_city"], r["hq_country"] = r["hq_country"], r["hq_city"]; r["_issues"].append("swap_hq_city_country")
        if _maybe(scen["abbrev_city"]):
            rev = {v:k for k,v in CITY_ABBREV.items()}
            ab = rev.get(r.get("hq_city","")); 
            if ab: r["hq_city"]=ab; r["_issues"].append("abbrev_city")
        if _maybe(scen["add_whitespace"]):
            for fld in ["legal_name","trade_name","hq_city","hq_country","address_line"]:
                r[fld] = _add_ws(r.get(fld))
            r["_issues"].append("add_whitespace")
        if _maybe(scen["random_typo_name"]):
            r["legal_name"] = _random_typo(r.get("legal_name","")); r["_issues"].append("random_typo_name")
        if _maybe(scen["missing_postal"]):
            r["postal_code"] = None; r["_issues"].append("missing_postal")
        if _maybe(scen["invalid_website"]):
            dom = (r.get("website") or "example.com").split("://")[-1].split("/")[0]
            r["website"] = _bad_website(dom); r["_issues"].append("invalid_website")
        if _maybe(scen["invalid_email"]):
            dom = (r.get("website") or "example.com").replace("https://","").replace("http://","").split("/")[0]
            r["email"] = _bad_email(dom); r["_issues"].append("invalid_email")
        return r

    def generate_batch(self, n: int) -> List[Dict[str, Any]]:
        return [ self._apply_issues(self._clean_record()) for _ in range(max(1,int(n))) ]

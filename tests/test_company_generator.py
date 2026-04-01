# tests/test_company_generator.py
import random
from domains.company.generator import (
    CompanyGenerator,
    COUNTRY_CITY,
    CITY_TO_COUNTRY_EXACT,
    CITY_TO_COUNTRY_NORM,
)


def test_company_clean_alignment_no_mismatch():
    g = CompanyGenerator()
    zeros = {k: 0.0 for k in g.default_scenarios().keys()}
    g.set_scenarios(zeros)

    random.seed(123)
    batch = g.generate_batch(200)
    assert len(batch) == 200

    for rec in batch:
        assert rec["domain"] == "company"
        assert isinstance(rec["company_id"], str) and rec["company_id"].startswith("COMP_")

        city    = rec["hq_city"]
        country = rec["hq_country"]
        mapped  = CITY_TO_COUNTRY_EXACT.get(city) or CITY_TO_COUNTRY_NORM.get(
            (city or "").lower().replace(" ", "")
        )
        assert mapped is not None, f"City {city!r} should map to a country"
        assert mapped == country, f"Expected {mapped!r} but got {country!r} for city {city!r}"
        assert rec.get("_issues") == []


def test_company_intentional_swap_mismatch_present():
    g = CompanyGenerator()
    ones = {k: 0.0 for k in g.default_scenarios().keys()}
    ones["swap_hq_city_country"] = 1.0
    g.set_scenarios(ones)

    random.seed(321)
    batch = g.generate_batch(30)

    # After a swap, hq_city holds a country name — verify at least one maps to a known country key
    assert any(rec["hq_city"] in COUNTRY_CITY for rec in batch)
    assert any("swap_hq_city_country" in rec.get("_issues", []) for rec in batch)

# tests/test_company_generator.py
import random
from generator.domains.company import (
    CompanyGenerator,
    COUNTRY_CITY,
    CITY_TO_COUNTRY_EXACT,
    CITY_TO_COUNTRY_NORM,
)

def test_company_clean_alignment_no_mismatch():
    g = CompanyGenerator()
    # Kill all issue scenarios to generate clean data
    zeros = {k: 0.0 for k in g.default_scenarios().keys()}
    g.set_scenarios(zeros)

    random.seed(123)
    batch = g.generate_batch(200)
    assert len(batch) == 200

    for rec in batch:
        assert rec["domain"] == "company"
        assert isinstance(rec["company_id"], str) and rec["company_id"].startswith("COMP_")
        # City-country consistency must hold when mismatch scenarios are off
        city = rec["hq_city"]
        country = rec["hq_country"]
        mapped = CITY_TO_COUNTRY_EXACT.get(city) or CITY_TO_COUNTRY_NORM.get(
            (city or "").lower().replace(" ", "")
        )
        assert mapped is not None, f"City {city!r} should map to a country"
        assert mapped == country, f"Expected {mapped} but got {country} for city {city}"

        # Should have zero issues
        assert rec.get("_issues") == []


def test_company_intentional_swap_mismatch_present():
    g = CompanyGenerator()
    ones = {k: 0.0 for k in g.default_scenarios().keys()}
    ones["swap_hq_city_country"] = 1.0
    g.set_scenarios(ones)

    random.seed(321)
    batch = g.generate_batch(30)

    # After swap, hq_city becomes a country name; verify at least one like that.
    assert any(rec["hq_city"] in COUNTRY_CITY.keys() for rec in batch)
    # Issues should reflect the swap
    assert any("swap_hq_city_country" in rec.get("_issues", []) for rec in batch)

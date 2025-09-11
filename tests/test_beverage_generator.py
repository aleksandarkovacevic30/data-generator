# tests/test_beverage_generator.py
import random
from generator.domains.beverage import BeverageGenerator

def test_beverage_single_text_field_and_reasonable_format():
    g = BeverageGenerator()
    # Ensure text_tech isn't blanked by the null-like scenario
    scen = {k: 0.0 for k in g.default_scenarios().keys()}
    scen["null_like_fields"] = 0.0
    g.set_scenarios(scen)

    random.seed(456)
    batch = g.generate_batch(50)
    assert len(batch) == 50

    for rec in batch:
        assert rec["domain"] == "beverage"
        assert "text_tech" in rec and isinstance(rec["text_tech"], str)
        assert rec["text_tech"] != ""

        # Only these non-internal keys should exist at top level
        allowed = {"domain", "record_id", "text_tech"}
        for k in rec.keys():
            if k.startswith("_"):
                continue
            assert k in allowed, f"Unexpected public key in beverage record: {k}"

        # The tech text should have volume hints either in ML/CL/L or gallons (G)
        tt = rec["text_tech"].upper()
        assert ("ML" in tt) or ("CL" in tt) or (" L" in tt) or ("G" in tt) or ("L " in tt)

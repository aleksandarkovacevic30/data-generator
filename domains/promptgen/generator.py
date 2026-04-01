from __future__ import annotations

import itertools
from typing import Any, Dict, List

DEFAULT_SYSTEM = (
    "You generate realistic tabular data. "
    "Fields must be consistent and plausible. Use locale-appropriate formats."
)


class PromptGenerator:
    """
    LLM-driven generator with three prompt types:
      - gen_prompt:  canonical data
      - dupe_prompt: near-duplicates that evade naive string similarity
      - dq_prompt:   data-quality issues typical for this domain

    Requires OPENAI_API_KEY (and optionally OPENAI_MODEL) in the environment.
    """

    name = "promptgen"

    def __init__(self, config: Dict[str, Any]) -> None:
        self.batch_size: int = int(config.get("batch_size", 10))
        p = config.get("promptgen") or {}
        self.spec_prompt:      str = p.get("spec_prompt", "").strip()
        self.gen_prompt:       str = p.get("gen_prompt", "").strip()
        self.dupe_prompt:      str = p.get("dupe_prompt", "").strip()
        self.dq_prompt:        str = p.get("dq_prompt", "").strip()
        self.schema_hint:      str = p.get("schema_hint", "").strip()
        self.max_variant_rows: int = int(p.get("max_variant_rows", self.batch_size))
        self._llm = None

    def _ensure_llm(self):
        if self._llm is None:
            from generator.llm import LLM
            self._llm = LLM()

    def generate_batch(self, n: int) -> List[Dict[str, Any]]:
        self._ensure_llm()
        n = max(1, int(n))
        base_each = max(1, int(n * 0.70))
        dupe_each = max(0, int(n * 0.15))
        dq_each   = max(0, n - base_each - dupe_each)

        rows: List[Dict[str, Any]] = []

        if self.gen_prompt or self.spec_prompt:
            rows.extend(self._llm.complete_json(
                system=DEFAULT_SYSTEM,
                user=(
                    f"SPEC: {self.spec_prompt}\n"
                    f"TASK: {self.gen_prompt or 'Generate canonical rows strictly following the spec.'}"
                ),
                schema_hint=self.schema_hint,
                max_rows=min(self.max_variant_rows, base_each),
            ))

        if self.dupe_prompt:
            rows.extend(self._llm.complete_json(
                system=DEFAULT_SYSTEM,
                user=(
                    f"SPEC: {self.spec_prompt}\n"
                    f"TASK: {self.dupe_prompt}\n"
                    "Make pairs/sets that are the same entity with formatting differences, "
                    "abbreviations, nicknames, swapped tokens, Unicode lookalikes, OCR-like mistakes."
                ),
                schema_hint=self.schema_hint,
                max_rows=min(self.max_variant_rows, dupe_each),
            ))

        if self.dq_prompt:
            rows.extend(self._llm.complete_json(
                system=DEFAULT_SYSTEM,
                user=(
                    f"SPEC: {self.spec_prompt}\n"
                    f"TASK: {self.dq_prompt}\n"
                    "Include typical issues: missing values, invalid categories, wrong encodings, "
                    "transposed digits, invalid dates, leading/trailing whitespace."
                ),
                schema_hint=self.schema_hint,
                max_rows=min(self.max_variant_rows, dq_each),
            ))

        out: List[Dict[str, Any]] = []
        seq = itertools.count(1)
        for r in rows:
            if isinstance(r, dict):
                if "record_id" not in r:
                    r["record_id"] = next(seq)
                out.append(r)
        return out

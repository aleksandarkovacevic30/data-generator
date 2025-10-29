# generator/domains/promptgen.py
from __future__ import annotations
import itertools
from typing import Any, Dict, List, Optional
from . import utils  # if you have shared helpers; else remove
from ..llm import LLM, LLMError

NAME = "promptgen"

DEFAULT_SYSTEM = (
    "You generate realistic tabular data. "
    "Fields must be consistent and plausible. Use locale-appropriate formats."
)

class Domain:
    """
    Prompt-driven domain with 3 specialized prompts:
      - gen_prompt: canonical data
      - dupe_prompt: near-duplicates that evade naive string similarity
      - dq_prompt: data-quality issues typical for this domain
    """

    def __init__(self, config: Dict[str, Any]):
        # Config is set via POST /config
        self.running: bool = bool(config.get("running", False))
        self.batch_size: int = int(config.get("batch_size", 10))
        self.interval_seconds: int = int(config.get("interval_seconds", 1))
        # prompt payload
        p = config.get("promptgen", {}) or {}
        self.spec_prompt: str = p.get("spec_prompt", "").strip()
        self.gen_prompt: str = p.get("gen_prompt", "").strip()
        self.dupe_prompt: str = p.get("dupe_prompt", "").strip()
        self.dq_prompt: str = p.get("dq_prompt", "").strip()
        self.max_variant_rows: int = int(p.get("max_variant_rows", self.batch_size))

        # Optional schema hint (string the model can read to emit consistent keys)
        self.schema_hint: str = p.get("schema_hint", "").strip()
        self.llm = None

    @property
    def name(self) -> str:
        return NAME

    def schema(self) -> Dict[str, str]:
        """A loose schema for UI/CSV headers; optional if you prefer pure free-form."""
        # If you want to fix headers, declare them here; otherwise return {}.
        return {}  # keep free-form for promptgen

    def _ensure_llm(self):
        if self.llm is None:
            self.llm = LLM()

    def generate_rows(self, n: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Called by runner or /generate-now.
        We produce a mix: mostly canonical, with some dupes and dq-issues mixed in.
        """
        self._ensure_llm()
        n = n or self.batch_size
        base_each = max(1, int(n * 0.7))
        dupe_each = max(0, int(n * 0.15))
        dq_each = max(0, n - base_each - dupe_each)

        rows: List[Dict[str, Any]] = []

        # 1) Canonical
        if self.gen_prompt or self.spec_prompt:
            rows.extend(self.llm.complete_json(
                system=DEFAULT_SYSTEM,
                user=f"SPEC: {self.spec_prompt}\nTASK: {self.gen_prompt or 'Generate canonical rows strictly following the spec.'}",
                schema_hint=self.schema_hint,
                max_rows=min(self.max_variant_rows, base_each),
            ))

        # 2) Near-duplicates
        if self.dupe_prompt:
            rows.extend(self.llm.complete_json(
                system=DEFAULT_SYSTEM,
                user=f"SPEC: {self.spec_prompt}\nTASK: {self.dupe_prompt}\n"
                     f"Make pairs/sets that are the same entity with formatting differences, abbreviations, nicknames, swapped tokens, Unicode lookalikes, OCR-like mistakes, etc.",
                schema_hint=self.schema_hint,
                max_rows=min(self.max_variant_rows, dupe_each),
            ))

        # 3) Data-quality issues
        if self.dq_prompt:
            rows.extend(self.llm.complete_json(
                system=DEFAULT_SYSTEM,
                user=f"SPEC: {self.spec_prompt}\nTASK: {self.dq_prompt}\n"
                     f"Include typical issues: missing values, invalid categories, wrong encodings, transposed digits, invalid dates, leading/trailing whitespace.",
                schema_hint=self.schema_hint,
                max_rows=min(self.max_variant_rows, dq_each),
            ))

        # Normalize: ensure dicts only, and add a generic id if missing.
        out: List[Dict[str, Any]] = []
        seq = itertools.count(1)
        for r in rows:
            if isinstance(r, dict):
                if "record_id" not in r and "company_id" not in r:
                    r["record_id"] = next(seq)  # TODO: align with your sink's primary key
                out.append(r)
        return out

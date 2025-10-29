# generator/derivatives.py
from typing import Callable, Dict, Any, List

Transform = Callable[[Dict[str, Any]], Dict[str, Any]]

class DerivedDomain:
    """
    Compose a new domain from a base domain without rewriting generation.
    base: a Domain-like object with .generate_rows(n) -> list[dict]
    transform: per-row mapper that can add/remove/rename fields
    name: exported name for config.domain
    """
    def __init__(self, base, transform: Transform, name: str):
        self._base = base
        self._transform = transform
        self._name = name
        # inherit scheduler knobs from base; you may override via config
        self.batch_size = getattr(base, "batch_size", 10)
        self.interval_seconds = getattr(base, "interval_seconds", 1)

    @property
    def name(self) -> str:
        return self._name

    def schema(self) -> Dict[str, str]:
        # Optional: derive or declare your own headers
        return {}

    def generate_rows(self, n: int = None) -> List[Dict[str, Any]]:
        rows = self._base.generate_rows(n)
        out = []
        for r in rows:
            try:
                out.append(self._transform(r))
            except Exception:
                continue
        return out

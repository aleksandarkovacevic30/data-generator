from typing import Any, Dict, List


class CustomerGenerator:
    """Derive customer records from the company generator."""

    name = "customer"

    def __init__(self, cfg: Dict[str, Any]) -> None:
        import domains.company as company_domain
        self._base = company_domain.make_generator(cfg)

    def generate_batch(self, n: int) -> List[Dict[str, Any]]:
        rows = self._base.generate_batch(n)
        return [self._transform(r) for r in rows]

    @staticmethod
    def _transform(row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "customer_id": row.get("company_id") or row.get("record_id"),
            "name":        row.get("legal_name") or row.get("name"),
            "email":       row.get("email") or row.get("primary_email"),
            "phone":       row.get("phone"),
            "address":     row.get("address_line") or row.get("hq_address_line1"),
            "vat_number":  row.get("lei") or row.get("registration_number"),
        }

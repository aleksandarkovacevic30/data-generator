# generator/domains/vendor.py
from typing import Dict, Any
from . import company  # base domain
from ..derivatives import DerivedDomain

NAME = "vendor"

def _transform(row: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: align keys with your sink's vendor schema
    return {
        "vendor_id": row.get("company_id") or row.get("record_id"),
        "vendor_name": row.get("name") or row.get("company_name"),
        "contact_email": row.get("email") or row.get("primary_email"),
        "contact_phone": row.get("phone"),
        "hq_address": row.get("address"),
        "lei": row.get("lei") or row.get("vat_number")
    }

def make(config: Dict[str, Any]):
    base = company.Domain(config)
    return DerivedDomain(base, _transform, NAME)

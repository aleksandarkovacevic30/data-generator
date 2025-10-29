# generator/domains/customer.py
from typing import Dict, Any
from . import company  # assuming you have generator/domains/company.py
from ..derivatives import DerivedDomain

NAME = "customer"

def _transform(row: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: align field names from your company domain
    return {
        "customer_id": row.get("company_id") or row.get("record_id"),
        "name": row.get("name") or row.get("company_name"),
        "email": row.get("email") or row.get("primary_email"),
        "phone": row.get("phone"),
        "address": row.get("address"),
        "vat_number": row.get("vat_number") or row.get("lei")  # GLEIF -> LEI as VAT-ish stand-in
    }

def make(config: Dict[str, Any]):
    base = company.Domain(config)
    return DerivedDomain(base, _transform, NAME)

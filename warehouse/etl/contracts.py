"""Versioned source contract: additive fields are safe, breaking changes are audited."""
from datetime import date, datetime
from functools import lru_cache
import json
import math
from numbers import Real
from pathlib import Path


@lru_cache(maxsize=1)
def order_contract():
    return json.loads((Path(__file__).resolve().parents[1] / "contracts/orders-v1.json").read_text(encoding="utf-8"))


def missing(value):
    return value is None or (isinstance(value, Real) and math.isnan(float(value))) or (isinstance(value, str) and not value.strip())


def check_order_contract(row, platform):
    spec = order_contract()
    errors = []
    # A mixed DataFrame supplies NaN for the absent primary alias. Do not let it
    # hide a declared breaking version in the secondary alias.
    for version in (row.get("schema_version"), row.get("_schema_version")):
        if missing(version):continue
        parts = str(version).split(".")
        if not all(p.isdigit() for p in parts) or len(parts) > 2 or parts[0] != "1":
            if "CONTRACT_UNSUPPORTED_VERSION" not in errors:errors.append("CONTRACT_UNSUPPORTED_VERSION")
    fields = {**spec["common_required"], **spec["platform_required"].get(platform, {})}
    for name, kind in fields.items():
        value = row.get(name)
        if missing(value):
            errors.append("CONTRACT_MISSING_" + name.upper())
            continue
        valid = False
        if kind == "number":
            try:
                valid = not isinstance(value, bool) and math.isfinite(float(value))
            except (TypeError, ValueError, OverflowError):
                pass
        elif kind == "identifier":
            valid = isinstance(value, str) or (isinstance(value, Real) and not isinstance(value, bool) and math.isfinite(float(value)))
        elif kind == "string":
            valid = isinstance(value, str)
        elif kind == "timestamp":
            valid = isinstance(value, (str, datetime, date))
        if not valid:
            errors.append("CONTRACT_INVALID_TYPE_" + name.upper())
    for name in spec["optional_scalar_fields"]:
        if isinstance(row.get(name), (dict, list, tuple, set)):
            errors.append("CONTRACT_INVALID_TYPE_" + name.upper())
    return errors


def safe_scalar_source(row):
    """Prevent a malformed known field crashing normalization of the entire batch.

    Original values remain in raw_payload. Unknown additive nested fields survive.
    """
    spec = order_contract()
    known = set(spec["common_required"]) | set(spec["optional_scalar_fields"]) | {"sku"}
    for fields in spec["platform_required"].values(): known.update(fields)
    return {k: None if k in known and isinstance(v, (dict, list, tuple, set)) else v for k,v in row.items()}

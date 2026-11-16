"""Small shared helpers: money, dates, ids, canonical fingerprints."""
from __future__ import annotations

import calendar
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"


def period_to_end(period: str) -> str:
    """Return the last calendar day (YYYY-MM-DD) of a YYYY-MM period."""
    year, month = (int(x) for x in period.split("-"))
    last_day = calendar.monthrange(year, month)[1]
    return f"{period}-{last_day:02d}"


def to_cents(value) -> int:
    """Convert yuan (float/str/Decimal) or an explicit integer cent value.

    Accepted inputs:
      int           -> treated as cents directly
      float/str/Decimal -> yuan, rounded to whole cents (half up)
    """
    if isinstance(value, bool):
        raise ValueError("boolean is not a money value")
    if isinstance(value, int):
        return value
    d = Decimal(str(value))
    return int((d * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def canonical_json(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def fingerprint(parts: list) -> str:
    """Deterministic SHA-256 over canonical JSON of all input parts."""
    h = hashlib.sha256()
    for part in parts:
        h.update(canonical_json(part).encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


def rows_to_dicts(rows) -> list[dict]:
    return [dict(r) for r in rows]

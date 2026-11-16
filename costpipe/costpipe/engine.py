"""Allocation engine: weights -> integer-cent allocation with full conservation.

Rounding strategy is the largest-remainder (Hamilton) method:
  1. every participant gets floor(amount * weight);
  2. the remaining cents are handed out one each, highest fractional remainder
     first (ties broken by org_unit_id for determinism).

So sum(allocated) == charge amount exactly, and every cent moved in step 2 is
recorded in the rounding audit and on the receiving line's note — the rounding
tail is explainable per charge, not hidden in an aggregate.
"""
from __future__ import annotations

from . import repository as repo


def build_org_paths(snapshot: dict[str, dict]) -> dict[str, str]:
    """Resolve each org id to a root-to-node path using an as-of snapshot."""
    paths: dict[str, str] = {}

    def path_of(org_id: str, seen: frozenset = frozenset()) -> str:
        if org_id in paths:
            return paths[org_id]
        row = snapshot.get(org_id)
        if row is None or org_id in seen:
            return org_id
        parent = row["parent_id"]
        if not parent or parent not in snapshot:
            p = f"{row['name']}({org_id})"
        else:
            p = f"{path_of(parent, seen | {org_id})} / {row['name']}({org_id})"
        paths[org_id] = p
        return p

    for org_id in snapshot:
        path_of(org_id)
    return paths


def leaf_orgs(snapshot: dict[str, dict]) -> list[str]:
    parents = {r["parent_id"] for r in snapshot.values() if r["parent_id"]}
    return sorted(org_id for org_id in snapshot if org_id not in parents)


def _normalize(raw: dict[str, float]) -> dict[str, float]:
    total = sum(raw.values())
    if total <= 0:
        raise ValueError("weight total must be positive")
    return {k: v / total for k, v in raw.items()}


def largest_remainder(amount_cents: int,
                      weights: dict[str, float]) -> tuple[dict[str, int], list[dict]]:
    """Allocate whole cents by largest remainder.

    Returns (allocation per org, rounding audit rows). Conservation is asserted.
    """
    floors: dict[str, int] = {}
    fractions: dict[str, float] = {}
    for org_id, w in weights.items():
        exact = amount_cents * w
        floors[org_id] = int(exact)
        fractions[org_id] = exact - floors[org_id]

    remainder = amount_cents - sum(floors.values())
    order = sorted(weights, key=lambda o: (-fractions[o], o))
    winners = set(order[:remainder])
    audit = []
    allocation: dict[str, int] = {}
    for org_id in weights:
        cents = floors[org_id] + (1 if org_id in winners else 0)
        allocation[org_id] = cents
        if org_id in winners:
            audit.append({
                "org_unit_id": org_id,
                "fractional_remainder": round(fractions[org_id], 6),
                "adjustment_cents": 1,
            })
    assert sum(allocation.values()) == amount_cents, "allocation broke conservation"
    return allocation, audit


def _weights_for_charge(charge: dict, inputs: dict, leaves: list[str],
                        paths: dict[str, str]) -> tuple[dict[str, float], str]:
    """Return (normalized weights, human-readable basis note)."""
    service = charge["service_id"]
    rules = [r for r in inputs["rules"]
             if r["service_id"] == service or r["service_id"] == "*"]
    exact = [r for r in rules if r["service_id"] == service]
    rule = (exact or rules or [None])[0]

    if rule is None:
        n = len(leaves)
        return {o: 1.0 / n for o in leaves}, \
            "no rule configured; auto equal-split across all leaf orgs"

    if rule["basis"] == "equal":
        n = len(leaves)
        return {o: 1.0 / n for o in leaves}, f"rule {rule['rule_id']} v{rule['version']}: equal"

    if rule["basis"] == "fixed":
        raw = {o: float(w) for o, w in (rule.get("fixed_weights") or {}).items()
               if o in paths and float(w) > 0}
        if not raw:
            n = len(leaves)
            return {o: 1.0 / n for o in leaves}, \
                f"rule {rule['rule_id']} fixed weights matched no org; equal fallback"
        return _normalize(raw), f"rule {rule['rule_id']} v{rule['version']}: fixed weights"

    # basis == 'usage'
    raw: dict[str, float] = {}
    for u in inputs["usage"]:
        if u["service_id"] == service and u["org_unit_id"] in paths:
            raw[u["org_unit_id"]] = raw.get(u["org_unit_id"], 0.0) + float(u["usage_qty"])
    raw = {o: v for o, v in raw.items() if v > 0}
    if not raw:
        n = len(leaves)
        return {o: 1.0 / n for o in leaves}, \
            f"rule {rule['rule_id']} v{rule['version']}: usage basis but no usage found; equal fallback"
    return _normalize(raw), f"rule {rule['rule_id']} v{rule['version']}: usage-proportional"


def allocate_period(inputs: dict) -> tuple[list[dict], list[dict]]:
    """Turn one period+scenario input snapshot into alloc_line rows + audit."""
    snapshot = {r["org_unit_id"]: r for r in inputs["org"]}
    paths = build_org_paths(snapshot)
    leaves = leaf_orgs(snapshot)

    lines: list[dict] = []
    audit: list[dict] = []

    for charge in inputs["charges"]:
        if not leaves:
            lines.append({
                "charge_id": charge["charge_id"],
                "service_id": charge["service_id"],
                "org_unit_id": repo.RESIDUAL_ORG,
                "org_path": repo.RESIDUAL_PATH,
                "weight": 1.0,
                "allocated_cents": charge["amount_cents"],
                "is_residual": 1,
                "note": "no org hierarchy available; unallocated residual",
            })
            continue

        weights, note = _weights_for_charge(charge, inputs, leaves, paths)
        allocation, rounding = largest_remainder(
            charge["amount_cents"], weights)
        for org_id in sorted(weights):
            lines.append({
                "charge_id": charge["charge_id"],
                "service_id": charge["service_id"],
                "org_unit_id": org_id,
                "org_path": paths[org_id],
                "weight": round(weights[org_id], 8),
                "allocated_cents": allocation[org_id],
                "is_residual": 0,
                "note": note + ("; +1c largest-remainder"
                                if any(a["org_unit_id"] == org_id for a in rounding)
                                else ""),
            })
        if rounding:
            audit.append({
                "charge_id": charge["charge_id"],
                "service_id": charge["service_id"],
                "amount_cents": charge["amount_cents"],
                "remainder_cents": len(rounding),
                "adjustments": rounding,
            })

    total = sum(ln["allocated_cents"] for ln in lines)
    expected = sum(c["amount_cents"] for c in inputs["charges"])
    assert total == expected, f"conservation broken: {total} != {expected}"
    return lines, audit

"""Fictional seed data: org tree, two months of charges/usage, rules.

All entities are invented. Money is entered in yuan and converted to cents
inside the pipeline. Run directly: python -m costpipe.seed --db data/costpipe.db
"""
from __future__ import annotations

import argparse

from . import repository as repo
from .db import connect, init_db

# Fictional org tree (seed version 1 of every unit):
#   ROOT 星云科技
#   ├─ ENG 研发中心
#   │  ├─ PLAT 平台工程组
#   │  └─ DATA 数据工程组
#   └─ SALES 销售中心
#      ├─ APAC 亚太销售组
#      └─ EMEA 欧洲销售组
ORG_SEED = [
    ("ROOT", "星云科技(虚拟)", None),
    ("ENG", "研发中心", "ROOT"),
    ("PLAT", "平台工程组", "ENG"),
    ("DATA", "数据工程组", "ENG"),
    ("SALES", "销售中心", "ROOT"),
    ("APAC", "亚太销售组", "SALES"),
    ("EMEA", "欧洲销售组", "SALES"),
]

CHARGES = [
    # 2026-10
    {"charge_id": "INV-202610-S3-01", "period": "2026-10", "service_id": "S3",
     "vendor": "虚构云厂A", "amount_cents": 120000},          # 1200.00 yuan
    {"charge_id": "INV-202610-EC2-01", "period": "2026-10", "service_id": "EC2",
     "vendor": "虚构云厂A", "amount_cents": 87533},           # 875.33
    {"charge_id": "INV-202610-SW-01", "period": "2026-10", "service_id": "SUPPORT",
     "vendor": "虚构支持商B", "amount_cents": 50000},         # 500.00
    # 2026-11
    {"charge_id": "INV-202611-S3-01", "period": "2026-11", "service_id": "S3",
     "vendor": "虚构云厂A", "amount_cents": 135678},          # 1356.78
    {"charge_id": "INV-202611-EC2-01", "period": "2026-11", "service_id": "EC2",
     "vendor": "虚构云厂A", "amount_cents": 90210},           # 902.10
    {"charge_id": "INV-202611-SW-01", "period": "2026-11", "service_id": "SUPPORT",
     "vendor": "虚构支持商B", "amount_cents": 50000},         # 500.00
]

# GB-hours / call counts by service x leaf org
USAGE = [
    ("UG-202610-S3-PLAT", "2026-10", "S3", "PLAT", 500),
    ("UG-202610-S3-DATA", "2026-10", "S3", "DATA", 300),
    ("UG-202610-S3-APAC", "2026-10", "S3", "APAC", 150),
    ("UG-202610-S3-EMEA", "2026-10", "S3", "EMEA", 50),
    ("UG-202610-EC2-PLAT", "2026-10", "EC2", "PLAT", 400),
    ("UG-202610-EC2-DATA", "2026-10", "EC2", "DATA", 350),
    ("UG-202610-EC2-APAC", "2026-10", "EC2", "APAC", 200),
    ("UG-202610-EC2-EMEA", "2026-10", "EC2", "EMEA", 50),
    # November: S3 usage shifts toward DATA; SUPPORT still has no usage meter
    ("UG-202611-S3-PLAT", "2026-11", "S3", "PLAT", 450),
    ("UG-202611-S3-DATA", "2026-11", "S3", "DATA", 420),
    ("UG-202611-S3-APAC", "2026-11", "S3", "APAC", 180),
    ("UG-202611-S3-EMEA", "2026-11", "S3", "EMEA", 70),
    ("UG-202611-EC2-PLAT", "2026-11", "EC2", "PLAT", 410),
    ("UG-202611-EC2-DATA", "2026-11", "EC2", "DATA", 360),
    ("UG-202611-EC2-APAC", "2026-11", "EC2", "APAC", 210),
    ("UG-202611-EC2-EMEA", "2026-11", "EC2", "EMEA", 60),
]

RULES = [
    # (rule_id, service, basis, fixed_weights, effective_from)
    ("rule_s3", "S3", "usage", None, "2026-01"),
    ("rule_ec2", "EC2", "usage", None, "2026-01"),
    # support fee has no usage meter -> equal split across leaf orgs
    ("rule_default", "*", "equal", None, "2026-01"),
]

# What-if scenario: fixed 60/30/10 engineering split for S3, never real money.
BUDGET_RULES = [
    ("budget_s3", "S3", "fixed",
     {"PLAT": 0.60, "DATA": 0.30, "APAC": 0.10}, "2026-01"),
]


def seed(db_path: str, reset: bool = False) -> None:
    if reset:
        for suffix in ("", "-wal", "-shm"):
            p = db_path + suffix
            from pathlib import Path
            Path(p).unlink(missing_ok=True)
    conn = connect(db_path)
    init_db(conn)
    repo.ensure_real_scenario(conn)

    for org_id, name, parent in ORG_SEED:
        if not conn.execute("SELECT 1 FROM org_unit WHERE org_unit_id = ?",
                            (org_id,)).fetchone():
            repo.seed_org_unit(conn, org_id, name, parent)

    for c in CHARGES:
        repo.insert_charge(conn, c)
    for usage_id, period, service, org, qty in USAGE:
        repo.insert_usage(conn, {
            "usage_id": usage_id, "period": period, "service_id": service,
            "org_unit_id": org, "usage_qty": qty})

    existing_rules = {r["rule_id"] for r in repo.list_rules(conn)}
    for rule_id, service, basis, weights, eff in RULES:
        if rule_id not in existing_rules:
            repo.create_rule_version(
                conn, rule_id, repo.REAL_SCENARIO, service, basis, weights, eff)

    if not conn.execute("SELECT 1 FROM scenario WHERE scenario_id = 'budget2027'"
                        ).fetchone():
        repo.create_scenario(conn, "budget2027", "2027预算情景（假设固定比例）")
        for rule_id, service, basis, weights, eff in BUDGET_RULES:
            repo.create_rule_version(
                conn, rule_id, "budget2027", service, basis, weights, eff)
        # budget scenario inherits the equal default rule via '*' on actual?
        # rules are scenario-scoped, so give it its own default + ec2 rule.
        repo.create_rule_version(conn, "budget_ec2", "budget2027", "EC2",
                                 "usage", None, "2026-01")
        repo.create_rule_version(conn, "budget_default", "budget2027", "*",
                                 "equal", None, "2026-01")

    # Enqueue initial recomputation for every period x scenario.
    for period in repo.list_periods(conn):
        for s in repo.list_scenarios(conn):
            repo.enqueue_job(conn, period, s["scenario_id"], "seed")
    conn.commit()
    print(f"[seed] wrote {len(CHARGES)} charges, {len(USAGE)} usage rows, "
          f"{len(ORG_SEED)} org units to {db_path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="seed fictional costpipe data")
    ap.add_argument("--db", default="data/costpipe.db")
    ap.add_argument("--reset", action="store_true",
                    help="delete the database files before seeding")
    args = ap.parse_args()
    seed(args.db, reset=args.reset)


if __name__ == "__main__":
    main()

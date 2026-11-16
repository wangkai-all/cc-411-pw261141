"""Data-access layer: every SQL statement in the pipeline lives here."""
from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta

from .util import canonical_json, fingerprint, period_to_end, rows_to_dicts, utcnow_iso

OPEN_ENDED = "9999-12-31"
REAL_SCENARIO = "actual"
RESIDUAL_ORG = "__rounding__"
RESIDUAL_PATH = "ROUNDING-RESIDUAL (largest-remainder bucket)"


# --------------------------------------------------------------------------- #
# scenarios
# --------------------------------------------------------------------------- #
def ensure_real_scenario(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO scenario(scenario_id, name, is_real, created_at) "
        "VALUES (?, ?, 1, ?)",
        (REAL_SCENARIO, "真实费用（实际账单）", utcnow_iso()),
    )
    conn.commit()


def create_scenario(conn, scenario_id: str, name: str) -> dict:
    conn.execute(
        "INSERT INTO scenario(scenario_id, name, is_real, created_at) "
        "VALUES (?, ?, 0, ?)",
        (scenario_id, name, utcnow_iso()),
    )
    return {"scenario_id": scenario_id, "name": name, "is_real": 0}


def list_scenarios(conn) -> list[dict]:
    return rows_to_dicts(
        conn.execute("SELECT * FROM scenario ORDER BY is_real DESC, scenario_id")
    )


# --------------------------------------------------------------------------- #
# raw inputs: charges and usage
# --------------------------------------------------------------------------- #
def insert_charge(conn, c: dict) -> bool:
    """Insert a charge. Returns True if new, False if duplicated."""
    cur = conn.execute(
        "INSERT OR IGNORE INTO charge(charge_id, period, service_id, vendor, "
        "amount_cents, currency, received_at) VALUES (?,?,?,?,?,?,?)",
        (
            c["charge_id"], c["period"], c["service_id"], c["vendor"],
            c["amount_cents"], c.get("currency", "CNY"), utcnow_iso(),
        ),
    )
    return cur.rowcount > 0


def insert_usage(conn, u: dict) -> bool:
    cur = conn.execute(
        "INSERT OR IGNORE INTO usage_record(usage_id, period, service_id, "
        "org_unit_id, usage_qty, received_at) VALUES (?,?,?,?,?,?)",
        (
            u["usage_id"], u["period"], u["service_id"], u["org_unit_id"],
            float(u["usage_qty"]), utcnow_iso(),
        ),
    )
    return cur.rowcount > 0


def list_charges(conn, period: str | None = None) -> list[dict]:
    sql = "SELECT * FROM charge"
    args: tuple = ()
    if period:
        sql += " WHERE period = ?"
        args = (period,)
    sql += " ORDER BY period, charge_id"
    return rows_to_dicts(conn.execute(sql, args))


def list_usage(conn, period: str | None = None) -> list[dict]:
    sql = "SELECT * FROM usage_record"
    args: tuple = ()
    if period:
        sql += " WHERE period = ?"
        args = (period,)
    sql += " ORDER BY period, service_id, org_unit_id"
    return rows_to_dicts(conn.execute(sql, args))


def list_periods(conn) -> list[str]:
    return [r["period"] for r in conn.execute(
        "SELECT DISTINCT period FROM charge ORDER BY period"
    )]


# --------------------------------------------------------------------------- #
# organization (SCD2)
# --------------------------------------------------------------------------- #
def seed_org_unit(conn, org_unit_id, name, parent_id, valid_from="2026-01-01"):
    conn.execute(
        "INSERT INTO org_unit(org_unit_id, version, name, parent_id, valid_from, "
        "valid_to, changed_at, change_reason) VALUES (?,1,?,?,?,'9999-12-31',?,?)",
        (org_unit_id, name, parent_id, valid_from, utcnow_iso(), "initial import"),
    )


def list_org_units(conn) -> list[dict]:
    return rows_to_dicts(conn.execute(
        "SELECT * FROM org_unit ORDER BY org_unit_id, version"
    ))


def org_snapshot(conn, on_date: str) -> dict[str, dict]:
    """Org hierarchy as effective on_date: org_unit_id -> row."""
    rows = conn.execute(
        "SELECT * FROM org_unit WHERE valid_from <= ? AND valid_to >= ?",
        (on_date, on_date),
    )
    return {r["org_unit_id"]: dict(r) for r in rows}


def apply_org_correction(conn, org_unit_id: str, new_name: str | None,
                         new_parent_id: str | None, valid_from: str,
                         reason: str) -> dict:
    """SCD2 correction: close the open row, insert a new version.

    The old version keeps its validity window and every alloc_line it produced,
    so retroactive recomputation can show what the old structure contributed.
    """
    current = conn.execute(
        "SELECT * FROM org_unit WHERE org_unit_id = ? AND valid_to = ?",
        (org_unit_id, OPEN_ENDED),
    ).fetchone()
    if current is None:
        raise ValueError(f"unknown org_unit_id {org_unit_id!r} (no open version)")
    current = dict(current)

    if new_parent_id is not None and new_parent_id != org_unit_id:
        if conn.execute(
            "SELECT 1 FROM org_unit WHERE org_unit_id = ? AND valid_to = ?",
            (new_parent_id, OPEN_ENDED),
        ).fetchone() is None:
            raise ValueError(f"new parent {new_parent_id!r} does not exist")

    close_date = (date.fromisoformat(valid_from) - timedelta(days=1)).isoformat()
    conn.execute(
        "UPDATE org_unit SET valid_to = ? WHERE id = ?",
        (close_date, current["id"]),
    )
    next_version = current["version"] + 1
    conn.execute(
        "INSERT INTO org_unit(org_unit_id, version, name, parent_id, valid_from, "
        "valid_to, changed_at, change_reason) VALUES (?,?,?,?,?,'9999-12-31',?,?)",
        (
            org_unit_id, next_version,
            new_name if new_name is not None else current["name"],
            new_parent_id if new_parent_id is not None else current["parent_id"],
            valid_from, utcnow_iso(), reason,
        ),
    )
    return {
        "org_unit_id": org_unit_id,
        "old_version": current["version"],
        "new_version": next_version,
        "old": {"name": current["name"], "parent_id": current["parent_id"]},
        "new": {
            "name": new_name if new_name is not None else current["name"],
            "parent_id": (new_parent_id if new_parent_id is not None
                          else current["parent_id"]),
        },
        "valid_from": valid_from,
    }


# --------------------------------------------------------------------------- #
# allocation rules (versioned)
# --------------------------------------------------------------------------- #
def create_rule_version(conn, rule_id: str | None, scenario_id: str,
                        service_id: str, basis: str, fixed_weights: dict | None,
                        effective_from: str) -> dict:
    if basis not in ("usage", "equal", "fixed"):
        raise ValueError("basis must be one of usage|equal|fixed")
    if basis == "fixed" and not fixed_weights:
        raise ValueError("fixed basis requires fixed_weights {org_unit_id: weight}")
    if rule_id is None:
        rule_id = f"rule_{service_id.replace('*', 'default')}"
    row = conn.execute(
        "SELECT COALESCE(MAX(version), 0) AS v FROM rule WHERE rule_id = ?",
        (rule_id,),
    ).fetchone()
    version = row["v"] + 1
    conn.execute(
        "INSERT INTO rule(rule_id, version, scenario_id, service_id, basis, "
        "fixed_weights_json, effective_from, created_at, active) "
        "VALUES (?,?,?,?,?,?,?,?,1)",
        (rule_id, version, scenario_id, service_id, basis,
         json.dumps(fixed_weights, sort_keys=True) if fixed_weights else None,
         effective_from, utcnow_iso()),
    )
    return {
        "rule_id": rule_id, "version": version, "scenario_id": scenario_id,
        "service_id": service_id, "basis": basis,
        "fixed_weights": fixed_weights, "effective_from": effective_from,
    }


def list_rules(conn) -> list[dict]:
    out = []
    for r in conn.execute("SELECT * FROM rule ORDER BY rule_id, version"):
        d = dict(r)
        d["fixed_weights"] = json.loads(d.pop("fixed_weights_json")) if d["fixed_weights_json"] else None
        out.append(d)
    return out


def get_active_rule(conn, scenario_id: str, service_id: str, period: str) -> dict | None:
    """Most specific active rule effective for the period.

    Exact service match beats the '*' default; newer effective_from wins.
    """
    rows = conn.execute(
        "SELECT * FROM rule WHERE active = 1 AND scenario_id = ? "
        "AND service_id IN (?, '*') AND effective_from <= ? "
        "ORDER BY effective_from DESC",
        (scenario_id, service_id, period_to_end(period)),
    ).fetchall()
    exact = [r for r in rows if r["service_id"] == service_id]
    picked = (exact or rows)
    if not picked:
        return None
    r = dict(picked[0])
    r["fixed_weights"] = json.loads(r.pop("fixed_weights_json")) if r["fixed_weights_json"] else None
    return r


# --------------------------------------------------------------------------- #
# jobs
# --------------------------------------------------------------------------- #
def enqueue_job(conn, period: str, scenario_id: str, trigger_event: str,
                detail: dict | None = None) -> int:
    # Coalesce: an identical queued job makes a new one redundant.
    existing = conn.execute(
        "SELECT id FROM job WHERE period = ? AND scenario_id = ? AND status = 'queued'",
        (period, scenario_id),
    ).fetchone()
    if existing:
        return existing["id"]
    cur = conn.execute(
        "INSERT INTO job(period, scenario_id, trigger_event, status, detail, created_at) "
        "VALUES (?,?,?,'queued',?,?)",
        (period, scenario_id, trigger_event,
         json.dumps(detail, ensure_ascii=False) if detail else None, utcnow_iso()),
    )
    return int(cur.lastrowid)


def lease_job(conn) -> sqlite3.Row | None:
    row = conn.execute(
        "SELECT * FROM job WHERE status = 'queued' ORDER BY id LIMIT 1"
    ).fetchone()
    if row is None:
        return None
    conn.execute(
        "UPDATE job SET status = 'running', started_at = ? WHERE id = ? AND status = 'queued'",
        (utcnow_iso(), row["id"]),
    )
    return row


def finish_job(conn, job_id: int, status: str, detail: dict) -> None:
    conn.execute(
        "UPDATE job SET status = ?, detail = ?, finished_at = ? WHERE id = ?",
        (status, json.dumps(detail, ensure_ascii=False), utcnow_iso(), job_id),
    )


def list_jobs(conn, limit: int = 50) -> list[dict]:
    return rows_to_dicts(conn.execute(
        "SELECT * FROM job ORDER BY id DESC LIMIT ?", (limit,)
    ))


# --------------------------------------------------------------------------- #
# event stream and consumer offsets
# --------------------------------------------------------------------------- #
def publish_event(conn, event_type: str, payload: dict) -> int:
    cur = conn.execute(
        "INSERT INTO publication_log(event_type, payload_json, created_at) "
        "VALUES (?,?,?)",
        (event_type, canonical_json(payload), utcnow_iso()),
    )
    return int(cur.lastrowid)


def read_events(conn, after_seq: int, limit: int = 100) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM publication_log WHERE seq > ? ORDER BY seq LIMIT ?",
        (after_seq, limit),
    ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["payload"] = json.loads(d.pop("payload_json"))
        out.append(d)
    return out


def get_consumer(conn, consumer_id: str) -> dict:
    row = conn.execute(
        "SELECT * FROM consumer_offset WHERE consumer_id = ?", (consumer_id,)
    ).fetchone()
    if row is None:
        return {"consumer_id": consumer_id, "last_seq": 0}
    return dict(row)


def ack_consumer(conn, consumer_id: str, seq: int) -> None:
    conn.execute(
        "INSERT INTO consumer_offset(consumer_id, last_seq, updated_at) "
        "VALUES (?,?,?) ON CONFLICT(consumer_id) DO UPDATE SET "
        "last_seq = excluded.last_seq, updated_at = excluded.updated_at",
        (consumer_id, seq, utcnow_iso()),
    )


# --------------------------------------------------------------------------- #
# allocation inputs / outputs
# --------------------------------------------------------------------------- #
def allocation_inputs(conn, period: str, scenario_id: str) -> dict:
    """Snapshot of every input that determines an allocation result."""
    end = period_to_end(period)
    charges = rows_to_dicts(conn.execute(
        "SELECT charge_id, service_id, vendor, amount_cents FROM charge "
        "WHERE period = ? ORDER BY charge_id", (period,)
    ))
    usage = rows_to_dicts(conn.execute(
        "SELECT service_id, org_unit_id, usage_qty FROM usage_record "
        "WHERE period = ? ORDER BY service_id, org_unit_id", (period,)
    ))
    org = [
        {k: r[k] for k in ("org_unit_id", "version", "name", "parent_id",
                           "valid_from", "valid_to")}
        for r in conn.execute(
            "SELECT * FROM org_unit WHERE valid_from <= ? AND valid_to >= ? "
            "ORDER BY org_unit_id", (end, end))
    ]
    rules = []
    for r in conn.execute(
        "SELECT rule_id, version, scenario_id, service_id, basis, "
        "fixed_weights_json, effective_from FROM rule "
        "WHERE active = 1 AND scenario_id = ? AND effective_from <= ? "
        "ORDER BY rule_id, version",
        (scenario_id, end),
    ):
        d = dict(r)
        d["fixed_weights"] = json.loads(d.pop("fixed_weights_json")) if d["fixed_weights_json"] else None
        rules.append(d)
    return {"period": period, "scenario_id": scenario_id,
            "charges": charges, "usage": usage, "org": org, "rules": rules}


def input_fingerprint(inputs: dict) -> str:
    return fingerprint([
        {"period": inputs["period"], "scenario_id": inputs["scenario_id"]},
        inputs["charges"], inputs["usage"], inputs["org"], inputs["rules"],
    ])


def current_alloc_version(conn, period: str, scenario_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM alloc_version WHERE period = ? AND scenario_id = ? "
        "AND status = 'current'", (period, scenario_id)
    ).fetchone()
    return dict(row) if row else None


def get_alloc_version(conn, version_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM alloc_version WHERE id = ?", (version_id,)
    ).fetchone()
    return dict(row) if row else None


def list_alloc_versions(conn, period: str | None = None,
                        scenario_id: str | None = None) -> list[dict]:
    sql = "SELECT * FROM alloc_version WHERE 1=1"
    args: list = []
    if period:
        sql += " AND period = ?"
        args.append(period)
    if scenario_id:
        sql += " AND scenario_id = ?"
        args.append(scenario_id)
    sql += " ORDER BY id DESC"
    return rows_to_dicts(conn.execute(sql, args))


def list_alloc_lines(conn, alloc_version_id: int) -> list[dict]:
    return rows_to_dicts(conn.execute(
        "SELECT * FROM alloc_line WHERE alloc_version_id = ? "
        "ORDER BY charge_id, is_residual, org_unit_id",
        (alloc_version_id,),
    ))


def save_allocation(conn, period: str, scenario_id: str, fp: str,
                    total_cents: int, lines: list[dict],
                    trigger: str) -> dict:
    """Insert an immutable alloc_version + lines, superseding the prior one."""
    previous = current_alloc_version(conn, period, scenario_id)
    prev_id = previous["id"] if previous else None
    next_no = (previous["version_no"] + 1) if previous else 1
    if previous:
        conn.execute(
            "UPDATE alloc_version SET status = 'superseded' WHERE id = ?",
            (prev_id,),
        )
    cur = conn.execute(
        "INSERT INTO alloc_version(period, scenario_id, version_no, supersedes_id, "
        "input_fingerprint, total_cents, status, created_at) "
        "VALUES (?,?,?,?,?,?,'current',?)",
        (period, scenario_id, next_no, prev_id, fp, total_cents, utcnow_iso()),
    )
    version_id = int(cur.lastrowid)
    conn.executemany(
        "INSERT INTO alloc_line(alloc_version_id, charge_id, service_id, "
        "org_unit_id, org_path, weight, allocated_cents, is_residual, note) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        [
            (version_id, ln["charge_id"], ln["service_id"], ln["org_unit_id"],
             ln["org_path"], ln["weight"], ln["allocated_cents"],
             ln.get("is_residual", 0), ln.get("note"))
            for ln in lines
        ],
    )
    return {"alloc_version_id": version_id, "version_no": next_no,
            "supersedes_id": prev_id}


def org_totals(conn, alloc_version_id: int) -> dict[str, int]:
    rows = conn.execute(
        "SELECT org_unit_id, SUM(allocated_cents) AS c FROM alloc_line "
        "WHERE alloc_version_id = ? GROUP BY org_unit_id",
        (alloc_version_id,),
    )
    return {r["org_unit_id"]: r["c"] for r in rows}


def trace_charge(conn, charge_id: str, scenario_id: str) -> dict | None:
    """Every line of the current version that a single charge produced."""
    charge = conn.execute(
        "SELECT * FROM charge WHERE charge_id = ?", (charge_id,)
    ).fetchone()
    if charge is None:
        return None
    ver = conn.execute(
        "SELECT * FROM alloc_version WHERE period = ? AND scenario_id = ? "
        "AND status = 'current'",
        (charge["period"], scenario_id),
    ).fetchone()
    if ver is None:
        return {"charge": dict(charge), "alloc_version": None, "lines": []}
    lines = rows_to_dicts(conn.execute(
        "SELECT * FROM alloc_line WHERE alloc_version_id = ? AND charge_id = ? "
        "ORDER BY is_residual, org_unit_id",
        (ver["id"], charge_id),
    ))
    return {"charge": dict(charge), "alloc_version": dict(ver), "lines": lines}

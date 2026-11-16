"""Allocation worker: an independent process that drains the job queue.

Each job recomputes one (period, scenario). Results are written as immutable
versions; subscribers receive an allocation.published event containing the
full org-level diff against the previous version, so an offline consumer can
catch up every historical contribution rather than only the new total name.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time

from . import repository as repo
from .db import connect, init_db
from .engine import allocate_period
from .util import rows_to_dicts


def process_job(conn: sqlite3.Connection, job: sqlite3.Row,
                force: bool = False) -> dict:
    period, scenario_id = job["period"], job["scenario_id"]
    inputs = repo.allocation_inputs(conn, period, scenario_id)
    fp = repo.input_fingerprint(inputs)

    current = repo.current_alloc_version(conn, period, scenario_id)
    if current and current["input_fingerprint"] == fp and not force:
        result = {"skipped": True, "reason": "unchanged input fingerprint",
                  "alloc_version_id": current["id"]}
        repo.finish_job(conn, job["id"], "done", result)
        conn.commit()
        return result

    lines, audit = allocate_period(inputs)
    total = sum(ln["allocated_cents"] for ln in lines)
    saved = repo.save_allocation(conn, period, scenario_id, fp, total, lines,
                                 trigger=job["trigger_event"])

    new_totals = repo.org_totals(conn, saved["alloc_version_id"])
    old_totals = (repo.org_totals(conn, saved["supersedes_id"])
                  if saved["supersedes_id"] else {})

    all_orgs = sorted(set(new_totals) | set(old_totals))
    diff = {
        org: {
            "old_cents": old_totals.get(org, 0) if old_totals else None,
            "new_cents": new_totals.get(org, 0),
            "delta_cents": new_totals.get(org, 0) - old_totals.get(org, 0),
        }
        for org in all_orgs
    }
    scenario = conn.execute(
        "SELECT * FROM scenario WHERE scenario_id = ?", (scenario_id,)
    ).fetchone()

    payload = {
        "event_type": "allocation.published",
        "period": period,
        "scenario_id": scenario_id,
        "is_real": bool(scenario["is_real"]) if scenario else None,
        "alloc_version_id": saved["alloc_version_id"],
        "version_no": saved["version_no"],
        "supersedes_id": saved["supersedes_id"],
        "total_cents": total,
        "org_totals_cents": new_totals,
        "diff_from_previous": diff,
        "rounding_audit": audit,
        "input_fingerprint": fp,
        "trigger_event": job["trigger_event"],
        "line_count": len(lines),
    }
    seq = repo.publish_event(conn, "allocation.published", payload)
    result = {"skipped": False, "seq": seq, **saved, "total_cents": total,
              "line_count": len(lines),
              "rounding_adjustment_cents": sum(a["remainder_cents"] for a in audit)}
    repo.finish_job(conn, job["id"], "done", result)
    conn.commit()
    return result


def process_all(conn: sqlite3.Connection, force: bool = False) -> list[dict]:
    results = []
    while True:
        job = repo.lease_job(conn)
        if job is None:
            break
        try:
            results.append(process_job(conn, job, force=force))
        except Exception as exc:  # a bad job must not kill the worker loop
            conn.rollback()
            repo.finish_job(conn, job["id"], "failed", {"error": str(exc)})
            conn.commit()
            results.append({"job_id": job["id"], "failed": str(exc)})
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="costpipe allocation worker")
    ap.add_argument("--db", default="data/costpipe.db")
    ap.add_argument("--once", action="store_true",
                    help="drain the queue once and exit (default: poll forever)")
    ap.add_argument("--poll-seconds", type=float, default=1.0)
    args = ap.parse_args()

    conn = connect(args.db)
    init_db(conn)
    repo.ensure_real_scenario(conn)
    while True:
        results = process_all(conn)
        for r in results:
            print(json.dumps(r, ensure_ascii=False))
        if args.once:
            break
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()

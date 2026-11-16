"""Ingest service: the only writer-facing process (HTTP API + web console).

It accepts the three independently updated inputs (charges, usage, org
relations) plus versioned rules, appends every change to the publication log,
and enqueues allocation jobs for the worker. The worker and subscriber never
write here.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import repository as repo
from .db import connect, init_db
from .util import to_cents, utcnow_iso

PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
WEB_DIR = Path(__file__).resolve().parent.parent / "web"


# --------------------------------------------------------------------------- #
# domain actions (also directly unit-testable without HTTP)
# --------------------------------------------------------------------------- #
def _all_periods(conn: sqlite3.Connection) -> list[str]:
    return repo.list_periods(conn)


def _all_scenarios(conn: sqlite3.Connection) -> list[str]:
    return [s["scenario_id"] for s in repo.list_scenarios(conn)]


def ingest_charges(conn: sqlite3.Connection, items: list[dict]) -> dict:
    new_ids, dup_ids = [], []
    periods: set[str] = set()
    for c in items:
        if not PERIOD_RE.match(c["period"]):
            raise ValueError(f"bad period {c.get('period')!r}, want YYYY-MM")
        c = dict(c)
        c["amount_cents"] = to_cents(c["amount_cents"])
        if c["amount_cents"] <= 0:
            raise ValueError("charge amount must be positive")
        if repo.insert_charge(conn, c):
            new_ids.append(c["charge_id"])
            periods.add(c["period"])
        else:
            dup_ids.append(c["charge_id"])
    if new_ids:
        repo.publish_event(conn, "charge.received",
                           {"charge_ids": new_ids, "periods": sorted(periods)})
        for period in periods:
            for sid in _all_scenarios(conn):
                repo.enqueue_job(conn, period, sid, "charge.received")
    conn.commit()
    return {"inserted": new_ids, "duplicated": dup_ids}


def ingest_usage(conn: sqlite3.Connection, items: list[dict]) -> dict:
    new_ids, dup_ids = [], []
    periods: set[str] = set()
    for u in items:
        if not PERIOD_RE.match(u["period"]):
            raise ValueError(f"bad period {u.get('period')!r}, want YYYY-MM")
        if float(u["usage_qty"]) < 0:
            raise ValueError("usage_qty must be >= 0")
        if repo.insert_usage(conn, u):
            new_ids.append(u["usage_id"])
            periods.add(u["period"])
        else:
            dup_ids.append(u["usage_id"])
    if new_ids:
        repo.publish_event(conn, "usage.received",
                           {"usage_ids": new_ids, "periods": sorted(periods)})
        for period in periods:
            for sid in _all_scenarios(conn):
                repo.enqueue_job(conn, period, sid, "usage.received")
    conn.commit()
    return {"inserted": new_ids, "duplicated": dup_ids}


def correct_org(conn: sqlite3.Connection, body: dict) -> dict:
    result = repo.apply_org_correction(
        conn,
        org_unit_id=body["org_unit_id"],
        new_name=body.get("new_name"),
        new_parent_id=body.get("new_parent_id"),
        valid_from=body["valid_from"],
        reason=body.get("reason", "manual correction"),
    )
    # Retroactive: every historical period must be re-derived under the new
    # structure. Renaming the summary alone is explicitly not enough.
    affected = _all_periods(conn)
    repo.publish_event(conn, "org.corrected",
                       {"correction": result, "affected_periods": affected})
    for period in affected:
        for sid in _all_scenarios(conn):
            repo.enqueue_job(conn, period, sid, "org.corrected",
                             detail={"org_unit_id": body["org_unit_id"]})
    conn.commit()
    return {**result, "affected_periods": affected}


def add_rule(conn: sqlite3.Connection, body: dict) -> dict:
    if not PERIOD_RE.match(body["effective_from"]):
        raise ValueError("effective_from must be YYYY-MM")
    result = repo.create_rule_version(
        conn,
        rule_id=body.get("rule_id"),
        scenario_id=body["scenario_id"],
        service_id=body["service_id"],
        basis=body["basis"],
        fixed_weights=body.get("fixed_weights"),
        effective_from=body["effective_from"],
    )
    repo.publish_event(conn, "rule.created", {"rule": result})
    for period in _all_periods(conn):
        # rule can only change periods on/after its effective month
        if period >= body["effective_from"]:
            repo.enqueue_job(conn, period, body["scenario_id"], "rule.created",
                             detail={"rule_id": result["rule_id"],
                                     "version": result["version"]})
    conn.commit()
    return result


def add_scenario(conn: sqlite3.Connection, body: dict) -> dict:
    if body["scenario_id"] == repo.RESIDUAL_ORG:
        raise ValueError("reserved scenario id")
    result = repo.create_scenario(conn, body["scenario_id"], body["name"])
    repo.publish_event(conn, "scenario.created", result)
    for period in _all_periods(conn):
        repo.enqueue_job(conn, period, body["scenario_id"], "scenario.created")
    conn.commit()
    return result


def request_recompute(conn: sqlite3.Connection, body: dict, force: bool) -> dict:
    scenarios = ([body["scenario_id"]] if body.get("scenario_id")
                 else _all_scenarios(conn))
    periods = [body["period"]] if body.get("period") else _all_periods(conn)
    queued = []
    for period in periods:
        for sid in scenarios:
            jid = repo.enqueue_job(
                conn, period, sid,
                "manual.force" if force else "manual.recompute")
            queued.append({"job_id": jid, "period": period, "scenario_id": sid})
    conn.commit()
    return {"queued": queued, "force": force}


def dashboard_summary(conn: sqlite3.Connection) -> dict:
    versions = repo.list_alloc_versions(conn)
    current = [v for v in versions if v["status"] == "current"]
    out = []
    for v in current:
        totals = repo.org_totals(conn, v["id"])
        out.append({**v, "org_totals_cents": totals})
    return {
        "scenarios": repo.list_scenarios(conn),
        "periods": repo.list_periods(conn),
        "current_allocations": sorted(out, key=lambda v: (v["period"], v["scenario_id"])),
        "superseded_count": len(versions) - len(current),
        "queued_jobs": conn.execute(
            "SELECT COUNT(*) c FROM job WHERE status = 'queued'").fetchone()["c"],
        "max_event_seq": conn.execute(
            "SELECT COALESCE(MAX(seq), 0) m FROM publication_log").fetchone()["m"],
        "consumers": [dict(r) for r in conn.execute(
            "SELECT * FROM consumer_offset ORDER BY consumer_id")],
    }


# --------------------------------------------------------------------------- #
# HTTP layer
# --------------------------------------------------------------------------- #
class Handler(BaseHTTPRequestHandler):
    server_version = "costpipe/1.0"

    def log_message(self, fmt, *args):  # quieter logs
        print(f"[ingest] {self.address_string()} {fmt % args}")

    @property
    def conn(self) -> sqlite3.Connection:
        return self.server.db_conn

    def _send(self, code: int, obj):
        body = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict | list:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _as_list(self, body) -> list[dict]:
        return body if isinstance(body, list) else [body]

    # ---------------------------------------------------------- routing GET
    def do_GET(self):
        parsed = urlparse(self.path)
        path, qs = parsed.path, parse_qs(parsed.query)
        try:
            if path == "/health":
                return self._send(200, {"ok": True, "time": utcnow_iso()})
            if path == "/" or path == "/index.html":
                return self._serve_file("index.html", "text/html; charset=utf-8")
            if path == "/api/summary":
                return self._send(200, dashboard_summary(self.conn))
            if path == "/api/periods":
                return self._send(200, repo.list_periods(self.conn))
            if path == "/api/charges":
                return self._send(200, repo.list_charges(
                    self.conn, qs.get("period", [None])[0]))
            if path == "/api/usage":
                return self._send(200, repo.list_usage(
                    self.conn, qs.get("period", [None])[0]))
            if path == "/api/org":
                return self._send(200, repo.list_org_units(self.conn))
            if path == "/api/rules":
                return self._send(200, repo.list_rules(self.conn))
            if path == "/api/scenarios":
                return self._send(200, repo.list_scenarios(self.conn))
            if path == "/api/jobs":
                return self._send(200, repo.list_jobs(self.conn))
            if path == "/api/allocations":
                return self._send(200, repo.list_alloc_versions(
                    self.conn, qs.get("period", [None])[0],
                    qs.get("scenario", [None])[0]))
            if path.startswith("/api/allocations/versions/") and path.endswith("/lines"):
                vid = int(path.split("/")[4])
                ver = repo.get_alloc_version(self.conn, vid)
                if ver is None:
                    return self._send(404, {"error": "unknown version"})
                return self._send(200, {
                    "version": ver, "lines": repo.list_alloc_lines(self.conn, vid)})
            if path.startswith("/api/trace/"):
                charge_id = path.split("/")[-1]
                scenario = qs.get("scenario", [repo.REAL_SCENARIO])[0]
                result = repo.trace_charge(self.conn, charge_id, scenario)
                if result is None:
                    return self._send(404, {"error": "unknown charge_id"})
                return self._send(200, result)
            if path == "/api/events":
                after = int(qs.get("after", ["0"])[0])
                limit = int(qs.get("limit", ["100"])[0])
                return self._send(200, repo.read_events(self.conn, after, limit))
            if path == "/api/consumers":
                return self._send(200, [dict(r) for r in self.conn.execute(
                    "SELECT * FROM consumer_offset ORDER BY consumer_id")])
            return self._send(404, {"error": f"no route for {path}"})
        except Exception as exc:
            return self._send(400, {"error": str(exc)})

    # --------------------------------------------------------- routing POST
    def do_POST(self):
        path = urlparse(self.path).path
        try:
            body = self._read_body()
            if path == "/api/charges":
                return self._send(200, ingest_charges(self.conn, self._as_list(body)))
            if path == "/api/usage":
                return self._send(200, ingest_usage(self.conn, self._as_list(body)))
            if path == "/api/org/correction":
                return self._send(200, correct_org(self.conn, body))
            if path == "/api/rules":
                return self._send(200, add_rule(self.conn, body))
            if path == "/api/scenarios":
                return self._send(200, add_scenario(self.conn, body))
            if path == "/api/recompute":
                return self._send(200, request_recompute(
                    self.conn, body if isinstance(body, dict) else {},
                    force=bool(isinstance(body, dict) and body.get("force"))))
            if path.startswith("/api/consumers/") and path.endswith("/ack"):
                consumer_id = path.split("/")[3]
                repo.ack_consumer(self.conn, consumer_id, int(body["seq"]))
                self.conn.commit()
                return self._send(200, {"consumer_id": consumer_id,
                                        "last_seq": int(body["seq"])})
            return self._send(404, {"error": f"no route for {path}"})
        except sqlite3.IntegrityError as exc:
            return self._send(409, {"error": str(exc)})
        except (ValueError, KeyError, json.JSONDecodeError) as exc:
            return self._send(400, {"error": str(exc)})

    def _serve_file(self, name: str, content_type: str):
        f = WEB_DIR / name
        if not f.exists():
            return self._send(404, {"error": "web asset missing"})
        data = f.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def build_server(db_path: str, host: str, port: int) -> ThreadingHTTPServer:
    conn = connect(db_path)
    init_db(conn)
    repo.ensure_real_scenario(conn)
    server = ThreadingHTTPServer((host, port), Handler)
    server.db_conn = conn
    return server


def main() -> None:
    ap = argparse.ArgumentParser(description="costpipe ingest service")
    ap.add_argument("--db", default="data/costpipe.db")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8791)
    args = ap.parse_args()
    server = build_server(args.db, args.host, args.port)
    print(f"[ingest] listening on http://{args.host}:{args.port} "
          f"(db={args.db})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()

"""Report subscriber: an independent consumer of the publication event stream.

Talks to the ingest service over HTTP (never opens the writer's database),
keeps its own materialized SQLite store and a durable offset, and verifies
amount conservation on every allocation event. If the service is unreachable
it retries with backoff — that is the "offline" path: on reconnect it simply
continues from last_seq and receives every version it missed, including
superseded ones, so retroactive corrections are replayed in order.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
import urllib.error
import urllib.request
from pathlib import Path

MATERIALIZED_SCHEMA = """
CREATE TABLE IF NOT EXISTS received_event (
    seq INTEGER PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    received_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS materialized_report (
    period TEXT, scenario_id TEXT, org_unit_id TEXT,
    alloc_version_id INTEGER, version_no INTEGER,
    allocated_cents INTEGER, is_latest INTEGER,
    PRIMARY KEY (period, scenario_id, org_unit_id, alloc_version_id)
);
CREATE TABLE IF NOT EXISTS conservation_check (
    seq INTEGER PRIMARY KEY,
    period TEXT, scenario_id TEXT,
    charge_total_cents INTEGER, line_total_cents INTEGER, ok INTEGER,
    detail TEXT
);
"""


class Subscriber:
    def __init__(self, base_url: str, consumer_id: str, store_path: str):
        self.base_url = base_url.rstrip("/")
        self.consumer_id = consumer_id
        Path(store_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(store_path)
        self.conn.executescript(MATERIALIZED_SCHEMA)
        self.conn.commit()
        self.last_seq = self.conn.execute(
            "SELECT COALESCE(MAX(seq), 0) FROM received_event"
        ).fetchone()[0]

    # ------------------------------------------------------------------ HTTP
    def _get(self, path: str, retries: int = 5):
        url = f"{self.base_url}{path}"
        last_err = None
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    return json.loads(resp.read().decode("utf-8"))
            except (urllib.error.URLError, ConnectionError, OSError) as exc:
                last_err = exc
                time.sleep(min(2 ** attempt * 0.2, 2.0))
        raise ConnectionError(f"cannot reach {url}: {last_err}")

    def _post(self, path: str, body: dict):
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base_url}{path}", data=data,
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read().decode("utf-8"))

    # --------------------------------------------------------------- process
    def poll_once(self) -> list[dict]:
        events = self._get(f"/api/events?after={self.last_seq}&limit=100")
        processed = []
        for ev in events:
            self._handle(ev)
            self.last_seq = ev["seq"]
            processed.append(ev)
        if processed:
            self._post(f"/api/consumers/{self.consumer_id}/ack",
                       {"seq": self.last_seq})
        return processed

    def _handle(self, ev: dict) -> None:
        p = ev["payload"]
        self.conn.execute(
            "INSERT OR IGNORE INTO received_event(seq, event_type, payload_json, "
            "received_at) VALUES (?,?,?,?)",
            (ev["seq"], ev["event_type"], json.dumps(p, ensure_ascii=False),
             ev["created_at"]),
        )

        if ev["event_type"] == "allocation.published":
            self._materialize(p)
            self._check_conservation(p)
        self.conn.commit()

    def _materialize(self, p: dict) -> None:
        # Mark prior rows of this (period, scenario) as history, keep them.
        self.conn.execute(
            "UPDATE materialized_report SET is_latest = 0 "
            "WHERE period = ? AND scenario_id = ?",
            (p["period"], p["scenario_id"]),
        )
        for org_id, cents in p["org_totals_cents"].items():
            self.conn.execute(
                "INSERT OR REPLACE INTO materialized_report(period, scenario_id, "
                "org_unit_id, alloc_version_id, version_no, allocated_cents, is_latest) "
                "VALUES (?,?,?,?,?,?,1)",
                (p["period"], p["scenario_id"], org_id, p["alloc_version_id"],
                 p["version_no"], cents),
            )

    def _check_conservation(self, p: dict) -> None:
        line_total = sum(p["org_totals_cents"].values())
        ok = line_total == p["total_cents"]
        detail = ""
        if not ok:
            detail = (f"line total {line_total} != published total "
                      f"{p['total_cents']} (delta {line_total - p['total_cents']})")
        self.conn.execute(
            "INSERT INTO conservation_check(seq, period, scenario_id, "
            "charge_total_cents, line_total_cents, ok, detail) VALUES (?,?,?,?,?,?,?)",
            (self.last_seq, p["period"], p["scenario_id"], p["total_cents"],
             line_total, 1 if ok else 0, detail),
        )

    # ----------------------------------------------------------------- views
    def status(self) -> dict:
        rows = self.conn.execute(
            "SELECT event_type, COUNT(*) c FROM received_event GROUP BY event_type"
        ).fetchall()
        checks = self.conn.execute(
            "SELECT COUNT(*) total, SUM(ok) passed FROM conservation_check"
        ).fetchone()
        latest = self.conn.execute(
            "SELECT period, scenario_id, org_unit_id, allocated_cents, version_no "
            "FROM materialized_report WHERE is_latest = 1 "
            "ORDER BY period, scenario_id, org_unit_id"
        ).fetchall()
        return {
            "consumer_id": self.consumer_id,
            "last_seq": self.last_seq,
            "events": dict(rows),
            "conservation_checks": {"total": checks[0], "passed": checks[1]},
            "latest_report": [
                {"period": r[0], "scenario_id": r[1], "org_unit_id": r[2],
                 "allocated_cents": r[3], "version_no": r[4]} for r in latest
            ],
        }

    def run(self, once: bool, poll_seconds: float) -> None:
        while True:
            try:
                processed = self.poll_once()
                for ev in processed:
                    print(f"[subscriber {self.consumer_id}] seq={ev['seq']} "
                          f"{ev['event_type']}")
            except ConnectionError as exc:
                print(f"[subscriber {self.consumer_id}] OFFLINE/retrying: {exc}")
            if once:
                break
            time.sleep(poll_seconds)


def main() -> None:
    ap = argparse.ArgumentParser(description="costpipe report subscriber")
    ap.add_argument("--url", default="http://127.0.0.1:8791")
    ap.add_argument("--consumer", default="report-consumer-1")
    ap.add_argument("--store", default="data/subscriber.db")
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--poll-seconds", type=float, default=1.0)
    args = ap.parse_args()
    Subscriber(args.url, args.consumer, args.store).run(
        once=args.once, poll_seconds=args.poll_seconds)


if __name__ == "__main__":
    main()

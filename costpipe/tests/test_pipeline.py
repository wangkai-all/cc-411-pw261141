"""Pipeline tests: conservation, rounding tails, SCD2 correction, scenarios,
immutable versions, fingerprint skip, and the HTTP/subscriber catch-up path.

Run from costpipe/:  python -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from costpipe import repository as repo
from costpipe.db import connect, init_db
from costpipe.engine import allocate_period, largest_remainder
from costpipe.ingest import build_server, ingest_charges, ingest_usage, correct_org
from costpipe.subscriber import Subscriber
from costpipe import worker as worker_mod
from costpipe.seed import seed


class TempDB(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.tmp.name) / "t.db")
        self.conn = connect(self.db_path)
        init_db(self.conn)
        repo.ensure_real_scenario(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def seed_minimal(self):
        for oid, name, parent in [
            ("ROOT", "根", None), ("ENG", "研发", "ROOT"),
            ("A", "A组", "ENG"), ("B", "B组", "ENG"),
        ]:
            repo.seed_org_unit(self.conn, oid, name, parent)
        ingest_charges(self.conn, [
            {"charge_id": "C1", "period": "2026-10", "service_id": "SVC",
             "vendor": "v", "amount_cents": 1000},  # 10.00 yuan as integer cents
        ])
        ingest_usage(self.conn, [
            {"usage_id": "U1", "period": "2026-10", "service_id": "SVC",
             "org_unit_id": "A", "usage_qty": 1},
            {"usage_id": "U2", "period": "2026-10", "service_id": "SVC",
             "org_unit_id": "B", "usage_qty": 1},
        ])
        repo.create_rule_version(self.conn, "r1", repo.REAL_SCENARIO, "SVC",
                                 "usage", None, "2026-01")


class TestLargestRemainder(TempDB):
    def test_conservation_many_splits(self):
        # 100 cents over 3 equal weights -> 33/33/34, never 99 or 101.
        alloc, audit = largest_remainder(100, {"A": 1 / 3, "B": 1 / 3, "C": 1 / 3})
        self.assertEqual(sum(alloc.values()), 100)
        self.assertEqual(sorted(alloc.values()), [33, 33, 34])
        self.assertEqual(len(audit), 1)
        self.assertEqual(audit[0]["adjustment_cents"], 1)

    def test_indivisible_amount_deterministic(self):
        alloc1, _ = largest_remainder(7, {"A": 0.5, "B": 0.5})
        alloc2, _ = largest_remainder(7, {"A": 0.5, "B": 0.5})
        self.assertEqual(alloc1, alloc2)  # tie broken by org id
        self.assertEqual(sum(alloc1.values()), 7)

    def test_zero_weight_excluded(self):
        alloc, _ = largest_remainder(50, {"A": 1.0, "B": 0.0})
        self.assertEqual(alloc, {"A": 50, "B": 0})


class TestAllocation(TempDB):
    def test_usage_basis_and_charge_conservation(self):
        self.seed_minimal()
        repo.enqueue_job(self.conn, "2026-10", repo.REAL_SCENARIO, "test")
        results = worker_mod.process_all(self.conn)
        self.assertEqual(len(results), 1)
        ver = repo.current_alloc_version(self.conn, "2026-10", repo.REAL_SCENARIO)
        lines = repo.list_alloc_lines(self.conn, ver["id"])
        self.assertEqual(sum(l["allocated_cents"] for l in lines), 1000)
        self.assertEqual({l["org_unit_id"]: l["allocated_cents"] for l in lines},
                         {"A": 500, "B": 500})

    def test_equal_fallback_when_no_usage(self):
        for oid, name, parent in [("ROOT", "根", None), ("A", "A", "ROOT"),
                                  ("B", "B", "ROOT"), ("C", "C", "ROOT")]:
            repo.seed_org_unit(self.conn, oid, name, parent)
        ingest_charges(self.conn, [
            {"charge_id": "C1", "period": "2026-10", "service_id": "SVC",
             "vendor": "v", "amount_cents": 1000}])
        repo.create_rule_version(self.conn, "r1", repo.REAL_SCENARIO, "SVC",
                                 "usage", None, "2026-01")
        repo.enqueue_job(self.conn, "2026-10", repo.REAL_SCENARIO, "test")
        worker_mod.process_all(self.conn)
        ver = repo.current_alloc_version(self.conn, "2026-10", repo.REAL_SCENARIO)
        totals = repo.org_totals(self.conn, ver["id"])
        self.assertEqual(sum(totals.values()), 1000)
        self.assertTrue(all("equal fallback" in l["note"]
                            for l in repo.list_alloc_lines(self.conn, ver["id"])))

    def test_yuan_string_conversion(self):
        from costpipe.util import to_cents
        self.assertEqual(to_cents("875.33"), 87533)
        self.assertEqual(to_cents(1356.78), 135678)
        self.assertEqual(to_cents(50000), 50000)  # int stays cents


class TestSCD2Correction(TempDB):
    def test_correction_keeps_old_version_and_recomputes(self):
        self.seed_minimal()
        repo.enqueue_job(self.conn, "2026-10", repo.REAL_SCENARIO, "test")
        worker_mod.process_all(self.conn)
        v1 = repo.current_alloc_version(self.conn, "2026-10", repo.REAL_SCENARIO)

        # Move B under a new parent with a new name: must create version 2,
        # and the historical period must get a new immutable allocation.
        correct_org(self.conn, {
            "org_unit_id": "B", "new_name": "B-改名组", "new_parent_id": "ROOT",
            "valid_from": "2026-10-01", "reason": "test correction"})
        jobs = repo.list_jobs(self.conn)
        self.assertTrue(any(j["trigger_event"] == "org.corrected" for j in jobs))
        worker_mod.process_all(self.conn)

        v2 = repo.current_alloc_version(self.conn, "2026-10", repo.REAL_SCENARIO)
        self.assertNotEqual(v1["id"], v2["id"])
        self.assertEqual(v2["supersedes_id"], v1["id"])
        old = repo.get_alloc_version(self.conn, v1["id"])
        self.assertEqual(old["status"], "superseded")
        # old lines survive untouched
        old_lines = repo.list_alloc_lines(self.conn, v1["id"])
        self.assertTrue(any(l["org_path"].endswith("研发(ENG) / B组(B)")
                            for l in old_lines))
        new_lines = repo.list_alloc_lines(self.conn, v2["id"])
        self.assertTrue(any(l["org_path"].endswith("B-改名组(B)")
                            and "研发(ENG)" not in l["org_path"]
                            for l in new_lines))
        # money still conserved across both versions
        for vid in (v1["id"], v2["id"]):
            self.assertEqual(
                sum(l["allocated_cents"]
                    for l in repo.list_alloc_lines(self.conn, vid)), 1000)

    def test_unknown_org_rejected(self):
        with self.assertRaises(ValueError):
            correct_org(self.conn, {
                "org_unit_id": "GHOST", "new_name": "x",
                "valid_from": "2026-10-01"})


class TestScenarios(TempDB):
    def test_scenario_isolated_from_real(self):
        self.seed_minimal()
        repo.create_scenario(self.conn, "whatif", "假设情景")
        repo.create_rule_version(self.conn, "w1", "whatif", "SVC", "fixed",
                                 {"A": 1.0}, "2026-01")
        for sid in (repo.REAL_SCENARIO, "whatif"):
            repo.enqueue_job(self.conn, "2026-10", sid, "test")
        worker_mod.process_all(self.conn)

        real = repo.current_alloc_version(self.conn, "2026-10", repo.REAL_SCENARIO)
        what = repo.current_alloc_version(self.conn, "2026-10", "whatif")
        self.assertNotEqual(real["id"], what["id"])
        what_lines = repo.list_alloc_lines(self.conn, what["id"])
        self.assertEqual({l["org_unit_id"] for l in what_lines}, {"A"})
        # real allocation remains the 50/50 usage split
        real_totals = repo.org_totals(self.conn, real["id"])
        self.assertEqual(real_totals, {"A": 500, "B": 500})


class TestFingerprintAndVersions(TempDB):
    def test_unchanged_inputs_skip_recompute(self):
        self.seed_minimal()
        repo.enqueue_job(self.conn, "2026-10", repo.REAL_SCENARIO, "test")
        first = worker_mod.process_all(self.conn)[0]
        self.assertFalse(first["skipped"])
        repo.enqueue_job(self.conn, "2026-10", repo.REAL_SCENARIO, "test")
        second = worker_mod.process_all(self.conn)[0]
        self.assertTrue(second["skipped"])
        # force overrides the fingerprint
        repo.enqueue_job(self.conn, "2026-10", repo.REAL_SCENARIO, "test")
        job = repo.lease_job(self.conn)
        forced = worker_mod.process_job(self.conn, job, force=True)
        self.assertFalse(forced["skipped"])

    def test_usage_change_changes_fingerprint_and_result(self):
        self.seed_minimal()
        repo.enqueue_job(self.conn, "2026-10", repo.REAL_SCENARIO, "test")
        worker_mod.process_all(self.conn)
        inputs1 = repo.allocation_inputs(self.conn, "2026-10", repo.REAL_SCENARIO)
        ingest_usage(self.conn, [
            {"usage_id": "U3", "period": "2026-10", "service_id": "SVC",
             "org_unit_id": "A", "usage_qty": 3}])
        inputs2 = repo.allocation_inputs(self.conn, "2026-10", repo.REAL_SCENARIO)
        self.assertNotEqual(
            repo.input_fingerprint(inputs1), repo.input_fingerprint(inputs2))
        worker_mod.process_all(self.conn)
        ver = repo.current_alloc_version(self.conn, "2026-10", repo.REAL_SCENARIO)
        totals = repo.org_totals(self.conn, ver["id"])
        self.assertEqual(totals, {"A": 800, "B": 200})  # 4:1 weights


class TestHTTPSubscriber(TempDB):
    def test_http_ingest_and_subscriber_catchup(self):
        seed(self.db_path, reset=False)
        server = build_server(self.db_path, "127.0.0.1", 0)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = f"http://127.0.0.1:{port}"
        try:
            # drain initial seed jobs in-process
            worker_mod.process_all(self.conn)

            sub_store = str(Path(self.tmp.name) / "sub.db")
            sub = Subscriber(base, "test-consumer", sub_store)
            processed = sub.poll_once()
            self.assertTrue(processed)
            status = sub.status()
            self.assertEqual(status["conservation_checks"]["total"],
                             status["conservation_checks"]["passed"])
            self.assertEqual(
                status["last_seq"],
                self.conn.execute("SELECT MAX(seq) m FROM publication_log")
                    .fetchone()["m"])

            # more changes, then a second poll simulates reconnect catch-up
            req = urllib.request.Request(
                base + "/api/charges", method="POST",
                headers={"Content-Type": "application/json"},
                data=json.dumps([{
                    "charge_id": "INV-X-1", "period": "2026-10",
                    "service_id": "S3", "vendor": "v",
                    "amount_cents": 1001}]).encode())
            urllib.request.urlopen(req, timeout=5).read()
            worker_mod.process_all(self.conn)
            again = sub.poll_once()
            self.assertTrue(again)
            # every event is retained locally, none lost in the gap
            local_max = sub.conn.execute(
                "SELECT MAX(seq) FROM received_event").fetchone()[0]
            server_max = self.conn.execute(
                "SELECT MAX(seq) m FROM publication_log").fetchone()["m"]
            self.assertEqual(local_max, server_max)
            # conservation still holds for the odd-cent amount
            bad = sub.conn.execute(
                "SELECT COUNT(*) FROM conservation_check WHERE ok = 0").fetchone()[0]
            self.assertEqual(bad, 0)
        finally:
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()

"""SQLite connection management and schema (append-only / versioned design).

Storage layout:
  Raw inputs     : charge, usage_record, org_unit (SCD2), rule (versioned)
  Processing     : job, scenario
  Results        : alloc_version (immutable), alloc_line (immutable)
  Distribution   : publication_log (append-only event stream), consumer_offset

Nothing in the result layer is ever updated in place. A recompute inserts a
new alloc_version that supersedes the previous one; the old alloc_line rows
stay so every historical contribution remains traceable.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS charge (
    id              INTEGER PRIMARY KEY,
    charge_id       TEXT NOT NULL UNIQUE,
    period          TEXT NOT NULL,          -- YYYY-MM
    service_id      TEXT NOT NULL,
    vendor          TEXT NOT NULL,
    amount_cents    INTEGER NOT NULL,
    currency        TEXT NOT NULL DEFAULT 'CNY',
    received_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_charge_period ON charge(period);

CREATE TABLE IF NOT EXISTS usage_record (
    id              INTEGER PRIMARY KEY,
    usage_id        TEXT NOT NULL UNIQUE,
    period          TEXT NOT NULL,
    service_id      TEXT NOT NULL,
    org_unit_id     TEXT NOT NULL,
    usage_qty       REAL NOT NULL,
    received_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_lookup
    ON usage_record(period, service_id, org_unit_id);

-- Slowly-changing dimension type 2: corrections never rewrite a row.
CREATE TABLE IF NOT EXISTS org_unit (
    id                  INTEGER PRIMARY KEY,
    org_unit_id         TEXT NOT NULL,
    version             INTEGER NOT NULL,
    name                TEXT NOT NULL,
    parent_id           TEXT,
    valid_from          TEXT NOT NULL,      -- inclusive YYYY-MM-DD
    valid_to            TEXT NOT NULL,      -- inclusive, '9999-12-31' = open
    changed_at          TEXT NOT NULL,
    change_reason       TEXT NOT NULL,
    UNIQUE(org_unit_id, version)
);
CREATE INDEX IF NOT EXISTS idx_org_unit_id ON org_unit(org_unit_id);

CREATE TABLE IF NOT EXISTS scenario (
    scenario_id     TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    is_real         INTEGER NOT NULL,       -- 1 = real charges, 0 = what-if
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rule (
    id                  INTEGER PRIMARY KEY,
    rule_id             TEXT NOT NULL,
    version             INTEGER NOT NULL,
    scenario_id         TEXT NOT NULL,
    service_id          TEXT NOT NULL,      -- '*' = default rule
    basis               TEXT NOT NULL,      -- 'usage' | 'equal' | 'fixed'
    fixed_weights_json  TEXT,               -- {org_unit_id: weight}
    effective_from      TEXT NOT NULL,      -- YYYY-MM
    created_at          TEXT NOT NULL,
    active              INTEGER NOT NULL DEFAULT 1,
    UNIQUE(rule_id, version)
);
CREATE INDEX IF NOT EXISTS idx_rule_match
    ON rule(scenario_id, service_id, effective_from);

CREATE TABLE IF NOT EXISTS job (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    period          TEXT NOT NULL,
    scenario_id     TEXT NOT NULL,
    trigger_event   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued', -- queued|running|done|failed
    detail          TEXT,
    created_at      TEXT NOT NULL,
    started_at      TEXT,
    finished_at     TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_status ON job(status);

-- Immutable allocation outputs. A recompute inserts a new row and marks the
-- previous one superseded; old rows are never deleted or mutated.
CREATE TABLE IF NOT EXISTS alloc_version (
    id                  INTEGER PRIMARY KEY,
    period              TEXT NOT NULL,
    scenario_id         TEXT NOT NULL,
    version_no          INTEGER NOT NULL,
    supersedes_id       INTEGER,
    input_fingerprint   TEXT NOT NULL,
    total_cents         INTEGER NOT NULL,
    status              TEXT NOT NULL,      -- 'current' | 'superseded'
    created_at          TEXT NOT NULL,
    UNIQUE(period, scenario_id, version_no)
);

CREATE TABLE IF NOT EXISTS alloc_line (
    id                  INTEGER PRIMARY KEY,
    alloc_version_id    INTEGER NOT NULL REFERENCES alloc_version(id),
    charge_id           TEXT NOT NULL,
    service_id          TEXT NOT NULL,
    org_unit_id         TEXT NOT NULL,
    org_path            TEXT NOT NULL,
    weight              REAL NOT NULL,
    allocated_cents     INTEGER NOT NULL,
    is_residual         INTEGER NOT NULL DEFAULT 0,
    note                TEXT
);
CREATE INDEX IF NOT EXISTS idx_line_version ON alloc_line(alloc_version_id);
CREATE INDEX IF NOT EXISTS idx_line_charge ON alloc_line(charge_id);
CREATE INDEX IF NOT EXISTS idx_line_org ON alloc_line(org_unit_id);

-- Append-only event stream consumed by independent report subscribers.
CREATE TABLE IF NOT EXISTS publication_log (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type      TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS consumer_offset (
    consumer_id     TEXT PRIMARY KEY,
    last_seq        INTEGER NOT NULL DEFAULT 0,
    updated_at      TEXT NOT NULL
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()

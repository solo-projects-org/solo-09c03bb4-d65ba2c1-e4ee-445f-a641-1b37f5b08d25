"""SQLite schema and transaction helpers for the gate-tap-to-journey pipeline.

Model
-----
* ``batches``  – one row per accepted upload (audit trail).
* ``events``   – append-only.  Corrections are new rows with a higher
  ``revision`` for the same ``event_no``; the highest revision wins.
* ``results``  – station-pair counts per *service date* (the UTC day of the
  entry tap).  Rows are replaced on each successful computation.
* ``pending_taps`` – isolated / same-direction taps that could not be paired.
* ``day_state`` – publication state per service date: ``current`` means the
  results below it were computed from the newest accepted data; ``stale``
  means records changed since (or a recompute failed).
* ``day_epochs`` – monotonically increasing counter per date, bumped by every
  accepted upload that can affect that date.  It is the fencing token that
  stops a slow recompute from publishing over newer data.
* ``recompute_jobs`` – durable outbox.  Uploads only enqueue; only a
  successfully committed computation flips day states to ``current``.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    filename        TEXT    NOT NULL,
    received_at     TEXT    NOT NULL,
    row_count       INTEGER NOT NULL DEFAULT 0,
    accepted_count  INTEGER NOT NULL DEFAULT 0,
    ignored_count   INTEGER NOT NULL DEFAULT 0,
    day_start       TEXT,
    day_end         TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL REFERENCES batches(id),
    event_no     TEXT    NOT NULL,
    card_id      TEXT    NOT NULL,
    tap_time     TEXT    NOT NULL,          -- UTC "YYYY-MM-DDTHH:MM:SSZ"
    station      TEXT    NOT NULL,
    direction    TEXT    NOT NULL CHECK (direction IN ('in', 'out')),
    revision     INTEGER NOT NULL,
    created_at   TEXT    NOT NULL,
    UNIQUE (event_no, revision)
);
CREATE INDEX IF NOT EXISTS idx_events_tap_time ON events (tap_time);
CREATE INDEX IF NOT EXISTS idx_events_card_time ON events (card_id, tap_time);
CREATE INDEX IF NOT EXISTS idx_events_event_no ON events (event_no, revision);

CREATE TABLE IF NOT EXISTS results (
    service_date  TEXT    NOT NULL,
    from_station  TEXT    NOT NULL,
    to_station    TEXT    NOT NULL,
    trips         INTEGER NOT NULL,
    batch_id      INTEGER REFERENCES batches(id),
    computed_at   TEXT    NOT NULL,
    PRIMARY KEY (service_date, from_station, to_station)
);
CREATE INDEX IF NOT EXISTS idx_results_day ON results (service_date);

CREATE TABLE IF NOT EXISTS pending_taps (
    service_date  TEXT    NOT NULL,
    card_id       TEXT    NOT NULL,
    event_no      TEXT    NOT NULL,
    tap_time      TEXT    NOT NULL,
    station       TEXT    NOT NULL,
    direction     TEXT    NOT NULL CHECK (direction IN ('in', 'out')),
    reason        TEXT    NOT NULL,
    batch_id      INTEGER REFERENCES batches(id),
    computed_at   TEXT    NOT NULL,
    PRIMARY KEY (service_date, event_no)
);
CREATE INDEX IF NOT EXISTS idx_pending_day ON pending_taps (service_date);

CREATE TABLE IF NOT EXISTS day_state (
    service_date       TEXT PRIMARY KEY,
    status             TEXT NOT NULL CHECK (status IN ('current', 'stale')),
    published_batch_id INTEGER REFERENCES batches(id),
    computed_at        TEXT,
    data_epoch         INTEGER NOT NULL DEFAULT 0,
    last_error         TEXT,
    updated_at         TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS day_epochs (
    service_date TEXT PRIMARY KEY,
    epoch        INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS recompute_jobs (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at       TEXT    NOT NULL,
    started_at       TEXT,
    finished_at      TEXT,
    status           TEXT    NOT NULL CHECK (status IN ('pending', 'running', 'done', 'failed')),
    window_start     TEXT    NOT NULL,
    window_end       TEXT    NOT NULL,
    days_json        TEXT    NOT NULL,
    trigger_batch_id INTEGER REFERENCES batches(id),
    attempts         INTEGER NOT NULL DEFAULT 0,
    last_error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON recompute_jobs (status, id);

CREATE TABLE IF NOT EXISTS app_state (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=FULL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


@contextmanager
def transaction(conn: sqlite3.Connection, immediate: bool = True) -> Iterator[sqlite3.Connection]:
    """Explicit transaction; ``immediate`` takes the write lock up front."""
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


@contextmanager
def snapshot(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Read transaction holding a consistent WAL snapshot."""
    conn.execute("BEGIN")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def get_state(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO app_state (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )

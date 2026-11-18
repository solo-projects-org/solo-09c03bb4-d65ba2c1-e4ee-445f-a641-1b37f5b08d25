"""Business logic: accept uploads, invalidate, recompute, publish, recover.

Publication protocol
--------------------
1. ``accept_upload`` runs in one IMMEDIATE transaction.  It writes the batch
   and its new event revisions, derives old and new journeys, and collects
   every service date whose derived state changed.  It bumps each such date's
   epoch and marks the date's published results ``stale`` in the same
   transaction.  From the moment the upload is acknowledged, the UI cannot
   show the old numbers as current.
2. ``run_pending_jobs`` computes from a consistent read snapshot (recording
   the captured epoch of every date in the job), then re-takes the write lock
   and re-reads the epochs.  Only if every epoch is unchanged does it replace
   results, replace pending taps, and flip the job's dates to ``current``.
   A concurrent upload between snapshot and publish forces the job to abort
   and retry, so an old recompute can never overwrite a newer revision.
3. Every date in a job is published in the same transaction.  Because jobs
   cover adjacent dates together (uploads enqueue all affected dates and
   merge adjacent pending jobs), cross-midnight pairs and the two dates they
   touch become current together — never half-published.
4. Any failure leaves the dates ``stale`` with the error recorded.  On
   startup ``recover`` re-queues interrupted jobs, re-queues stale dates that
   lost their job, and demotes any ``current`` date whose epoch is behind.
"""

from __future__ import annotations

import base64
import binascii
import json
import sqlite3
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable

from . import db
from .ingest import Tap, UploadError, parse_csv
from .journeys import Event, Journey, PendingTap, derive

MAX_JOB_ATTEMPTS = 5

# The review list is paged, never loaded whole: one query reads at most this
# many rows (plus one look-ahead row to decide whether another page exists).
DEFAULT_PAGE_LIMIT = 100
MAX_PAGE_LIMIT = 100


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_stored_time(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def _row_to_event(row: sqlite3.Row) -> Event:
    return Event(
        event_no=row["event_no"],
        card_id=row["card_id"],
        tap_time=_parse_stored_time(row["tap_time"]),
        station=row["station"],
        direction=row["direction"],
        revision=row["revision"],
    )


_LATEST_EVENTS = (
    "SELECT e.event_no, e.card_id, e.tap_time, e.station, e.direction, e.revision "
    "FROM events e JOIN (SELECT event_no, MAX(revision) AS rev FROM events GROUP BY event_no) m "
    "ON m.event_no = e.event_no AND m.rev = e.revision"
)


def load_latest_events(conn: sqlite3.Connection, card_ids: Iterable[str] | None = None) -> list[Event]:
    if card_ids is not None:
        ids = sorted(set(card_ids))
        if not ids:
            return []
        sql = _LATEST_EVENTS + " WHERE e.card_id IN (%s)" % ",".join("?" for _ in ids)
        rows = conn.execute(sql, ids).fetchall()
    else:
        rows = conn.execute(_LATEST_EVENTS).fetchall()
    return [_row_to_event(row) for row in rows]


def _latest_revisions(conn: sqlite3.Connection, event_nos: Iterable[str]) -> dict[str, int]:
    ids = sorted(set(event_nos))
    out: dict[str, int] = {}
    for i in range(0, len(ids), 500):
        part = ids[i:i + 500]
        rows = conn.execute(
            "SELECT event_no, MAX(revision) AS rev FROM events WHERE event_no IN (%s) GROUP BY event_no"
            % ",".join("?" for _ in part),
            part,
        )
        for row in rows:
            out[row["event_no"]] = int(row["rev"])
    return out


def _existing_revision(conn: sqlite3.Connection, event_no: str, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT event_no, card_id, tap_time, station, direction FROM events "
        "WHERE event_no = ? AND revision = ?",
        (event_no, revision),
    ).fetchone()


def _bump_global_epoch(conn: sqlite3.Connection) -> int:
    current = int(db.get_state(conn, "data_epoch", "0") or "0")
    new = current + 1
    db.set_state(conn, "data_epoch", str(new))
    return new


def _day_epoch(conn: sqlite3.Connection, day: str) -> int:
    row = conn.execute("SELECT epoch FROM day_epochs WHERE service_date = ?", (day,)).fetchone()
    return int(row["epoch"]) if row else 0


def _next_publication_id(conn: sqlite3.Connection) -> int:
    """Global monotonic id, one per publication transaction.

    It changes only when displayed rows are replaced, which makes it the
    right token to bind a pagination cursor to: a cursor issued against one
    publication can never silently continue into a newer one.
    """
    current = int(db.get_state(conn, "publication_seq", "0") or "0")
    new = current + 1
    db.set_state(conn, "publication_seq", str(new))
    return new


def _stale_days(
    conn: sqlite3.Connection, days: Iterable[str], reason: str, published_batch_bumps: bool = True
) -> None:
    """Bump each date's epoch and mark it stale (never the other way around)."""
    now = utcnow()
    epoch = _bump_global_epoch(conn)
    for day in sorted(set(days)):
        conn.execute(
            "INSERT INTO day_epochs (service_date, epoch) VALUES (?, ?) "
            "ON CONFLICT(service_date) DO UPDATE SET epoch = excluded.epoch",
            (day, epoch),
        )
        conn.execute(
            "INSERT INTO day_state (service_date, status, data_epoch, last_error, updated_at) "
            "VALUES (?, 'stale', ?, ?, ?) "
            "ON CONFLICT(service_date) DO UPDATE SET "
            "  status = 'stale', data_epoch = excluded.data_epoch, "
            "  last_error = excluded.last_error, updated_at = excluded.updated_at",
            (day, epoch, reason, now),
        )


# ---------------------------------------------------------------------------
# accepting uploads
# ---------------------------------------------------------------------------

@dataclass
class AcceptResult:
    batch_id: int
    row_count: int
    inserted: int
    ignored: int
    affected_days: list[str]
    job_id: int


def accept_upload(db_path: str | Path, filename: str, payload: bytes) -> AcceptResult:
    taps = parse_csv(payload, filename)          # fully validated before any write
    conn = db.connect(db_path)
    try:
        with db.transaction(conn):
            return _accept(conn, filename, taps)
    finally:
        conn.close()


def _accept(conn: sqlite3.Connection, filename: str, taps: list[Tap]) -> AcceptResult:
    file_days = sorted({t.day for t in taps})
    card_ids = {t.card_id for t in taps}
    event_nos = sorted({t.event_no for t in taps})

    # A correction may reassign an event to another card; the *previous* card's
    # pairing must be re-derived too, even though it is absent from the file.
    for i in range(0, len(event_nos), 500):
        part = event_nos[i:i + 500]
        for row in conn.execute(
            "SELECT DISTINCT card_id FROM events WHERE event_no IN (%s) GROUP BY card_id"
            % ",".join("?" for _ in part),
            part,
        ):
            card_ids.add(row["card_id"])
    card_ids = sorted(card_ids)

    old_events = load_latest_events(conn, card_ids)

    now = utcnow()
    cursor = conn.execute(
        "INSERT INTO batches (filename, received_at, row_count, day_start, day_end) VALUES (?, ?, ?, ?, ?)",
        (filename, now, len(taps), file_days[0], file_days[-1]),
    )
    batch_id = int(cursor.lastrowid)

    max_rev = _latest_revisions(conn, event_nos)
    # Only the highest revision per event inside this file matters; lower ones
    # are superseded by their own file and reported as ignored.
    file_best: dict[str, int] = {}
    for tap in taps:
        file_best[tap.event_no] = max(file_best.get(tap.event_no, -1), tap.revision)

    inserted = 0
    ignored = 0
    for tap in taps:
        if tap.revision != file_best[tap.event_no]:
            ignored += 1
            continue
        latest = max_rev.get(tap.event_no, -1)
        if tap.revision < latest:
            ignored += 1
            continue
        existing = _existing_revision(conn, tap.event_no, tap.revision)
        if existing is not None:
            same = (
                existing["card_id"] == tap.card_id
                and existing["tap_time"] == tap.iso
                and existing["station"] == tap.station
                and existing["direction"] == tap.direction
            )
            if not same:
                raise UploadError(
                    f"事件号 {tap.event_no} 修订号 {tap.revision} 已存在且内容不同，"
                    "请使用更高的修订号更正"
                )
            ignored += 1
            continue
        conn.execute(
            "INSERT INTO events (batch_id, event_no, card_id, tap_time, station, direction, revision, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (batch_id, tap.event_no, tap.card_id, tap.iso, tap.station, tap.direction, tap.revision, now),
        )
        inserted += 1

    conn.execute(
        "UPDATE batches SET accepted_count = ?, ignored_count = ? WHERE id = ?",
        (inserted, ignored, batch_id),
    )

    new_events = load_latest_events(conn, card_ids)

    # Every date holding an event of a touched card can have its pairing
    # changed (inserting/removing one tap cascades through that card's whole
    # chronological sequence).  This set always contains both the entry day
    # and the exit day of any cross-midnight journey, which is what guarantees
    # the two dates are invalidated and re-published together.
    affected = {e.tap_time.strftime("%Y-%m-%d") for e in old_events}
    affected |= {e.tap_time.strftime("%Y-%m-%d") for e in new_events}
    affected |= set(file_days)
    affected = {d for d in affected if d}

    if inserted:
        _stale_days(conn, affected, f"批次 {batch_id} 受理，结果待重算")
        job_id = _enqueue_job(conn, sorted(affected), batch_id)
    else:
        job_id = 0

    return AcceptResult(
        batch_id=batch_id,
        row_count=len(taps),
        inserted=inserted,
        ignored=ignored,
        affected_days=sorted(affected) if inserted else [],
        job_id=job_id,
    )


def _enqueue_job(conn: sqlite3.Connection, days: list[str], batch_id: int | None) -> int:
    """Enqueue a recompute, merging any still-pending job whose window touches it."""
    merged = set(days)
    merged_trigger = batch_id
    for row in conn.execute(
        "SELECT id, days_json, trigger_batch_id FROM recompute_jobs WHERE status = 'pending'"
    ).fetchall():
        job_days = set(json.loads(row["days_json"]))
        if _touching(min(job_days), max(job_days), min(days), max(days)):
            merged |= job_days
            merged_trigger = merged_trigger or row["trigger_batch_id"]
            conn.execute("DELETE FROM recompute_jobs WHERE id = ?", (row["id"],))
    days_sorted = sorted(merged)
    cursor = conn.execute(
        "INSERT INTO recompute_jobs (created_at, status, window_start, window_end, days_json, trigger_batch_id) "
        "VALUES (?, 'pending', ?, ?, ?, ?)",
        (utcnow(), days_sorted[0], days_sorted[-1], json.dumps(days_sorted), merged_trigger),
    )
    return int(cursor.lastrowid)


def _touching(a_start: str, a_end: str, b_start: str, b_end: str) -> bool:
    """Overlapping or adjacent day ranges (adjacent ranges share midnight context)."""
    return date.fromisoformat(a_start) <= date.fromisoformat(b_end) + timedelta(days=1) and \
        date.fromisoformat(b_start) <= date.fromisoformat(a_end) + timedelta(days=1)


# ---------------------------------------------------------------------------
# recompute worker
# ---------------------------------------------------------------------------

class StaleDataError(RuntimeError):
    """The snapshot used for a computation is no longer current."""


def run_pending_jobs(
    db_path: str | Path,
    limit: int = 100,
    hook: Callable[[str, sqlite3.Connection, dict], None] | None = None,
) -> int:
    """Drain pending jobs, including ones deferred/merged while running.

    Each job attempt increments ``attempts``, so the loop terminates: a job
    that keeps hitting stale snapshots becomes ``failed`` after
    ``MAX_JOB_ATTEMPTS`` and stops being retried.  ``limit`` bounds total work.
    """
    conn = db.connect(db_path)
    attempted = 0
    try:
        while attempted < limit:
            row = conn.execute(
                "SELECT id FROM recompute_jobs WHERE status = 'pending' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                break
            attempted += 1
            _run_job(conn, int(row["id"]), hook)
    finally:
        conn.close()
    return attempted


def _run_job(
    conn: sqlite3.Connection,
    job_id: int,
    hook: Callable[[str, sqlite3.Connection, dict], None] | None,
) -> None:
    with db.transaction(conn):
        claimed = conn.execute(
            "UPDATE recompute_jobs SET status = 'running', started_at = ?, attempts = attempts + 1 "
            "WHERE id = ? AND status = 'pending'",
            (utcnow(), job_id),
        ).rowcount
    if claimed != 1:
        return
    job = conn.execute("SELECT * FROM recompute_jobs WHERE id = ?", (job_id,)).fetchone()
    days = json.loads(job["days_json"])
    attempts = int(job["attempts"])

    captured: dict[str, int] = {}
    publish_days: list[str] = list(days)
    try:
        with db.snapshot(conn):
            journeys, pending = derive(load_latest_events(conn))
            # Dates linked by a cross-midnight journey must become current in
            # the same transaction.  If an operation-linked pair spans the job
            # window (e.g. a targeted repair of only one side), widen the
            # publication set to include the partner date.
            publish_days = _linked_days(days, journeys)
            publish_set = set(publish_days)
            for day in publish_days:
                captured[day] = _day_epoch(conn, day)
            result_rows = _aggregate(journeys, publish_set)
            pending_rows = [p for p in pending if p.service_date in publish_set]

        if hook:
            hook("after_compute", conn, {"job_id": job_id, "days": publish_days})

        with db.transaction(conn):
            if {day: _day_epoch(conn, day) for day in publish_days} != captured:
                raise StaleDataError("重算期间收到新数据，放弃本次发布并重试")
            _publish(conn, job, publish_days, captured, result_rows, pending_rows)

        if hook:
            hook("after_publish", conn, {"job_id": job_id, "days": publish_days})
    except StaleDataError as exc:
        _fail_or_retry(conn, job_id, days, attempts, str(exc), retry=True)
    except Exception as exc:  # noqa: BLE001 - surfaced to the UI for triage
        _fail_or_retry(conn, job_id, days, attempts, f"{type(exc).__name__}: {exc}", retry=False)


def _linked_days(days: Iterable[str], journeys: list[Journey]) -> list[str]:
    """Close the day set over journeys whose two ends fall on different days."""
    result = set(days)
    for journey in journeys:
        if journey.service_date in result or journey.exit_date in result:
            result.add(journey.service_date)
            result.add(journey.exit_date)
    return sorted(result)


def _publish(
    conn: sqlite3.Connection,
    job: sqlite3.Row,
    days: list[str],
    epochs: dict[str, int],
    results: Counter,
    pending_rows: list[PendingTap],
) -> None:
    now = utcnow()
    publication_id = _next_publication_id(conn)
    for day in days:
        conn.execute("DELETE FROM results WHERE service_date = ?", (day,))
        conn.execute("DELETE FROM pending_taps WHERE service_date = ?", (day,))
    for (day, src, dst), trips in sorted(results.items()):
        conn.execute(
            "INSERT INTO results (service_date, from_station, to_station, trips, batch_id, computed_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (day, src, dst, trips, job["trigger_batch_id"], now),
        )
    for p in pending_rows:
        conn.execute(
            "INSERT INTO pending_taps (service_date, card_id, event_no, tap_time, station, direction, reason, batch_id, computed_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                p.service_date, p.card_id, p.event_no,
                p.tap_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                p.station, p.direction, p.reason, job["trigger_batch_id"], now,
            ),
        )
    for day in days:
        conn.execute(
            "INSERT INTO day_state (service_date, status, published_batch_id, computed_at, data_epoch, publication_id, last_error, updated_at) "
            "VALUES (?, 'current', ?, ?, ?, ?, NULL, ?) "
            "ON CONFLICT(service_date) DO UPDATE SET "
            "  status = 'current', published_batch_id = excluded.published_batch_id, "
            "  computed_at = excluded.computed_at, data_epoch = excluded.data_epoch, "
            "  publication_id = excluded.publication_id, "
            "  last_error = NULL, updated_at = excluded.updated_at",
            (day, job["trigger_batch_id"], now, epochs[day], publication_id, now),
        )
    conn.execute(
        "UPDATE recompute_jobs SET status = 'done', finished_at = ?, last_error = NULL WHERE id = ?",
        (now, job["id"]),
    )


def _fail_or_retry(
    conn: sqlite3.Connection, job_id: int, days: list[str], attempts: int, error: str, retry: bool
) -> None:
    with db.transaction(conn):
        if retry and attempts < MAX_JOB_ATTEMPTS:
            conn.execute(
                "UPDATE recompute_jobs SET status = 'pending', started_at = NULL, last_error = ? WHERE id = ?",
                (error, job_id),
            )
            return
        conn.execute(
            "UPDATE recompute_jobs SET status = 'failed', finished_at = ?, last_error = ? WHERE id = ?",
            (utcnow(), error, job_id),
        )
        # Do not touch a day that a newer, successful job already published.
        now = utcnow()
        for day in days:
            conn.execute(
                "UPDATE day_state SET status = 'stale', last_error = ?, updated_at = ? "
                "WHERE service_date = ? AND status = 'stale'",
                (error, now, day),
            )


def _aggregate(journeys: list[Journey], days: set[str]) -> Counter:
    counter: Counter = Counter()
    for journey in journeys:
        if journey.service_date in days:
            counter[(journey.service_date, journey.from_station, journey.to_station)] += 1
    return counter


# ---------------------------------------------------------------------------
# recovery
# ---------------------------------------------------------------------------

def recover(db_path: str | Path) -> None:
    """Bring persistent state back to a safe point after a restart/crash."""
    conn = db.connect(db_path)
    try:
        with db.transaction(conn):
            now = utcnow()
            interrupted = conn.execute(
                "SELECT id FROM recompute_jobs WHERE status = 'running'"
            ).fetchall()
            for row in interrupted:
                conn.execute(
                    "UPDATE recompute_jobs SET status = 'pending', started_at = NULL, "
                    "last_error = '服务重启，任务重新排队' WHERE id = ?",
                    (row["id"],),
                )
            # Neutralise any claim that is not backed by the current epoch.
            # Results of a demoted day are deleted so a stale number can never
            # be mistaken for a current one.
            demoted = [
                row["service_date"]
                for row in conn.execute(
                    "SELECT service_date, data_epoch FROM day_state WHERE status = 'current'"
                ).fetchall()
                if _day_epoch(conn, row["service_date"]) != int(row["data_epoch"])
            ]
            for day in demoted:
                conn.execute(
                    "UPDATE day_state SET status = 'stale', last_error = '数据已更新，结果待重算', updated_at = ? "
                    "WHERE service_date = ?",
                    (now, day),
                )
                conn.execute("DELETE FROM results WHERE service_date = ?", (day,))
                conn.execute("DELETE FROM pending_taps WHERE service_date = ?", (day,))
            _ensure_stale_jobs(conn)
    finally:
        conn.close()


def _ensure_stale_jobs(conn: sqlite3.Connection) -> None:
    covered: set[str] = set()
    for row in conn.execute(
        "SELECT days_json FROM recompute_jobs WHERE status IN ('pending', 'running')"
    ).fetchall():
        covered |= set(json.loads(row["days_json"]))
    stale = [
        row["service_date"]
        for row in conn.execute(
            "SELECT service_date FROM day_state WHERE status = 'stale' ORDER BY service_date"
        ).fetchall()
        if row["service_date"] not in covered
    ]
    groups: list[list[str]] = []
    for day in stale:
        if groups and date.fromisoformat(day) == date.fromisoformat(groups[-1][-1]) + timedelta(days=1):
            groups[-1].append(day)
        else:
            groups.append([day])
    for group in groups:
        _enqueue_job(conn, group, None)


def request_recompute(db_path: str | Path, days: list[str]) -> int | None:
    """Manually invalidate and re-queue dates (repair button / tests)."""
    days = sorted(set(days))
    if not days:
        return None
    conn = db.connect(db_path)
    try:
        with db.transaction(conn):
            _stale_days(conn, days, "手动请求重算")
            return _enqueue_job(conn, days, None)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# read models for the web UI
# ---------------------------------------------------------------------------

class DayChangedError(RuntimeError):
    """A pagination cursor no longer matches the date's current publication.

    Raised while serving a follow-up page when an upload, correction or
    recompute has made the date stale or replaced its published rows since
    the cursor was issued.  Callers must not append the page: the browser
    discards the old list and restarts from the current publication.
    """

    def __init__(self, day: str, status: str, reason: str):
        super().__init__(reason)
        self.day = day
        self.status = status
        self.reason = reason


def _encode_cursor(
    day: str, publication_id: int, delivered: int, tap_time: str, event_no: str
) -> str:
    payload = json.dumps(
        {
            "v": 1,
            "d": day,
            "p": publication_id,
            "n": delivered,
            "t": tap_time,
            "e": event_no,
        },
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii")


def _decode_cursor(cursor: str, day: str) -> tuple[int, int, str, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode("ascii"))
        data = json.loads(raw.decode("utf-8"))
        if not isinstance(data, dict) or data.get("v") != 1:
            raise ValueError("unsupported cursor")
        publication_id = int(data["p"])
        delivered = int(data["n"])
        tap_time = str(data["t"])
        event_no = str(data["e"])
    except (ValueError, KeyError, TypeError, UnicodeError, binascii.Error) as exc:
        raise ValueError(f"翻页游标无效：{exc}") from exc
    if data.get("d") != day:
        raise ValueError("翻页游标属于另一个日期")
    if delivered < 0:
        raise ValueError("翻页游标无效：已读条数为负")
    return publication_id, delivered, tap_time, event_no


def _day_summary(conn: sqlite3.Connection, day: str) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM day_state WHERE service_date = ?", (day,)).fetchone()


def _pending_page(
    conn: sqlite3.Connection,
    day: str,
    limit: int,
    after: tuple[str, str] | None,
) -> tuple[list[dict], bool]:
    """Read at most ``limit`` pending rows from one date.

    Ordering is ``(tap_time, event_no)`` — total and stable, and backed by
    ``idx_pending_day_order``.  ``after`` is the keyset of the last row of the
    previous page, so no offset scan or in-memory slicing is needed.  Whether
    another page exists is settled by a one-row ``LIMIT 1`` probe, so the row
    query itself never reads more than ``limit`` records.
    """
    where = "service_date = ?"
    params: list = [day]
    if after is not None:
        # Row-value comparison so SQLite can seek straight to the keyset
        # position in idx_pending_day_order instead of scanning from the day's
        # first row on every page.
        where += " AND (tap_time, event_no) > (?, ?)"
        params += [after[0], after[1]]
    rows = conn.execute(
        "SELECT card_id, event_no, tap_time, station, direction, reason "
        f"FROM pending_taps WHERE {where} ORDER BY tap_time, event_no LIMIT ?",
        params + [limit],
    ).fetchall()
    page = [dict(row) for row in rows]
    if len(page) < limit:
        return page, False
    last = page[-1]
    probe = conn.execute(
        "SELECT 1 FROM pending_taps WHERE service_date = ? "
        "AND (tap_time, event_no) > (?, ?) LIMIT 1",
        (day, last["tap_time"], last["event_no"]),
    ).fetchone()
    return page, probe is not None


def _station_pairs(conn: sqlite3.Connection, day: str) -> list[dict]:
    return [dict(row) for row in conn.execute(
        "SELECT from_station, to_station, trips, computed_at FROM results "
        "WHERE service_date = ? ORDER BY trips DESC, from_station, to_station",
        (day,),
    ).fetchall()]


def known_days(conn: sqlite3.Connection) -> list[dict]:
    """Service dates and their publication state.

    ``trips`` is only reported for ``current`` dates; stale numbers are never
    surfaced (they may describe superseded records).
    """
    rows = conn.execute(
        "SELECT ds.service_date AS date, ds.status, ds.computed_at, ds.last_error, "
        "       ds.published_batch_id, "
        "       CASE WHEN ds.status = 'current' THEN COALESCE(SUM(r.trips), 0) ELSE NULL END AS trips "
        "FROM day_state ds LEFT JOIN results r ON r.service_date = ds.service_date "
        "GROUP BY ds.service_date ORDER BY ds.service_date DESC"
    ).fetchall()
    return [dict(row) for row in rows]


def day_detail(
    conn: sqlite3.Connection,
    day: str,
    limit: int = DEFAULT_PAGE_LIMIT,
    cursor: str | None = None,
) -> dict:
    """One page of a date's review list plus its publication metadata.

    Everything returned (status, totals, station pairs, this page's rows) is
    read from a single short read transaction, so it all belongs to the same
    published version even if a recompute commits concurrently.  The
    transaction lasts only for this request; it is never held while the
    browser decides whether to ask for another page.
    """
    limit = max(1, min(int(limit), MAX_PAGE_LIMIT))
    decoded = _decode_cursor(cursor, day) if cursor else None

    with db.snapshot(conn):
        state = _day_summary(conn, day)
        status = state["status"] if state else "uncomputed"
        publication_id = int(state["publication_id"]) if state else 0

        if decoded is not None:
            cursor_publication, delivered, after_time, after_event = decoded
            if status != "current":
                raise DayChangedError(
                    day, status, "该日已过期或正在重算，旧清单不能再翻页"
                )
            if cursor_publication != publication_id:
                raise DayChangedError(
                    day, status, "该日已换版，旧清单不能再翻页"
                )
            after: tuple[str, str] | None = (after_time, after_event)
        else:
            delivered = 0
            after = None

        pending_total = 0
        pending: list[dict] = []
        pairs: list[dict] = []
        if status == "current":
            pending_total = int(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM pending_taps WHERE service_date = ?", (day,)
                ).fetchone()["n"]
            )
            pending, has_more = _pending_page(conn, day, limit, after)
            pairs = _station_pairs(conn, day)
        else:
            has_more = False

    return {
        "date": day,
        "status": status,
        "computed_at": state["computed_at"] if state else None,
        "last_error": state["last_error"] if state else None,
        "publication_id": publication_id,
        "trip_total": sum(row["trips"] for row in pairs),
        "station_pairs": pairs,
        "pending": pending,
        "pending_total": pending_total,
        "page": {
            "limit": limit,
            "returned": len(pending),
            "delivered": delivered + len(pending),
            "has_more": has_more,
            "next_cursor": _encode_cursor(
                day,
                publication_id,
                delivered + len(pending),
                pending[-1]["tap_time"],
                pending[-1]["event_no"],
            ) if has_more else None,
        },
    }


def overview(conn: sqlite3.Connection) -> dict:
    jobs = [dict(row) for row in conn.execute(
        "SELECT id, status, window_start, window_end, days_json, attempts, last_error, "
        "       created_at, finished_at FROM recompute_jobs ORDER BY id DESC LIMIT 20"
    ).fetchall()]
    for job in jobs:
        job["days"] = json.loads(job.pop("days_json"))
    counts = {row["status"]: row["n"] for row in conn.execute(
        "SELECT status, COUNT(*) AS n FROM recompute_jobs GROUP BY status"
    ).fetchall()}
    batches = [dict(row) for row in conn.execute(
        "SELECT id, filename, received_at, row_count, accepted_count, ignored_count, day_start, day_end "
        "FROM batches ORDER BY id DESC LIMIT 20"
    ).fetchall()]
    return {"jobs": jobs, "job_counts": counts, "batches": batches,
            "days": known_days(conn)}

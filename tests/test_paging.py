"""Paged review list: bounded reads, stable ordering, publication fencing."""

import importlib

import pytest
from fastapi.testclient import TestClient

from app import db, service

from helpers import make_csv, tap, upload


def pending_taps(n, day="2024-05-01"):
    """`n` unpaired entries: one isolated tap per card, all pending."""
    return [
        tap(f"e{i:04d}", f"c{i:04d}", f"{day}T{(i // 3600) % 24:02d}:{(i // 60) % 60:02d}:{i % 60:02d}Z",
            "A", "in")
        for i in range(n)
    ]


def fetch(db_path, day, limit=100, cursor=None):
    conn = db.connect(db_path)
    try:
        return service.day_detail(conn, day, limit=limit, cursor=cursor)
    finally:
        conn.close()


def all_pages(db_path, day, limit=100):
    page = fetch(db_path, day, limit=limit)
    rows = list(page["pending"])
    while page["page"]["next_cursor"]:
        page = fetch(db_path, day, limit=limit, cursor=page["page"]["next_cursor"])
        rows.extend(page["pending"])
    return page, rows


def test_first_page_is_capped_and_keeps_summary(db_path):
    upload(db_path, pending_taps(250))
    page = fetch(db_path, "2024-05-01")

    assert page["status"] == "current"
    # The whole-day total is still reported, but only one page of rows is read.
    assert page["pending_total"] == 250
    assert len(page["pending"]) == 100
    assert page["page"]["has_more"] is True
    assert page["page"]["returned"] == 100
    # Station pairs and totals still belong to the same payload.
    assert page["trip_total"] == 0
    assert page["station_pairs"] == []


def test_pages_are_disjoint_stable_and_complete(db_path):
    upload(db_path, pending_taps(250))
    last, rows = all_pages(db_path, "2024-05-01")

    keys = [(r["tap_time"], r["event_no"]) for r in rows]
    assert keys == sorted(keys)
    assert len(keys) == 250
    assert len(set(keys)) == 250
    assert last["pending_total"] == 250
    assert last["page"]["has_more"] is False
    assert last["page"]["next_cursor"] is None


def test_identical_tap_times_use_event_no_as_tiebreak(db_path):
    rows = [
        tap("e2", "c2", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e10", "c1", "2024-05-01T08:00:00Z", "B", "in"),
        tap("e1", "c3", "2024-05-01T08:00:00Z", "C", "in"),
    ]
    upload(db_path, rows)
    _, seen = all_pages(db_path, "2024-05-01", limit=1)
    assert [r["event_no"] for r in seen] == ["e1", "e10", "e2"]


def test_limit_is_bounded(db_path):
    upload(db_path, pending_taps(30))
    assert len(fetch(db_path, "2024-05-01", limit=5)["pending"]) == 5
    # Even an oversized request cannot make the server read more than 100 rows.
    assert len(fetch(db_path, "2024-05-01", limit=10_000)["pending"]) == 30


def test_cursor_from_another_day_is_rejected(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c2", "2024-05-02T08:00:00Z", "B", "in"),
    ])
    page = fetch(db_path, "2024-05-01")
    cursor = service._encode_cursor(
        "2024-05-02", page["publication_id"], 1, "2024-05-02T08:00:00Z", "e2"
    )
    with pytest.raises(ValueError):
        fetch(db_path, "2024-05-01", limit=1, cursor=cursor)


def test_upload_between_pages_invalidates_cursor(db_path):
    upload(db_path, pending_taps(250))
    first = fetch(db_path, "2024-05-01")
    cursor = first["page"]["next_cursor"]
    assert cursor

    # A correction is accepted and published, replacing the day's rows.
    upload(db_path, [tap("e0000", "c0000", "2024-05-01T00:00:00Z", "Z", "in", revision=1)])

    with pytest.raises(service.DayChangedError) as exc:
        fetch(db_path, "2024-05-01", cursor=cursor)
    assert exc.value.status == "current"
    assert "换版" in exc.value.reason

    # Restarting without a cursor serves the new publication from page 1.
    fresh = fetch(db_path, "2024-05-01")
    assert fresh["publication_id"] != first["publication_id"]
    assert fresh["pending_total"] == 250
    assert len(fresh["pending"]) == 100


def test_stale_day_invalidates_cursor_before_recompute(db_path):
    upload(db_path, pending_taps(250))
    first = fetch(db_path, "2024-05-01")

    # Accept but do not run the worker: the day is now stale, even though the
    # old rows may still physically exist.
    service.accept_upload(db_path, "late.csv", make_csv([
        tap("e0000", "c0000", "2024-05-01T00:00:00Z", "Z", "in", revision=1),
    ]))

    with pytest.raises(service.DayChangedError) as exc:
        fetch(db_path, "2024-05-01", cursor=first["page"]["next_cursor"])
    assert exc.value.status == "stale"
    assert "过期" in exc.value.reason

    stale = fetch(db_path, "2024-05-01")
    assert stale["status"] == "stale"
    assert stale["pending"] == []
    assert stale["pending_total"] == 0


def test_page_read_does_not_hold_a_read_transaction(db_path):
    upload(db_path, pending_taps(250))
    conn = db.connect(db_path)
    try:
        service.day_detail(conn, "2024-05-01")
        # Nothing stays open between requests, so a recompute can take the
        # write lock immediately instead of waiting on a reader.
        assert conn.in_transaction is False
        with db.transaction(conn):
            conn.execute("SELECT 1 FROM pending_taps LIMIT 1").fetchone()
        assert conn.in_transaction is False
    finally:
        conn.close()

    # Between pages the write path is free: an upload requiring an IMMEDIATE
    # transaction is accepted while another cursor is outstanding.
    first = fetch(db_path, "2024-05-01")
    accepted = service.accept_upload(db_path, "interleaved.csv", make_csv([
        tap("x1", "cx", "2024-05-01T12:00:00Z", "N", "in"),
    ]))
    assert accepted.inserted == 1
    with pytest.raises(service.DayChangedError):
        fetch(db_path, "2024-05-01", cursor=first["page"]["next_cursor"])
    service.run_pending_jobs(db_path)
    assert fetch(db_path, "2024-05-01")["pending_total"] == 251


def test_migration_adds_publication_id_to_old_database(tmp_path):
    import sqlite3

    path = tmp_path / "old.sqlite3"
    raw = sqlite3.connect(path)
    raw.execute(
        "CREATE TABLE day_state (service_date TEXT PRIMARY KEY, status TEXT NOT NULL, "
        "published_batch_id INTEGER, computed_at TEXT, data_epoch INTEGER NOT NULL DEFAULT 0, "
        "last_error TEXT, updated_at TEXT NOT NULL)"
    )
    raw.commit()
    raw.close()

    conn = db.connect(path)
    try:
        db.init_db(conn)
        columns = {row[1] for row in conn.execute("PRAGMA table_info(day_state)")}
        assert "publication_id" in columns
        db.init_db(conn)  # idempotent
    finally:
        conn.close()


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "api.sqlite3"
    monkeypatch.setenv("TAPS_DB", str(db_file))
    import app.main as main

    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def _post_upload(client, rows, name="taps.csv"):
    return client.post("/api/uploads", files={"file": (name, make_csv(rows), "text/csv")})


def test_api_pages_with_cursor(client):
    assert _post_upload(client, pending_taps(120)).status_code == 200
    body = client.get("/api/days/2024-05-01").json()
    assert body["pending_total"] == 120
    assert len(body["pending"]) == 100
    assert body["page"]["has_more"] is True
    cursor = body["page"]["next_cursor"]

    second = client.get("/api/days/2024-05-01", params={"cursor": cursor})
    assert second.status_code == 200
    page = second.json()
    assert len(page["pending"]) == 20
    assert page["page"]["has_more"] is False
    assert (page["pending"][0]["event_no"], page["pending"][-1]["event_no"]) == ("e0100", "e0119")


def test_api_409_when_publication_changed(client):
    _post_upload(client, pending_taps(120))
    first = client.get("/api/days/2024-05-01").json()

    _post_upload(client, [
        tap("e0000", "c0000", "2024-05-01T00:00:00Z", "Z", "in", revision=1),
    ], name="fix.csv")

    res = client.get("/api/days/2024-05-01", params={"cursor": first["page"]["next_cursor"]})
    assert res.status_code == 409
    body = res.json()
    assert body["code"] == "day_changed"
    assert body["status"] == "current"
    assert body["restart"] is True
    assert body["detail"]

    fresh = client.get("/api/days/2024-05-01").json()
    assert fresh["publication_id"] != first["publication_id"]
    assert fresh["pending_total"] == 120


def test_api_409_when_day_stale(client, monkeypatch):
    _post_upload(client, pending_taps(120))
    first = client.get("/api/days/2024-05-01").json()

    # Accept a correction but stop the inline worker from publishing it.
    import app.main as main

    monkeypatch.setattr(main.service, "run_pending_jobs", lambda *_a, **_k: 0)
    assert _post_upload(client, [
        tap("e0000", "c0000", "2024-05-01T00:00:00Z", "Z", "in", revision=1),
    ], name="fix.csv").status_code == 200

    res = client.get("/api/days/2024-05-01", params={"cursor": first["page"]["next_cursor"]})
    assert res.status_code == 409
    assert res.json()["status"] == "stale"


def test_api_rejects_bad_cursor_and_oversized_limit(client):
    _post_upload(client, pending_taps(120))
    assert client.get("/api/days/2024-05-01", params={"cursor": "not-a-cursor"}).status_code == 400
    assert client.get("/api/days/2024-05-01", params={"limit": 101}).status_code == 422
    assert client.get("/api/days/2024-05-01", params={"limit": 0}).status_code == 422

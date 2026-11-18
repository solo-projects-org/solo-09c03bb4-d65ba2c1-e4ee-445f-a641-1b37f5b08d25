"""End-to-end pipeline behaviour: invalidation, publication, correction, recovery."""

from app import db, service

from helpers import day, make_csv, pairs, state, tap, upload


def test_upload_publishes_and_counts(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
        tap("e3", "c2", "2024-05-01T09:00:00Z", "A", "in"),
        tap("e4", "c2", "2024-05-01T09:30:00Z", "C", "out"),
    ])
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "B"): 1, ("A", "C"): 1}
    assert detail["trip_total"] == 2


def test_acceptance_marks_days_stale_before_recompute(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    assert day(db_path, "2024-05-01")["status"] == "current"

    # Accept *without* running the worker: the old result must already be stale.
    result = service.accept_upload(db_path, "correction.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1),
    ]))
    assert "2024-05-01" in result.affected_days
    stale = day(db_path, "2024-05-01")
    assert stale["status"] == "stale"
    assert stale["station_pairs"] == []          # nothing is served as current
    assert stale["last_error"]


def test_correction_changes_pairing_after_recompute(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    assert pairs(day(db_path, "2024-05-01")) == {("A", "B"): 1}

    upload(db_path, [tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1)])
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "C"): 1}


def test_lower_revision_is_ignored(db_path):
    upload(db_path, [tap("e1", "c1", "2024-05-01T08:20:00Z", "B", "out", revision=2)])
    result = upload(db_path, [tap("e1", "c1", "2024-05-01T08:20:00Z", "Z", "out", revision=1)])
    assert result.inserted == 0
    assert result.ignored == 1
    conn = db.connect(db_path)
    try:
        row = conn.execute("SELECT station, revision FROM events WHERE event_no='e1'").fetchone()
        assert (row["station"], row["revision"]) == ("B", 2)
    finally:
        conn.close()


def test_cross_midnight_correction_updates_both_days_together(db_path):
    # Day 1: entry 23:50 A; day 2: exit 00:20 B -> one A->B trip on 2024-05-01.
    upload(db_path, [
        tap("in1", "c1", "2024-05-01T23:50:00Z", "A", "in"),
        tap("out1", "c1", "2024-05-02T00:20:00Z", "B", "out"),
    ])
    assert pairs(day(db_path, "2024-05-01")) == {("A", "B"): 1}
    assert day(db_path, "2024-05-02")["status"] == "current"
    assert day(db_path, "2024-05-02")["trip_total"] == 0

    # Correct the exit station: only a day-2 event changes, day-1 totals move.
    accepted = service.accept_upload(db_path, "fix.csv", make_csv([
        tap("out1", "c1", "2024-05-02T00:20:00Z", "C", "out", revision=1),
    ]))
    assert set(accepted.affected_days) == {"2024-05-01", "2024-05-02"}
    # Both days are stale in the same instant, before recompute runs.
    assert state(db_path, "2024-05-01")["status"] == "stale"
    assert state(db_path, "2024-05-02")["status"] == "stale"

    service.run_pending_jobs(db_path)
    assert pairs(day(db_path, "2024-05-01")) == {("A", "C"): 1}
    # The two days were published by one transaction: same computed_at and batch.
    s1, s2 = state(db_path, "2024-05-01"), state(db_path, "2024-05-02")
    assert s1["status"] == s2["status"] == "current"
    assert s1["computed_at"] == s2["computed_at"]


def test_failed_recompute_keeps_day_stale(db_path, monkeypatch):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    original = service._publish

    def boom(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr(service, "_publish", boom)
    accepted = service.accept_upload(db_path, "c.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1),
    ]))
    service.run_pending_jobs(db_path)

    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "stale"
    assert "disk full" in detail["last_error"]
    # Job is failed (non-retryable error) and the repair path can re-queue it.
    conn = db.connect(db_path)
    try:
        job = conn.execute("SELECT status FROM recompute_jobs WHERE id = ?", (accepted.job_id,)).fetchone()
        assert job["status"] == "failed"
    finally:
        conn.close()

    monkeypatch.setattr(service, "_publish", original)
    service.request_recompute(db_path, ["2024-05-01"])
    service.run_pending_jobs(db_path)
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "C"): 1}


def test_restart_recovery_requeues_and_never_publishes_old_results(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    # Simulate a crash mid-recompute after accepting new data: day is stale,
    # job left "running", and a newer revision sits in events.
    accepted = service.accept_upload(db_path, "c.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "D", "out", revision=1),
    ]))
    conn = db.connect(db_path)
    try:
        conn.execute("UPDATE recompute_jobs SET status='running' WHERE id=?", (accepted.job_id,))
        # Also fake a corrupt "current" claim with an outdated epoch.
        conn.execute(
            "UPDATE day_state SET status='current', data_epoch=0 WHERE service_date='2024-05-01'"
        )
    finally:
        conn.close()

    service.recover(db_path)
    # recovery demoted the bogus current claim and re-queued the interrupted job
    assert state(db_path, "2024-05-01")["status"] == "stale"
    service.run_pending_jobs(db_path)
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "D"): 1}


def test_recover_requeues_stale_day_without_job(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    conn = db.connect(db_path)
    try:
        # Lose the job and orphan the day -> recovery must repair it.
        conn.execute("DELETE FROM recompute_jobs")
        conn.execute("UPDATE day_state SET status='stale' WHERE service_date='2024-05-01'")
    finally:
        conn.close()
    service.recover(db_path)
    service.run_pending_jobs(db_path)
    assert day(db_path, "2024-05-01")["status"] == "current"


def test_slow_recompute_cannot_overwrite_newer_revision(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    first = service.accept_upload(db_path, "r2.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1),
    ]))

    # While the first job's snapshot is open, a competing upload lands.
    def hook(stage, conn, info):
        if stage == "after_compute" and info["job_id"] == first.job_id:
            service.accept_upload(db_path, "r3.csv", make_csv([
                tap("e2", "c1", "2024-05-02T08:20:00Z", "E", "out", revision=2),
            ]))

    service.run_pending_jobs(db_path, hook=hook)
    # The stale job did not publish; it was re-queued and the newest job wins.
    service.run_pending_jobs(db_path)
    detail = day(db_path, "2024-05-01")
    assert detail["status"] == "current"
    assert pairs(detail) == {("A", "E"): 1}
    assert detail["pending_total"] == 0


def test_targeted_recompute_of_one_side_publishes_both_days(db_path):
    upload(db_path, [
        tap("in1", "c1", "2024-05-01T23:50:00Z", "A", "in"),
        tap("out1", "c1", "2024-05-02T00:20:00Z", "B", "out"),
    ])
    # Invalidating only the entry day must not publish it alone: the pair is
    # linked across midnight, so both days turn current in one transaction.
    service.request_recompute(db_path, ["2024-05-01"])
    service.run_pending_jobs(db_path)
    d1, d2 = day(db_path, "2024-05-01"), day(db_path, "2024-05-02")
    assert d1["status"] == d2["status"] == "current"
    assert pairs(d1) == {("A", "B"): 1}
    assert d2["trip_total"] == 0
    assert state(db_path, "2024-05-01")["computed_at"] == state(db_path, "2024-05-02")["computed_at"]


def test_new_card_on_second_day_does_not_invalidate_first(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    first_computed = state(db_path, "2024-05-01")["computed_at"]

    accepted = service.accept_upload(db_path, "day2.csv", make_csv([
        tap("e3", "c2", "2024-05-02T09:00:00Z", "C", "in"),
        tap("e4", "c2", "2024-05-02T09:30:00Z", "D", "out"),
    ]))
    service.run_pending_jobs(db_path)

    assert accepted.affected_days == ["2024-05-02"]
    assert state(db_path, "2024-05-01")["computed_at"] == first_computed
    assert pairs(day(db_path, "2024-05-02")) == {("C", "D"): 1}


def test_pending_taps_listed_and_not_counted(db_path):
    upload(db_path, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:10:00Z", "B", "in"),
    ])
    detail = day(db_path, "2024-05-01")
    assert detail["trip_total"] == 0
    assert detail["pending_total"] == 2
    assert {p["event_no"] for p in detail["pending"]} == {"e1", "e2"}


def test_two_day_upload_and_span_validation(db_path):
    upload(db_path, [
        tap("a1", "c1", "2024-05-01T23:00:00Z", "A", "in"),
        tap("a2", "c1", "2024-05-02T00:10:00Z", "B", "out"),
        tap("b1", "c2", "2024-05-02T10:00:00Z", "C", "in"),
        tap("b2", "c2", "2024-05-02T10:30:00Z", "D", "out"),
    ])
    assert pairs(day(db_path, "2024-05-01")) == {("A", "B"): 1}
    assert pairs(day(db_path, "2024-05-02")) == {("C", "D"): 1}

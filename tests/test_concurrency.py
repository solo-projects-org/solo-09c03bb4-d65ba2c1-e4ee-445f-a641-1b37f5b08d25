"""Concurrency: parallel uploads must not lose revisions or publish stale data."""

import multiprocessing as mp
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import db, service  # noqa: E402

from helpers import make_csv, tap  # noqa: E402


def _upload_worker(args):
    db_path, filename, payload = args
    result = service.accept_upload(db_path, filename, payload)
    time.sleep(0.01)
    service.run_pending_jobs(db_path)
    return result.batch_id


def test_parallel_revision_uploads_converge_to_highest(db_path):
    # Seed with revision 0.
    service.accept_upload(db_path, "seed.csv", make_csv([
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ]))
    service.run_pending_jobs(db_path)

    payloads = []
    for rev in range(1, 9):
        payloads.append((
            str(db_path),
            f"r{rev}.csv",
            make_csv([tap("e2", "c1", "2024-05-01T08:20:00Z", "S%d" % rev, "out", revision=rev)]),
        ))

    with mp.get_context("spawn").Pool(4) as pool:
        pool.map(_upload_worker, payloads)

    conn = db.connect(db_path)
    try:
        row = conn.execute(
            "SELECT station, revision FROM events WHERE event_no='e2' ORDER BY revision DESC LIMIT 1"
        ).fetchone()
        assert (row["station"], row["revision"]) == ("S8", 8)
    finally:
        conn.close()

    # Whatever state we are in, a final drain must publish the top revision.
    service.recover(db_path)
    service.run_pending_jobs(db_path)
    detail = service.day_detail(db.connect(db_path), "2024-05-01")
    assert detail["status"] == "current"
    assert [(p["from_station"], p["to_station"]) for p in detail["station_pairs"]] == [("A", "S8")]


def test_no_job_left_after_drain(db_path):
    service.accept_upload(db_path, "a.csv", make_csv([
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ]))
    service.run_pending_jobs(db_path)
    service.accept_upload(db_path, "b.csv", make_csv([
        tap("e2", "c1", "2024-05-01T08:20:00Z", "C", "out", revision=1),
    ]))
    service.run_pending_jobs(db_path)
    conn = db.connect(db_path)
    try:
        left = conn.execute(
            "SELECT COUNT(*) AS n FROM recompute_jobs WHERE status IN ('pending','running')"
        ).fetchone()["n"]
        assert left == 0
        stale = conn.execute(
            "SELECT COUNT(*) AS n FROM day_state WHERE status='stale'"
        ).fetchone()["n"]
        assert stale == 0
    finally:
        conn.close()

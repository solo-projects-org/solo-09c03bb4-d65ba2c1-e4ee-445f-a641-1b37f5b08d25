import csv
import io

import pytest

from app import db, service


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "taps.sqlite3"
    conn = db.connect(path)
    db.init_db(conn)
    conn.close()
    return path


def make_csv(rows, header=None):
    header = header or ["事件号", "匿名卡号", "时刻", "站点", "方向", "修订号"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)
    return buf.getvalue().encode("utf-8")


def tap(event_no, card, time, station, direction, revision=0):
    return [event_no, card, time, station, "进站" if direction == "in" else "出站", revision]


def upload(db_path, rows):
    result = service.accept_upload(db_path, "test.csv", make_csv(rows))
    service.run_pending_jobs(db_path)
    return result


def day(db_path, date):
    conn = db.connect(db_path)
    try:
        return service.day_detail(conn, date)
    finally:
        conn.close()


def pairs(detail):
    return {(p["from_station"], p["to_station"]): p["trips"] for p in detail["station_pairs"]}


def state(db_path, date):
    conn = db.connect(db_path)
    try:
        row = conn.execute("SELECT * FROM day_state WHERE service_date = ?", (date,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

import importlib

import pytest
from fastapi.testclient import TestClient

from helpers import make_csv, tap


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "api.sqlite3"
    monkeypatch.setenv("TAPS_DB", str(db_file))
    import app.main as main

    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def upload(client, rows, name="taps.csv"):
    return client.post(
        "/api/uploads",
        files={"file": (name, make_csv(rows), "text/csv")},
    )


def test_index_and_health(client):
    assert client.get("/healthz").json() == {"ok": True}
    page = client.get("/")
    assert page.status_code == 200
    assert "地铁站间客流" in page.text


def test_upload_then_view_day(client):
    res = upload(client, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
        tap("e3", "c2", "2024-05-01T08:30:00Z", "C", "in"),
    ])
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["day_states"] == {"2024-05-01": "current"}

    day = client.get("/api/days/2024-05-01").json()
    assert day["status"] == "current"
    assert day["station_pairs"][0] == {
        "from_station": "A", "to_station": "B", "trips": 1,
        "computed_at": day["computed_at"],
    }
    assert day["pending_total"] == 1

    days = client.get("/api/days").json()["days"]
    assert [d["date"] for d in days] == ["2024-05-01"]


def test_bad_csv_returns_400(client):
    res = client.post(
        "/api/uploads",
        files={"file": ("bad.csv", b"foo,bar\n1,2\n", "text/csv")},
    )
    assert res.status_code == 400
    assert "表头" in res.json()["detail"]


def test_unknown_day_404(client):
    assert client.get("/api/days/2030-01-01").status_code == 404


def test_bad_day_format(client):
    assert client.get("/api/days/not-a-date").status_code == 400


def test_recompute_endpoint(client):
    upload(client, [
        tap("e1", "c1", "2024-05-01T08:00:00Z", "A", "in"),
        tap("e2", "c1", "2024-05-01T08:20:00Z", "B", "out"),
    ])
    res = client.post("/api/recompute?days=2024-05-01")
    assert res.status_code == 200
    assert res.json()["day_states"] == {"2024-05-01": "current"}
    assert client.post("/api/recompute").status_code == 400


def test_cross_midnight_correction_via_api(client):
    upload(client, [
        tap("in1", "c1", "2024-05-01T23:50:00Z", "A", "in"),
        tap("out1", "c1", "2024-05-02T00:20:00Z", "B", "out"),
    ])
    res = upload(client, [tap("out1", "c1", "2024-05-02T00:20:00Z", "D", "out", revision=1)])
    assert set(res.json()["affected_days"]) == {"2024-05-01", "2024-05-02"}
    day1 = client.get("/api/days/2024-05-01").json()
    day2 = client.get("/api/days/2024-05-02").json()
    assert day1["status"] == day2["status"] == "current"
    assert [(p["from_station"], p["to_station"]) for p in day1["station_pairs"]] == [("A", "D")]
    assert day2["trip_total"] == 0

from datetime import datetime, timezone

from app.journeys import Event, REASON_CONSEC_IN, REASON_IN_TRAILING, REASON_OUT_TRAILING, derive
from app.ingest import UploadError, parse_csv


def ev(no, card, t, station, direction, rev=0):
    return Event(no, card, datetime.fromisoformat(t).replace(tzinfo=timezone.utc), station, direction, rev)


def test_pairs_entry_then_exit():
    journeys, pending = derive([
        ev("e1", "c1", "2024-05-01T08:00:00", "A", "in"),
        ev("e2", "c1", "2024-05-01T08:20:00", "B", "out"),
    ])
    assert [(j.from_station, j.to_station, j.service_date) for j in journeys] == [("A", "B", "2024-05-01")]
    assert pending == []


def test_cross_midnight_counted_on_entry_day():
    journeys, pending = derive([
        ev("e1", "c1", "2024-05-01T23:50:00", "A", "in"),
        ev("e2", "c1", "2024-05-02T00:20:00", "B", "out"),
    ])
    assert len(journeys) == 1
    assert journeys[0].service_date == "2024-05-01"
    assert journeys[0].exit_date == "2024-05-02"
    assert pending == []


def test_isolated_and_same_direction_are_pending():
    journeys, pending = derive([
        ev("e1", "c1", "2024-05-01T08:00:00", "A", "in"),
        ev("e2", "c1", "2024-05-01T08:10:00", "B", "in"),   # same direction
        ev("e3", "c1", "2024-05-01T08:20:00", "C", "out"),  # pairs with e2
        ev("e4", "c2", "2024-05-01T09:00:00", "D", "out"),  # isolated exit
        ev("e5", "c3", "2024-05-01T09:00:00", "E", "in"),   # isolated entry
    ])
    assert {(j.entry_event_no, j.exit_event_no) for j in journeys} == {("e2", "e3")}
    reasons = {(p.event_no, p.reason) for p in pending}
    assert ("e1", REASON_CONSEC_IN) in reasons
    assert ("e4", REASON_OUT_TRAILING) in reasons
    assert ("e5", REASON_IN_TRAILING) in reasons


def test_duplicate_revisions_deduplicated_in_file():
    csv_bytes = (
        "事件号,匿名卡号,时刻,站点,方向,修订号\n"
        "e1,c1,2024-05-01T08:00:00Z,A,进站,0\n"
        "e1,c1,2024-05-01T08:00:00Z,A,进站,0\n"
    ).encode()
    taps = parse_csv(csv_bytes)
    assert len(taps) == 1


def test_upload_span_limit():
    csv_bytes = (
        "事件号,匿名卡号,时刻,站点,方向,修订号\n"
        "e1,c1,2024-05-01T08:00:00Z,A,进站,0\n"
        "e2,c1,2024-05-03T08:00:00Z,A,出站,0\n"
    ).encode()
    try:
        parse_csv(csv_bytes)
    except UploadError as exc:
        assert "连续" in str(exc)
    else:
        raise AssertionError("expected UploadError")


def test_bad_direction_rejected():
    csv_bytes = b"event_no,card_id,tap_time,station,direction,revision\ne1,c1,2024-05-01T08:00:00Z,A,sideways,0\n"
    try:
        parse_csv(csv_bytes)
    except UploadError as exc:
        assert "方向" in str(exc)
    else:
        raise AssertionError("expected UploadError")

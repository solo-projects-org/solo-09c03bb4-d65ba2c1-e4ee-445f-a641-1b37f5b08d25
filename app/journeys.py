"""Gate-tap pairing: turn per-card tap sequences into journeys.

Rule (as specified by operations): within one anonymous card, walk the taps in
time order and pair an entry ("进站") with the *next* tap when that tap is an
exit ("出站").  The journey belongs to the UTC day of its entry tap, so a trip
that starts at 23:50 and ends at 00:20 is counted on the entry day.

Anything that cannot be paired is reported for manual review and is never
counted as a journey:

* a tap at the end of the card's history with no partner ("孤立"),
* a tap followed by another tap in the same direction ("连续同向"),
* an exit that is followed by an entry (an exit can only close a trip, and
  taps are chronological, so it is an isolated exit).

The functions here are pure: callers supply events already reduced to their
latest revision.  They are used both by the upload transaction (to diff old
and new derivations and find affected service dates) and by the recompute
worker (to produce the counts that get published).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Event:
    event_no: str
    card_id: str
    tap_time: datetime
    station: str
    direction: str          # "in" | "out"
    revision: int


@dataclass(frozen=True)
class Journey:
    card_id: str
    entry_event_no: str
    exit_event_no: str
    service_date: str       # UTC day of the entry tap
    exit_date: str          # UTC day of the exit tap (may be the next day)
    from_station: str
    to_station: str

    @property
    def crosses_midnight(self) -> bool:
        return self.exit_date != self.service_date


@dataclass(frozen=True)
class PendingTap:
    card_id: str
    event_no: str
    service_date: str       # UTC day of the tap itself
    tap_time: datetime
    station: str
    direction: str
    reason: str

    @property
    def day(self) -> str:
        return self.service_date


REASON_IN_TRAILING = "孤立进站：之后没有出站记录"
REASON_OUT_TRAILING = "孤立出站：之后没有进站记录"
REASON_OUT_BEFORE_IN = "孤立出站：下一笔仍为进站，无法与其配对"
REASON_CONSEC_IN = "连续同向进站：前一笔记为进站，未形成配对"
REASON_CONSEC_OUT = "连续同向出站：前一笔记为出站，未形成配对"


def derive(events: list[Event]) -> tuple[list[Journey], list[PendingTap]]:
    """Pair taps per card; return (journeys, pending_taps)."""
    by_card: dict[str, list[Event]] = {}
    for event in events:
        by_card.setdefault(event.card_id, []).append(event)

    journeys: list[Journey] = []
    pending: list[PendingTap] = []

    for card_id, card_events in by_card.items():
        card_events.sort(key=lambda e: (e.tap_time, e.event_no))
        current: Event | None = None
        for event in card_events:
            if current is None:
                current = event
                continue
            if current.direction == "in" and event.direction == "out":
                journeys.append(
                    Journey(
                        card_id=card_id,
                        entry_event_no=current.event_no,
                        exit_event_no=event.event_no,
                        service_date=current.tap_time.strftime("%Y-%m-%d"),
                        exit_date=event.tap_time.strftime("%Y-%m-%d"),
                        from_station=current.station,
                        to_station=event.station,
                    )
                )
                current = None
            elif current.direction == "out" and event.direction == "in":
                pending.append(_pending(current, card_id, REASON_OUT_BEFORE_IN))
                current = event
            elif current.direction == "in":
                pending.append(_pending(current, card_id, REASON_CONSEC_IN))
                current = event
            else:
                pending.append(_pending(current, card_id, REASON_CONSEC_OUT))
                current = event
        if current is not None:
            reason = REASON_IN_TRAILING if current.direction == "in" else REASON_OUT_TRAILING
            pending.append(_pending(current, card_id, reason))

    return journeys, pending


def _pending(event: Event, card_id: str, reason: str) -> PendingTap:
    return PendingTap(
        card_id=card_id,
        event_no=event.event_no,
        service_date=event.tap_time.strftime("%Y-%m-%d"),
        tap_time=event.tap_time,
        station=event.station,
        direction=event.direction,
        reason=reason,
    )

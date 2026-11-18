"""CSV parsing for gate tap uploads.

Accepted header (case/space insensitive, order free, Chinese or English):

    事件号, 匿名卡号, 时刻, 站点, 方向, 修订号
    event_no, card_id, tap_time, station, direction, revision

Direction accepts 进站/in/entry and 出站/out/exit.  A file is split into
independent *sessions* (one per UTC day) so that "连续两个 UTC 日" uploads
are accepted while anything longer is rejected.

Parsing is deliberately forgiving about BOM/blank lines and strict about
values: a row that cannot be understood fails the whole upload with a
line-numbered message, because a silently dropped tap would corrupt journeys.
Duplicate ``(event_no, revision)`` rows inside one file are tolerated only if
byte-identical; conflicting duplicates are rejected.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from datetime import datetime, timezone

MAX_ROWS = 200_000
MAX_SPAN_DAYS = 2

HEADER_ALIASES = {
    "event_no": {"事件号", "event_no", "eventno", "event id", "event_id"},
    "card_id": {"匿名卡号", "卡号", "card_id", "card", "card_no", "cardno"},
    "tap_time": {"时刻", "时间", "tap_time", "time", "timestamp", "tapped_at"},
    "station": {"站点", "车站", "station", "station_name"},
    "direction": {"方向", "进出站方向", "direction", "in_out", "type"},
    "revision": {"修订号", "revision", "rev", "revision_no"},
}
REQUIRED = ("event_no", "card_id", "tap_time", "station", "direction", "revision")

IN_WORDS = {"in", "entry", "enter", "进", "进站", "入", "入站"}
OUT_WORDS = {"out", "exit", "出", "出站"}


class UploadError(ValueError):
    """A user-correctable upload problem (reported with HTTP 400)."""


@dataclass(frozen=True)
class Tap:
    event_no: str
    card_id: str
    tap_time: datetime          # timezone-aware UTC
    station: str
    direction: str              # "in" | "out"
    revision: int
    line: int

    @property
    def day(self) -> str:
        return self.tap_time.strftime("%Y-%m-%d")

    @property
    def iso(self) -> str:
        return self.tap_time.strftime("%Y-%m-%dT%H:%M:%SZ")


def _norm_header(name: str) -> str:
    return name.strip().lstrip("﻿").strip().lower().replace(" ", "")


def _map_header(raw: list[str]) -> dict[int, str]:
    normalised = {_norm_header(h): i for i, h in enumerate(raw) if h is not None}
    mapping: dict[int, str] = {}
    for field, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            idx = normalised.get(_norm_header(alias))
            if idx is not None:
                mapping[idx] = field
                break
    present = set(mapping.values())
    missing = [f for f in REQUIRED if f not in present]
    if missing:
        raise UploadError(
            "表头缺少必需列：" + ", ".join(missing) + "（需要 事件号/匿名卡号/时刻/站点/方向/修订号）"
        )
    return mapping


def _parse_time(value: str, line: int) -> datetime:
    text = value.strip()
    if not text:
        raise UploadError(f"第 {line} 行：时刻为空")
    # Accept trailing Z, explicit offsets, or naive (interpreted as UTC).
    candidate = text.replace("/", "-")
    if candidate.endswith("Z") or candidate.endswith("z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M"):
            try:
                dt = datetime.strptime(candidate, fmt)
                break
            except ValueError:
                continue
        else:
            raise UploadError(f"第 {line} 行：无法解析时刻 {text!r}（需 ISO-8601 UTC）")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def _parse_direction(value: str, line: int) -> str:
    key = value.strip().lower().replace(" ", "")
    if key in IN_WORDS:
        return "in"
    if key in OUT_WORDS:
        return "out"
    raise UploadError(f"第 {line} 行：方向 {value!r} 无法识别（应为 进站/出站 或 in/out）")


def parse_csv(payload: bytes, filename: str = "upload.csv") -> list[Tap]:
    if not payload.strip():
        raise UploadError("文件为空")
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = payload.decode("gb18030")
        except UnicodeDecodeError:
            raise UploadError("无法解码文件，请使用 UTF-8 或 GB18030 编码")

    reader = csv.reader(io.StringIO(text))
    try:
        raw_header = next(reader)
    except StopIteration:
        raise UploadError("文件为空")

    mapping = _map_header(raw_header)
    taps: list[Tap] = []
    seen: dict[tuple[str, int], Tap] = {}

    for offset, row in enumerate(reader, start=2):
        if not row or all(not (c or "").strip() for c in row):
            continue
        if len(taps) >= MAX_ROWS:
            raise UploadError(f"文件超过 {MAX_ROWS} 行上限，请拆分后上传")
        fields = {name: (row[i].strip() if i < len(row) else "") for i, name in mapping.items()}

        event_no = fields["event_no"]
        card_id = fields["card_id"]
        station = fields["station"]
        if not event_no:
            raise UploadError(f"第 {offset} 行：事件号为空")
        if not card_id:
            raise UploadError(f"第 {offset} 行：匿名卡号为空")
        if not station:
            raise UploadError(f"第 {offset} 行：站点为空")

        raw_rev = fields["revision"]
        try:
            revision = int(raw_rev)
        except (TypeError, ValueError):
            raise UploadError(f"第 {offset} 行：修订号 {raw_rev!r} 不是整数")
        if revision < 0:
            raise UploadError(f"第 {offset} 行：修订号不能为负")

        tap = Tap(
            event_no=event_no,
            card_id=card_id,
            tap_time=_parse_time(fields["tap_time"], offset),
            station=station,
            direction=_parse_direction(fields["direction"], offset),
            revision=revision,
            line=offset,
        )

        key = (tap.event_no, tap.revision)
        if key in seen:
            prior = seen[key]
            if (prior.card_id, prior.tap_time, prior.station, prior.direction) != (
                tap.card_id, tap.tap_time, tap.station, tap.direction
            ):
                raise UploadError(
                    f"第 {offset} 行：事件号 {event_no} 修订号 {revision} 在本文件内重复且内容不一致"
                )
            continue
        seen[key] = tap
        taps.append(tap)

    if not taps:
        raise UploadError("文件没有有效数据行")

    days = sorted({t.day for t in taps})
    _check_span(days)
    return taps


def _check_span(days: list[str]) -> None:
    parsed = [datetime.strptime(d, "%Y-%m-%d").date() for d in days]
    if len(days) > MAX_SPAN_DAYS:
        raise UploadError(
            f"一次只能上传最多 {MAX_SPAN_DAYS} 个连续 UTC 日的数据，本次包含 {len(days)} 天："
            + ", ".join(days)
        )
    span = (parsed[-1] - parsed[0]).days + 1
    if span > MAX_SPAN_DAYS:
        raise UploadError(
            f"数据必须落在连续 {MAX_SPAN_DAYS} 个 UTC 日内，本次跨越 {span} 天：{days[0]} … {days[-1]}"
        )


def session_windows(taps: list[Tap]) -> list[tuple[str, list[Tap]]]:
    """Split taps into contiguous UTC-day sessions (kept for UI/diagnostics)."""
    by_day: dict[str, list[Tap]] = {}
    for tap in taps:
        by_day.setdefault(tap.day, []).append(tap)
    return sorted(by_day.items())

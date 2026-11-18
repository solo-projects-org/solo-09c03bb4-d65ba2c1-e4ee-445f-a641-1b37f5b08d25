"""FastAPI app for the station-to-station ridership sheet.

Routes
------
GET  /                      dashboard (day list, uploads, jobs)
GET  /api/days             list of known service dates with publication state
GET  /api/days/{date}      station-pair counts + pending taps for one date
POST /api/uploads          multipart CSV upload; accepted, dates invalidated,
                           then recompute is attempted inline
POST /api/recompute        re-queue dates (repair after a failed recompute)
GET  /api/overview         batches, jobs and day states
GET  /healthz              liveness
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager, closing
from zoneinfo import ZoneInfo

from fastapi import FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import db, service
from .ingest import UploadError

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAX_UPLOAD_BYTES = 25 * 1024 * 1024


def _db_path() -> str:
    return os.environ.get("TAPS_DB", os.path.join(BASE_DIR, "..", "data", "taps.sqlite3"))


def _local_tz() -> ZoneInfo:
    return ZoneInfo(os.environ.get("DISPLAY_TZ", "Asia/Shanghai"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    conn = db.connect(_db_path())
    try:
        db.init_db(conn)
    finally:
        conn.close()
    service.recover(_db_path())
    service.run_pending_jobs(_db_path())
    yield


app = FastAPI(title="地铁站间客流", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=os.path.join(BASE_DIR, "static")), name="static")
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


def _conn():
    return closing(db.connect(_db_path()))


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    with _conn() as conn:
        days = service.known_days(conn)
        data = service.overview(conn)
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "days": days,
            "jobs": data["jobs"],
            "job_counts": data["job_counts"],
            "batches": data["batches"],
            "tz": str(_local_tz()),
        },
    )


@app.get("/api/days")
def api_days():
    with _conn() as conn:
        return {"days": service.known_days(conn)}


@app.get("/api/days/{day}")
def api_day(day: str):
    try:
        from datetime import date as date_cls

        date_cls.fromisoformat(day)
    except ValueError:
        raise HTTPException(status_code=400, detail="日期格式应为 YYYY-MM-DD")
    with _conn() as conn:
        known = conn.execute(
            "SELECT 1 FROM day_state WHERE service_date = ?", (day,)
        ).fetchone()
        if not known:
            raise HTTPException(status_code=404, detail=f"没有 {day} 的数据")
        return service.day_detail(conn, day)


@app.get("/api/overview")
def api_overview():
    with _conn() as conn:
        return service.overview(conn)


@app.post("/api/uploads")
def upload(file: UploadFile = File(...)):
    payload = file.file.read(MAX_UPLOAD_BYTES + 1)
    if len(payload) > MAX_UPLOAD_BYTES:
        raise HTTPException(status_code=413, detail="文件超过 25 MB 上限")
    try:
        accepted = service.accept_upload(_db_path(), file.filename or "upload.csv", payload)
    except UploadError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Acceptance is already durable: affected dates are stale at this point.
    service.run_pending_jobs(_db_path())

    with _conn() as conn:
        states = {}
        if accepted.affected_days:
            states = {
                row["service_date"]: row["status"]
                for row in conn.execute(
                    "SELECT service_date, status FROM day_state WHERE service_date IN (%s)"
                    % ",".join("?" for _ in accepted.affected_days),
                    accepted.affected_days,
                )
            }
        detail = service.day_detail(conn, accepted.affected_days[0]) if accepted.affected_days else None
    return {
        "batch_id": accepted.batch_id,
        "rows": accepted.row_count,
        "inserted": accepted.inserted,
        "ignored": accepted.ignored,
        "affected_days": accepted.affected_days,
        "day_states": states,
        "first_day": detail,
    }


@app.post("/api/recompute")
def recompute(days: list[str] = Query(default=[])):
    if not days:
        raise HTTPException(status_code=400, detail="请指定要重算的日期")
    for day in days:
        try:
            from datetime import date as date_cls

            date_cls.fromisoformat(day)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"日期格式错误：{day}")
    job_id = service.request_recompute(_db_path(), days)
    service.run_pending_jobs(_db_path())
    with _conn() as conn:
        states = {
            row["service_date"]: row["status"]
            for row in conn.execute(
                "SELECT service_date, status FROM day_state WHERE service_date IN (%s)"
                % ",".join("?" for _ in days),
                days,
            )
        }
    return {"job_id": job_id, "day_states": states}


@app.exception_handler(UploadError)
def upload_error_handler(request: Request, exc: UploadError):
    return JSONResponse(status_code=400, content={"detail": str(exc)})

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))
os.environ.setdefault("TAPS_DB", str(Path(__file__).resolve().parents[1] / "data" / "test.sqlite3"))

from app import db  # noqa: E402


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "taps.sqlite3"
    conn = db.connect(path)
    db.init_db(conn)
    conn.close()
    return path

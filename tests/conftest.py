import sqlite3

import pytest

from db.repositories.sessions_repo import init_db


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "test_trackday.db"
    init_db(str(path))
    return str(path)


@pytest.fixture()
def db_conn(db_path):
    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()


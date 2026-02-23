import sqlite3
from pathlib import Path


def get_conn(db_path: str = "trackday.db") -> sqlite3.Connection:
    path = Path(db_path)
    return sqlite3.connect(str(path))


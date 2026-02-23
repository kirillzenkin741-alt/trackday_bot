import sqlite3
from pathlib import Path

from config import load_settings


REQUIRED_TABLES = {
    "sessions",
    "tracks",
    "votes",
    "points",
    "themes",
    "nominations",
    "session_nominations",
    "nomination_votes",
    "session_main_winner",
    "session_nomination_winners",
    "point_events",
}


def main() -> int:
    settings = load_settings()
    db_path = Path("trackday.db")
    if not db_path.exists():
        print("healthcheck: trackday.db not found")
        return 1

    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {row[0] for row in c.fetchall()}
    conn.close()

    missing = sorted(REQUIRED_TABLES - existing)
    if missing:
        print(f"healthcheck: missing tables: {', '.join(missing)}")
        return 1

    print(
        "healthcheck: ok "
        f"(group_id={settings.group_id}, admin_id={settings.admin_id}, db={db_path.resolve()})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


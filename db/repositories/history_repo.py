from datetime import datetime

from db.connection import get_conn


def save_main_winner(session_id: int, track_id: int, votes: int, db_path: str = "trackday.db") -> None:
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute(
        """
        INSERT OR REPLACE INTO session_main_winner (session_id, track_id, votes, recorded_at)
        VALUES (?, ?, ?, ?)
        """,
        (session_id, track_id, votes, datetime.now().isoformat()),
    )
    conn.commit()
    conn.close()


def get_history_page(page: int = 0, per_page: int = 10, db_path: str = "trackday.db"):
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM session_main_winner")
    total = c.fetchone()[0]
    offset = page * per_page
    c.execute(
        """
        SELECT s.week, s.theme, t.full_name, t.track_url, w.votes
        FROM session_main_winner w
        JOIN sessions s ON s.id = w.session_id
        JOIN tracks t ON t.id = w.track_id
        ORDER BY s.week DESC
        LIMIT ? OFFSET ?
        """,
        (per_page, offset),
    )
    rows = c.fetchall()
    conn.close()
    return rows, total


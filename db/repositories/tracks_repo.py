from datetime import datetime

from db.connection import get_conn


def add_track(
    session_id: int,
    user_id: int,
    username: str,
    full_name: str,
    track_url: str,
    description: str = "",
    db_path: str = "trackday.db",
) -> int:
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute(
        """
        INSERT INTO tracks (session_id, user_id, username, full_name, track_url, track_description, submitted_at, song_links)
        VALUES (?, ?, ?, ?, ?, ?, ?, '')
        """,
        (session_id, user_id, username, full_name, track_url, description, datetime.now().isoformat()),
    )
    conn.commit()
    track_id = c.lastrowid
    conn.close()
    return track_id


def get_track_user_id(track_id: int, db_path: str = "trackday.db"):
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute("SELECT user_id FROM tracks WHERE id = ?", (track_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else None


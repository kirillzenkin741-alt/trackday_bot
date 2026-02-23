from datetime import datetime

from db.connection import get_conn


def add_main_vote(session_id: int, voter_id: int, track_id: int, db_path: str = "trackday.db") -> str:
    conn = get_conn(db_path)
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute("SELECT id FROM votes WHERE session_id = ? AND voter_id = ?", (session_id, voter_id))
    row = c.fetchone()
    if row:
        c.execute("UPDATE votes SET track_id = ?, voted_at = ? WHERE id = ?", (track_id, now, row[0]))
        conn.commit()
        conn.close()
        return "updated"
    c.execute(
        "INSERT INTO votes (session_id, voter_id, track_id, voted_at) VALUES (?, ?, ?, ?)",
        (session_id, voter_id, track_id, now),
    )
    conn.commit()
    conn.close()
    return "new"


def add_nomination_vote(
    session_id: int,
    nomination_id: int,
    voter_id: int,
    track_id: int,
    db_path: str = "trackday.db",
) -> str:
    conn = get_conn(db_path)
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute(
        "SELECT id FROM nomination_votes WHERE session_id = ? AND nomination_id = ? AND voter_id = ?",
        (session_id, nomination_id, voter_id),
    )
    row = c.fetchone()
    if row:
        c.execute("UPDATE nomination_votes SET track_id = ?, updated_at = ? WHERE id = ?", (track_id, now, row[0]))
        conn.commit()
        conn.close()
        return "updated"
    c.execute(
        """
        INSERT INTO nomination_votes (session_id, nomination_id, voter_id, track_id, voted_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (session_id, nomination_id, voter_id, track_id, now, now),
    )
    conn.commit()
    conn.close()
    return "new"


def user_completed_all_nominations(session_id: int, voter_id: int, db_path: str = "trackday.db") -> bool:
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM session_nominations WHERE session_id = ?", (session_id,))
    total = c.fetchone()[0]
    if total == 0:
        conn.close()
        return True
    c.execute(
        "SELECT COUNT(*) FROM nomination_votes WHERE session_id = ? AND voter_id = ?",
        (session_id, voter_id),
    )
    voted = c.fetchone()[0]
    conn.close()
    return voted >= total


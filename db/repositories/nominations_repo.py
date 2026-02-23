from datetime import datetime

from db.connection import get_conn


def add_nomination(name: str, db_path: str = "trackday.db") -> bool:
    conn = get_conn(db_path)
    c = conn.cursor()
    try:
        c.execute(
            "INSERT INTO nominations (name, is_active, created_at) VALUES (?, 1, ?)",
            (name, datetime.now().isoformat()),
        )
        conn.commit()
        return True
    except Exception:
        return False
    finally:
        conn.close()


def create_session_snapshot(session_id: int, db_path: str = "trackday.db"):
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM session_nominations WHERE session_id = ?", (session_id,))
    if c.fetchone()[0] > 0:
        c.execute(
            "SELECT nomination_id, nomination_name_snapshot FROM session_nominations WHERE session_id = ? ORDER BY nomination_id",
            (session_id,),
        )
        rows = c.fetchall()
        conn.close()
        return rows

    c.execute("SELECT id, name FROM nominations WHERE is_active = 1 ORDER BY id")
    active = c.fetchall()
    for nomination_id, name in active:
        c.execute(
            "INSERT INTO session_nominations (session_id, nomination_id, nomination_name_snapshot) VALUES (?, ?, ?)",
            (session_id, nomination_id, name),
        )
    conn.commit()
    c.execute(
        "SELECT nomination_id, nomination_name_snapshot FROM session_nominations WHERE session_id = ? ORDER BY nomination_id",
        (session_id,),
    )
    rows = c.fetchall()
    conn.close()
    return rows


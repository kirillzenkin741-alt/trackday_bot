from datetime import datetime

from db.connection import get_conn


def update_points(
    user_id: int,
    username: str,
    full_name: str,
    points_delta: int,
    is_win: bool = False,
    is_participation: bool = False,
    db_path: str = "trackday.db",
) -> None:
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute("SELECT user_id FROM points WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    wins_delta = 1 if is_win else 0
    part_delta = 1 if is_participation else 0
    if row:
        c.execute(
            """
            UPDATE points
            SET total_points = total_points + ?,
                wins = wins + ?,
                participations = participations + ?,
                username = ?,
                full_name = ?
            WHERE user_id = ?
            """,
            (points_delta, wins_delta, part_delta, username, full_name, user_id),
        )
    else:
        c.execute(
            """
            INSERT INTO points (user_id, username, full_name, total_points, wins, participations)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, username, full_name, points_delta, wins_delta, part_delta),
        )
    conn.commit()
    conn.close()


def apply_points_event(
    user_id: int,
    username: str,
    full_name: str,
    points_delta: int,
    event_key: str,
    event_type: str,
    session_id=None,
    is_win: bool = False,
    is_participation: bool = False,
    db_path: str = "trackday.db",
) -> bool:
    conn = get_conn(db_path)
    c = conn.cursor()
    try:
        c.execute(
            """
            INSERT INTO point_events (event_key, session_id, user_id, event_type, points, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (event_key, session_id, user_id, event_type, points_delta, datetime.now().isoformat()),
        )
        conn.commit()
    except Exception:
        conn.close()
        return False
    conn.close()
    update_points(
        user_id,
        username,
        full_name,
        points_delta,
        is_win=is_win,
        is_participation=is_participation,
        db_path=db_path,
    )
    return True


def get_total_points(user_id: int, db_path: str = "trackday.db") -> int:
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute("SELECT total_points FROM points WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return row[0] if row else 0


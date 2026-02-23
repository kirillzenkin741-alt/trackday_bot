from datetime import datetime

from db.connection import get_conn


def init_db(db_path: str = "trackday.db") -> None:
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            week TEXT UNIQUE,
            theme TEXT,
            state TEXT DEFAULT 'collecting',
            created_at TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS tracks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            user_id INTEGER,
            username TEXT,
            full_name TEXT,
            track_url TEXT,
            track_description TEXT,
            submitted_at TEXT,
            song_links TEXT DEFAULT ''
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            voter_id INTEGER,
            track_id INTEGER,
            voted_at TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS nomination_votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER NOT NULL,
            nomination_id INTEGER NOT NULL,
            voter_id INTEGER NOT NULL,
            track_id INTEGER NOT NULL,
            voted_at TEXT NOT NULL,
            updated_at TEXT
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS session_nominations (
            session_id INTEGER NOT NULL,
            nomination_id INTEGER NOT NULL,
            nomination_name_snapshot TEXT NOT NULL,
            PRIMARY KEY (session_id, nomination_id)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS nominations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS points (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            total_points INTEGER DEFAULT 0,
            wins INTEGER DEFAULT 0,
            participations INTEGER DEFAULT 0
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS point_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_key TEXT UNIQUE NOT NULL,
            session_id INTEGER,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            points INTEGER NOT NULL,
            created_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS session_main_winner (
            session_id INTEGER PRIMARY KEY,
            track_id INTEGER NOT NULL,
            votes INTEGER NOT NULL,
            recorded_at TEXT NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS session_nomination_winners (
            session_id INTEGER NOT NULL,
            nomination_id INTEGER NOT NULL,
            nomination_name_snapshot TEXT NOT NULL,
            track_id INTEGER NOT NULL,
            votes INTEGER NOT NULL,
            recorded_at TEXT NOT NULL,
            PRIMARY KEY (session_id, nomination_id)
        )
        """
    )
    c.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_votes_session_voter ON votes(session_id, voter_id)")
    c.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_nom_votes_session_nom_voter ON nomination_votes(session_id, nomination_id, voter_id)"
    )
    conn.commit()
    conn.close()


def create_session(week: str, theme: str, db_path: str = "trackday.db") -> int:
    conn = get_conn(db_path)
    c = conn.cursor()
    now = datetime.now().isoformat()
    c.execute(
        "INSERT INTO sessions (week, theme, state, created_at) VALUES (?, ?, 'collecting', ?)",
        (week, theme, now),
    )
    conn.commit()
    session_id = c.lastrowid
    conn.close()
    return session_id


def update_session_state(session_id: int, state: str, db_path: str = "trackday.db") -> None:
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute("UPDATE sessions SET state = ? WHERE id = ?", (state, session_id))
    conn.commit()
    conn.close()


from db.connection import get_conn
from db.repositories.history_repo import get_history_page, save_main_winner
from db.repositories.sessions_repo import create_session, init_db
from db.repositories.tracks_repo import add_track


def test_init_db_creates_required_tables_and_indexes(db_path):
    init_db(db_path)
    conn = get_conn(db_path)
    c = conn.cursor()
    c.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = {row[0] for row in c.fetchall()}
    assert "sessions" in tables
    assert "tracks" in tables
    assert "votes" in tables
    assert "nomination_votes" in tables
    assert "point_events" in tables
    assert "session_main_winner" in tables

    c.execute("PRAGMA index_list(votes)")
    vote_indexes = {row[1] for row in c.fetchall()}
    assert "ux_votes_session_voter" in vote_indexes

    c.execute("PRAGMA index_list(nomination_votes)")
    nom_vote_indexes = {row[1] for row in c.fetchall()}
    assert "ux_nom_votes_session_nom_voter" in nom_vote_indexes
    conn.close()


def test_history_page_desc_order(db_path):
    s1 = create_session("2026-W20", "Theme A", db_path=db_path)
    s2 = create_session("2026-W21", "Theme B", db_path=db_path)
    t1 = add_track(s1, 1, "u1", "User 1", "https://t1", db_path=db_path)
    t2 = add_track(s2, 2, "u2", "User 2", "https://t2", db_path=db_path)
    save_main_winner(s1, t1, 3, db_path=db_path)
    save_main_winner(s2, t2, 5, db_path=db_path)

    rows, total = get_history_page(page=0, per_page=10, db_path=db_path)
    assert total == 2
    assert rows[0][0] == "2026-W21"
    assert rows[1][0] == "2026-W20"


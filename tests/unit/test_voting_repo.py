from db.repositories.nominations_repo import add_nomination, create_session_snapshot
from db.repositories.sessions_repo import create_session
from db.repositories.tracks_repo import add_track
from db.repositories.votes_repo import add_main_vote, add_nomination_vote, user_completed_all_nominations


def test_main_vote_upsert(db_path):
    session_id = create_session("2026-W10", "Theme", db_path=db_path)
    t1 = add_track(session_id, 1, "u1", "User 1", "https://t1", db_path=db_path)
    t2 = add_track(session_id, 2, "u2", "User 2", "https://t2", db_path=db_path)

    assert add_main_vote(session_id, voter_id=99, track_id=t1, db_path=db_path) == "new"
    assert add_main_vote(session_id, voter_id=99, track_id=t2, db_path=db_path) == "updated"


def test_nomination_vote_upsert_and_gate(db_path):
    session_id = create_session("2026-W11", "Theme", db_path=db_path)
    t1 = add_track(session_id, 1, "u1", "User 1", "https://t1", db_path=db_path)
    t2 = add_track(session_id, 2, "u2", "User 2", "https://t2", db_path=db_path)
    assert add_nomination("Best mood", db_path=db_path)
    assert add_nomination("Best bass", db_path=db_path)
    snapshot = create_session_snapshot(session_id, db_path=db_path)
    assert len(snapshot) == 2

    assert user_completed_all_nominations(session_id, 77, db_path=db_path) is False
    nom1 = snapshot[0][0]
    nom2 = snapshot[1][0]
    assert add_nomination_vote(session_id, nom1, 77, t1, db_path=db_path) == "new"
    assert user_completed_all_nominations(session_id, 77, db_path=db_path) is False
    assert add_nomination_vote(session_id, nom2, 77, t2, db_path=db_path) == "new"
    assert user_completed_all_nominations(session_id, 77, db_path=db_path) is True
    assert add_nomination_vote(session_id, nom2, 77, t1, db_path=db_path) == "updated"


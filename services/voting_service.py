from db.repositories.nominations_repo import create_session_snapshot
from db.repositories.votes_repo import add_main_vote, add_nomination_vote, user_completed_all_nominations


def create_nomination_snapshot(session_id: int, db_path: str = "trackday.db"):
    return create_session_snapshot(session_id, db_path=db_path)


def vote_main(session_id: int, voter_id: int, track_id: int, db_path: str = "trackday.db") -> str:
    return add_main_vote(session_id, voter_id, track_id, db_path=db_path)


def vote_nomination(
    session_id: int, nomination_id: int, voter_id: int, track_id: int, db_path: str = "trackday.db"
) -> str:
    return add_nomination_vote(session_id, nomination_id, voter_id, track_id, db_path=db_path)


def is_nomination_stage_complete(session_id: int, voter_id: int, db_path: str = "trackday.db") -> bool:
    return user_completed_all_nominations(session_id, voter_id, db_path=db_path)


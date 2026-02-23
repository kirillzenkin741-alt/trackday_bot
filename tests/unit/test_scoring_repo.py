from db.repositories.points_repo import apply_points_event, get_total_points


def test_apply_points_event_idempotent(db_path):
    event_key = "main_vote:10:100"
    assert apply_points_event(
        user_id=100,
        username="u100",
        full_name="User 100",
        points_delta=1,
        event_key=event_key,
        event_type="main_vote",
        session_id=10,
        db_path=db_path,
    )
    assert get_total_points(100, db_path=db_path) == 1

    assert not apply_points_event(
        user_id=100,
        username="u100",
        full_name="User 100",
        points_delta=1,
        event_key=event_key,
        event_type="main_vote",
        session_id=10,
        db_path=db_path,
    )
    assert get_total_points(100, db_path=db_path) == 1


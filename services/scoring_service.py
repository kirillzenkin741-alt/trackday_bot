from db.repositories.points_repo import apply_points_event as repo_apply_points_event


def apply_points_event(*args, **kwargs):
    return repo_apply_points_event(*args, **kwargs)


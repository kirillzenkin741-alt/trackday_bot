from db.repositories.history_repo import get_history_page as repo_get_history_page


def get_history_page(page: int = 0, per_page: int = 10, db_path: str = "trackday.db"):
    return repo_get_history_page(page=page, per_page=per_page, db_path=db_path)


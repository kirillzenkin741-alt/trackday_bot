from aiogram.fsm.state import State, StatesGroup


class ThemeStates(StatesGroup):
    waiting_theme_admin = State()
    waiting_theme_user = State()
    waiting_delete_theme_id = State()
    waiting_toggle_theme_id = State()


class NominationStates(StatesGroup):
    waiting_nomination_name = State()
    waiting_nomination_points = State()
    waiting_nomination_delete_id = State()


class SubmitStates(StatesGroup):
    waiting_track_input = State()
    waiting_candidate_choice = State()
    waiting_confirmation = State()

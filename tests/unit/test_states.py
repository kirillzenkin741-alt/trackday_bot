from states import NominationStates, SubmitStates, ThemeStates


def test_states_declared():
    assert ThemeStates.waiting_theme_admin.state.endswith("waiting_theme_admin")
    assert ThemeStates.waiting_theme_user.state.endswith("waiting_theme_user")
    assert ThemeStates.waiting_delete_theme_id.state.endswith("waiting_delete_theme_id")
    assert NominationStates.waiting_nomination_name.state.endswith("waiting_nomination_name")
    assert NominationStates.waiting_nomination_delete_id.state.endswith("waiting_nomination_delete_id")
    assert SubmitStates.waiting_track_input.state.endswith("waiting_track_input")


"""Tests that promote() fails the build when any asset push actually fails."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from ol_superset.commands.promote import promote

ASSET_COUNTS = {
    "published_dashboards": 1,
    "dashboards": 1,
    "charts": 1,
    "datasets": 1,
}


def _completed(returncode: int) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=[], returncode=returncode)


@pytest.fixture(autouse=True)
def _common_mocks(tmp_path: Path):
    """Stub out everything promote() touches except run_sup_command."""
    assets_dir = tmp_path / "assets"
    assets_dir.mkdir()
    with (
        patch("ol_superset.commands.promote.get_assets_dir", return_value=assets_dir),
        patch("ol_superset.commands.promote.count_assets", return_value=ASSET_COUNTS),
        patch(
            "ol_superset.commands.promote.check_git_status",
            return_value=(False, []),
        ),
        patch("ol_superset.commands.promote.map_database_uuids"),
        patch("ol_superset.commands.promote.sync_physical_dataset_connections"),
    ):
        yield


def _run_sup_side_effect(success_steps: set[str]):
    def _side_effect(args, check=False):  # noqa: ARG001
        step = args[0] if args else ""
        if step in ("dataset", "chart", "dashboard"):
            return _completed(0 if step in success_steps else 1)
        return _completed(0)

    return _side_effect


def test_promote_succeeds_when_all_pushes_succeed():
    with patch(
        "ol_superset.commands.promote.run_sup_command",
        side_effect=_run_sup_side_effect({"dataset", "chart", "dashboard"}),
    ):
        promote(force=True, skip_validation=True)


def test_promote_fails_build_when_dataset_push_fails():
    with (
        patch(
            "ol_superset.commands.promote.run_sup_command",
            side_effect=_run_sup_side_effect({"chart", "dashboard"}),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        promote(force=True, skip_validation=True)

    assert exc_info.value.code == 1


def test_promote_fails_build_when_chart_push_fails():
    with (
        patch(
            "ol_superset.commands.promote.run_sup_command",
            side_effect=_run_sup_side_effect({"dataset", "dashboard"}),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        promote(force=True, skip_validation=True)

    assert exc_info.value.code == 1


def test_promote_fails_build_when_dashboard_push_fails_and_forced():
    """--force must not silently swallow a dashboard push failure."""
    with (
        patch(
            "ol_superset.commands.promote.run_sup_command",
            side_effect=_run_sup_side_effect({"dataset", "chart"}),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        promote(force=True, skip_validation=True)

    assert exc_info.value.code == 1


def _confirm_action_side_effect(continue_after_failure: bool):
    def _side_effect(prompt, require_exact=None):  # noqa: ARG001
        if prompt == "Continue anyway?":
            return continue_after_failure
        # The earlier "FINAL CONFIRMATION: Deploy ... PRODUCTION?" prompt
        # (and any git-status prompt) must say yes so execution reaches the
        # push steps under test.
        return True

    return _side_effect


def test_promote_dashboard_failure_aborts_immediately_when_not_forced_and_declined():
    with (
        patch(
            "ol_superset.commands.promote.run_sup_command",
            side_effect=_run_sup_side_effect({"dataset", "chart"}),
        ),
        patch(
            "ol_superset.commands.promote.confirm_action",
            side_effect=_confirm_action_side_effect(continue_after_failure=False),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        promote(force=False, skip_validation=True)

    assert exc_info.value.code == 1


def test_promote_dashboard_failure_still_fails_build_if_user_continues_anyway():
    """Choosing to continue past a failure doesn't turn it into a success."""
    with (
        patch(
            "ol_superset.commands.promote.run_sup_command",
            side_effect=_run_sup_side_effect({"dataset", "chart"}),
        ),
        patch(
            "ol_superset.commands.promote.confirm_action",
            side_effect=_confirm_action_side_effect(continue_after_failure=True),
        ),
        pytest.raises(SystemExit) as exc_info,
    ):
        promote(force=False, skip_validation=True)

    assert exc_info.value.code == 1

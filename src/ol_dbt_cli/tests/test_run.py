"""Tests for commands/run.py — incremental state-based dbt execution."""

from __future__ import annotations

from pathlib import Path

import pytest

from ol_dbt_cli.commands.run import _build_dbt_command

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROFILES = Path("/dbt/project")
STATE = Path("/dbt/project/.dbt-state")

NO_STATE: dict[str, bool] = {"manifest.json": False, "run_results.json": False}
MANIFEST_ONLY: dict[str, bool] = {"manifest.json": True, "run_results.json": False}
RESULTS_ONLY: dict[str, bool] = {"manifest.json": False, "run_results.json": True}
FULL_STATE: dict[str, bool] = {"manifest.json": True, "run_results.json": True}


def build(
    subcommand: str = "build",
    available: dict[str, bool] = NO_STATE,
    select: str | None = None,
    full_refresh: bool = False,
    defer: bool = True,
    target: str | None = None,
    vars: str | None = None,
    extra_args: list[str] | None = None,
) -> list[str]:
    """Wrap _build_dbt_command with sensible test defaults."""
    return _build_dbt_command(
        subcommand=subcommand,
        profiles_dir=PROFILES,
        state_dir=STATE,
        available=available,
        select=select,
        full_refresh=full_refresh,
        defer=defer,
        target=target,
        vars=vars,
        extra_args=extra_args or [],
    )


# ---------------------------------------------------------------------------
# Base command structure
# ---------------------------------------------------------------------------


def test_always_includes_profiles_dir() -> None:
    cmd = build()
    assert "--profiles-dir" in cmd
    assert str(PROFILES) in cmd


def test_profiles_dir_position() -> None:
    """--profiles-dir should appear right after the subcommand."""
    cmd = build(subcommand="build")
    assert cmd[0] == "dbt"
    assert cmd[1] == "build"
    assert cmd[2] == "--profiles-dir"
    assert cmd[3] == str(PROFILES)


# ---------------------------------------------------------------------------
# First run (no state)
# ---------------------------------------------------------------------------


def test_first_run_no_select_no_state_flags() -> None:
    """With no state, no --select, --state, or --defer should be added."""
    cmd = build(available=NO_STATE)
    assert "--select" not in cmd
    assert "--state" not in cmd
    assert "--defer" not in cmd


def test_first_run_full_refresh_no_state_flags() -> None:
    cmd = build(available=NO_STATE, full_refresh=True)
    assert "--state" not in cmd
    assert "--defer" not in cmd
    assert "--full-refresh" in cmd


# ---------------------------------------------------------------------------
# Incremental selection (state present)
# ---------------------------------------------------------------------------


def test_manifest_only_selects_state_modified() -> None:
    cmd = build(available=MANIFEST_ONLY)
    assert "--select" in cmd
    sel_idx = cmd.index("--select")
    assert "state:modified+" in cmd[sel_idx + 1]
    assert "result:error+" not in cmd[sel_idx + 1]


def test_results_only_selects_result_error() -> None:
    cmd = build(available=RESULTS_ONLY)
    assert "--select" in cmd
    sel_idx = cmd.index("--select")
    assert "result:error+" in cmd[sel_idx + 1]
    assert "result:fail+" in cmd[sel_idx + 1]
    assert "state:modified+" not in cmd[sel_idx + 1]


def test_full_state_selects_both() -> None:
    cmd = build(available=FULL_STATE)
    sel_idx = cmd.index("--select")
    selection = cmd[sel_idx + 1]
    assert "state:modified+" in selection
    assert "result:error+" in selection
    assert "result:fail+" in selection


def test_full_state_passes_state_flag() -> None:
    cmd = build(available=FULL_STATE)
    assert "--state" in cmd
    state_idx = cmd.index("--state")
    assert cmd[state_idx + 1] == str(STATE)


def test_full_state_with_defer_passes_defer_flag() -> None:
    cmd = build(available=FULL_STATE, defer=True)
    assert "--defer" in cmd


def test_full_state_without_defer_no_defer_flag() -> None:
    cmd = build(available=FULL_STATE, defer=False)
    assert "--defer" not in cmd
    # --state must still be present for the selector to work
    assert "--state" in cmd


# ---------------------------------------------------------------------------
# --defer requires manifest.json
# ---------------------------------------------------------------------------


def test_results_only_no_defer_even_when_requested() -> None:
    """--defer must not be passed when manifest.json is absent (results only)."""
    cmd = build(available=RESULTS_ONLY, defer=True)
    assert "--defer" not in cmd
    # --state should still be present because result:error+ needs it
    assert "--state" in cmd


def test_manifest_only_defer_included() -> None:
    cmd = build(available=MANIFEST_ONLY, defer=True)
    assert "--defer" in cmd
    assert "--state" in cmd


# ---------------------------------------------------------------------------
# --full-refresh
# ---------------------------------------------------------------------------


def test_full_refresh_ignores_state() -> None:
    """With --full-refresh, no state selectors or --state/--defer should appear."""
    cmd = build(available=FULL_STATE, full_refresh=True)
    assert "--state" not in cmd
    assert "--defer" not in cmd
    assert "--select" not in cmd
    assert "--full-refresh" in cmd


def test_full_refresh_with_explicit_select_passes_select() -> None:
    cmd = build(available=FULL_STATE, select="my_model", full_refresh=True)
    assert "--select" in cmd
    assert cmd[cmd.index("--select") + 1] == "my_model"
    assert "--full-refresh" in cmd


# ---------------------------------------------------------------------------
# Explicit --select override
# ---------------------------------------------------------------------------


def test_explicit_select_overrides_state_selection() -> None:
    cmd = build(available=FULL_STATE, select="stg_users+")
    sel_idx = cmd.index("--select")
    assert cmd[sel_idx + 1] == "stg_users+"
    # State incremental expression should NOT be present
    assert "state:modified+" not in " ".join(cmd)


def test_explicit_select_still_passes_state_for_defer() -> None:
    """When --select is explicit and state exists, --state and --defer still apply."""
    cmd = build(available=FULL_STATE, select="stg_users+", defer=True)
    assert "--state" in cmd
    assert "--defer" in cmd


def test_explicit_select_no_state_no_defer_flag() -> None:
    cmd = build(available=NO_STATE, select="stg_users+", defer=True)
    assert "--state" not in cmd
    assert "--defer" not in cmd


# ---------------------------------------------------------------------------
# Target and vars pass-through
# ---------------------------------------------------------------------------


def test_target_appended() -> None:
    cmd = build(target="dev_qa")
    assert "--target" in cmd
    assert cmd[cmd.index("--target") + 1] == "dev_qa"


def test_no_target_by_default() -> None:
    cmd = build()
    assert "--target" not in cmd


def test_vars_appended() -> None:
    cmd = build(vars='{"schema_suffix": "alice"}')
    assert "--vars" in cmd
    assert cmd[cmd.index("--vars") + 1] == '{"schema_suffix": "alice"}'


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("subcommand", ["build", "run", "test"])
def test_subcommand_is_second_token(subcommand: str) -> None:
    cmd = build(subcommand=subcommand)
    assert cmd[1] == subcommand


# ---------------------------------------------------------------------------
# Extra args pass-through
# ---------------------------------------------------------------------------


def test_extra_args_appended_at_end() -> None:
    cmd = build(extra_args=["--threads", "8"])
    assert cmd[-2:] == ["--threads", "8"]

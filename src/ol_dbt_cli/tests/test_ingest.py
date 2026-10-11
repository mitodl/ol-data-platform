"""Tests for `ol-dbt ingest local`."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

from ol_dbt_cli.commands import ingest
from ol_dbt_cli.lib.inventory import load_units, local_ingest_sources

REPO_ROOT = Path(__file__).resolve().parents[3]


def _unit(deployment: str, local: str) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "deployment": deployment,
        "layer": "app_postgres",
        "scope": "scoped",
        "strategies": {"qa": "ingest", "local": local},
        "loader": "dlt",
        "table_prefix": f"raw__{deployment}__app__postgres__",
        "dlt": {"source_module": f"ol_dlt.sources.{deployment}", "write_disposition": "replace"},
        "tables": [],
    }


@pytest.fixture
def inventory_dir(tmp_path: Path) -> Path:
    units = tmp_path / "ingestion" / "inventory" / "units"
    units.mkdir(parents=True)
    for unit in (_unit("keycloak", "ingest"), _unit("mitxonline", "ingest"), _unit("xpro", "fixture")):
        (units / f"{unit['deployment']}__app_postgres.yml").write_text(yaml.safe_dump(unit))
    return units.parent


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[dict[str, Any]]:
    """Record each source run. The one for Keycloak fails."""
    recorded: list[dict[str, Any]] = []

    def run(command: list[str], *, cwd: Path, env: dict[str, str], check: bool) -> subprocess.CompletedProcess[str]:
        recorded.append({"command": command, "cwd": cwd, "env": env, "check": check})
        return subprocess.CompletedProcess(command, returncode=int(command[-1].endswith("keycloak")))

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(ingest, "get_repo_root", lambda _: tmp_path)
    return recorded


def test_runs_every_ingest_unit_and_fails_when_one_does(inventory_dir: Path, runs: list[dict[str, Any]]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        ingest.local(inventory_dir=inventory_dir)

    assert exit_info.value.code == 1
    assert [run["command"][-1] for run in runs] == ["ol_dlt.sources.keycloak", "ol_dlt.sources.mitxonline"]
    assert {run["cwd"] for run in runs} == {inventory_dir.parents[1] / "src" / "ol_dlt"}


def test_the_callers_dlt_profile_is_not_used(
    inventory_dir: Path, runs: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DLT_PROFILE", "production")
    monkeypatch.setenv("VIRTUAL_ENV", "/somewhere/else")

    ingest.local(inventory_dir=inventory_dir, unit=["mitxonline/app_postgres"])

    (run,) = runs
    assert run["env"]["DLT_PROFILE"] == "dev"
    assert "VIRTUAL_ENV" not in run["env"]


@pytest.mark.parametrize("unit", ["xpro/app_postgres", "nothing/here"])
def test_a_unit_that_is_not_ingested_locally_is_refused(
    inventory_dir: Path, runs: list[dict[str, Any]], unit: str
) -> None:
    with pytest.raises(SystemExit):
        ingest.local(inventory_dir=inventory_dir, unit=["mitxonline/app_postgres", unit])

    assert runs == []


def test_every_unit_the_inventory_marks_ingest_names_a_runnable_source() -> None:
    sources = local_ingest_sources(load_units(REPO_ROOT / "ingestion" / "inventory"))

    assert sources.keys() == {"keycloak/app_postgres", "mitxonline/app_postgres"}
    for module in sources.values():
        assert (REPO_ROOT / "src" / "ol_dlt" / Path(*module.split(".")) / "__main__.py").is_file(), module

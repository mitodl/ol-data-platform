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
REAL_INVENTORY = REPO_ROOT / "ingestion" / "inventory"


def _unit(deployment: str, local: str) -> dict[str, Any]:
    prefix = f"raw__{deployment}__app__postgres__"
    return {
        "schema_version": 1,
        "deployment": deployment,
        "layer": "app_postgres",
        "scope": "scoped",
        "strategies": {"qa": "ingest", "local": local},
        "loader": "dlt",
        "table_prefix": prefix,
        "dlt": {"source_module": f"ol_dlt.sources.{deployment}", "write_disposition": "replace"},
        "tables": [{"name": f"{prefix}things", "raw_table": f"{prefix}things", "sync_mode": "full_refresh_overwrite"}],
    }


def _write(inventory_dir: Path, unit: dict[str, Any]) -> None:
    (inventory_dir / "units" / f"{unit['deployment']}__app_postgres.yml").write_text(yaml.safe_dump(unit))


@pytest.fixture
def inventory_dir(tmp_path: Path) -> Path:
    """Build a valid three-unit inventory on the real schema and vocabulary."""
    root = tmp_path / "ingestion" / "inventory"
    (root / "units").mkdir(parents=True)
    (root / "schema").mkdir()
    (root / "schema" / "unit.schema.json").write_text((REAL_INVENTORY / "schema" / "unit.schema.json").read_text())
    (root / "vocabulary.yml").write_text((REAL_INVENTORY / "vocabulary.yml").read_text())
    for unit in (_unit("keycloak", "ingest"), _unit("mitxonline", "ingest"), _unit("xpro", "fixture")):
        _write(root, unit)
    return root


@pytest.fixture
def runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> list[dict[str, Any]]:
    """Record each source run. The one for Keycloak fails."""
    recorded: list[dict[str, Any]] = []

    def run(command: list[str], *, cwd: Path, env: dict[str, str], check: bool) -> subprocess.CompletedProcess[str]:
        recorded.append({"command": command, "cwd": cwd, "env": env, "check": check})
        return subprocess.CompletedProcess(command, returncode=int(command[-1].endswith("keycloak")))

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setattr(ingest, "get_repo_root", lambda _: tmp_path)
    for name in ingest._CATALOG_URL_VARIABLES:  # noqa: SLF001
        monkeypatch.delenv(name, raising=False)
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
    monkeypatch.setenv("PYICEBERG_HOME", "/somewhere/else")

    ingest.local(inventory_dir=inventory_dir, unit=["mitxonline/app_postgres"])

    (run,) = runs
    assert run["env"]["DLT_PROFILE"] == "dev"
    assert run["env"]["OL_DLT_LOCAL_SOURCES_ONLY"] == "1"
    assert not {"VIRTUAL_ENV", "PYICEBERG_HOME"} & run["env"].keys()


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__URI", "https://catalog.example.org/iceberg"),
        ("PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__S3__ENDPOINT", "http://10.0.0.5:9000"),
    ],
)
def test_a_catalog_override_that_leaves_this_machine_is_refused(
    inventory_dir: Path, runs: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(SystemExit):
        ingest.local(inventory_dir=inventory_dir, unit=["mitxonline/app_postgres"])

    assert runs == []


def test_an_override_for_a_forward_on_another_port_is_kept(
    inventory_dir: Path, runs: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    overrides = {
        "PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__URI": "http://localhost:19001/iceberg",
        "PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__S3__ENDPOINT": "http://localhost:19000",
    }
    for name, value in overrides.items():
        monkeypatch.setenv(name, value)

    ingest.local(inventory_dir=inventory_dir, unit=["mitxonline/app_postgres"])

    (run,) = runs
    assert overrides.items() <= run["env"].items()


@pytest.mark.parametrize("unit", ["xpro/app_postgres", "nothing/here"])
def test_a_unit_that_is_not_ingested_locally_is_refused(
    inventory_dir: Path, runs: list[dict[str, Any]], unit: str
) -> None:
    with pytest.raises(SystemExit):
        ingest.local(inventory_dir=inventory_dir, unit=["mitxonline/app_postgres", unit])

    assert runs == []


def test_an_inventory_that_does_not_validate_runs_nothing(inventory_dir: Path, runs: list[dict[str, Any]]) -> None:
    unit = _unit("mitxonline", "ingest")
    unit["loader"] = "dagster"
    del unit["dlt"]
    _write(inventory_dir, unit)

    with pytest.raises(SystemExit):
        ingest.local(inventory_dir=inventory_dir)

    assert runs == []


def test_a_source_that_cannot_start_does_not_stop_the_others(
    inventory_dir: Path, runs: list[dict[str, Any]], monkeypatch: pytest.MonkeyPatch
) -> None:
    started = []

    def run(command: list[str], **_: object) -> subprocess.CompletedProcess[str]:
        started.append(command[-1])
        if command[-1].endswith("keycloak"):
            raise FileNotFoundError("uv")
        return subprocess.CompletedProcess(command, returncode=0)

    monkeypatch.setattr(subprocess, "run", run)

    with pytest.raises(SystemExit):
        ingest.local(inventory_dir=inventory_dir)

    assert started == ["ol_dlt.sources.keycloak", "ol_dlt.sources.mitxonline"]


def test_every_unit_the_inventory_marks_ingest_names_a_runnable_source() -> None:
    sources = local_ingest_sources(load_units(REAL_INVENTORY))

    assert sources.keys() == {"keycloak/app_postgres", "mitxonline/app_postgres"}
    for module in sources.values():
        assert (REPO_ROOT / "src" / "ol_dlt" / Path(*module.split(".")) / "__main__.py").is_file(), module

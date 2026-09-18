"""Tests for the per-model QA branch contract (RFC 12711 steps 4 and 5)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from ol_dbt_cli.commands.validate import _check_qa_branch_contract, _update_qa_baseline
from ol_dbt_cli.lib.dimensional_layering import load_baseline
from ol_dbt_cli.lib.inventory import Unit
from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry, registry_from_manifest
from ol_dbt_cli.lib.qa_contract import check_qa_contracts, check_qa_gaps, qa_gaps, render_qa_baseline, upstream_units
from ol_dbt_cli.lib.qa_observation import (
    OBSERVATION_FILENAME,
    Observation,
    TableState,
    _current_snapshot,
    load_observation,
    observed_tables,
    render_observation,
)
from ol_dbt_cli.lib.validation import Severity, ValidationReport


def _unit(deployment: str, layer: str, scope: str, *tables: str, qa: str | None = None) -> Unit:
    data: dict[str, Any] = {
        "deployment": deployment,
        "layer": layer,
        "scope": scope,
        "strategies": {"qa": qa or ("ingest" if scope == "scoped" else "mirror")},
        "tables": [{"raw_table": t} for t in tables],
    }
    if data["strategies"]["qa"] == "mirror":
        data["mirror_max_age_days"] = 90
    return Unit(path=Path(f"{deployment}__{layer}.yml"), data=data)


UNITS = [
    _unit("mitxonline", "app_postgres", "scoped", "raw__mitxonline__app__postgres__users_user"),
    _unit("xpro", "app_postgres", "scoped", "raw__xpro__app__postgres__auth_user"),
    _unit("emeritus", "bigquery", "singleton", "raw__emeritus__bigquery__api_enrollments"),
    _unit("mitlearn", "app_postgres", "scoped", "raw__mitlearn__app__postgres__users_user"),
]


def _source(name: str, identifier: str = "") -> ManifestModel:
    return ManifestModel(
        unique_id=f"source.pkg.raw.{name}",
        name=name,
        resource_type="source",
        original_file_path="models/staging/_sources.yml",
        schema="",
        database="",
        identifier=identifier or name,
    )


def _model(name: str, path: str, depends_on: list[str], meta: dict[str, Any] | None = None) -> ManifestModel:
    return ManifestModel(
        unique_id=f"model.pkg.{name}",
        name=name,
        resource_type="model",
        original_file_path=path,
        schema="",
        database="",
        depends_on=depends_on,
        meta=meta or {},
    )


def _registry(*nodes: ManifestModel) -> ManifestRegistry:
    registry = ManifestRegistry()
    registry.nodes = {n.unique_id: n for n in nodes}
    return registry


def _dag(dim_user_meta: dict[str, Any] | None) -> ManifestRegistry:
    """dim_user unions two scoped app databases and a singleton, via staging."""
    return _registry(
        _source("raw__mitxonline__app__postgres__users_user"),
        _source("raw__xpro__app__postgres__auth_user"),
        _source("emeritus_enrollments", identifier="raw__emeritus__bigquery__api_enrollments"),
        _source("raw__bootcamps__app__postgres__auth_user"),
        _model("stg_mitxonline", "models/staging/a.sql", ["source.pkg.raw.raw__mitxonline__app__postgres__users_user"]),
        _model("stg_xpro", "models/staging/b.sql", ["source.pkg.raw.raw__xpro__app__postgres__auth_user"]),
        _model("stg_emeritus", "models/staging/c.sql", ["source.pkg.raw.emeritus_enrollments"]),
        _model("stg_bootcamps", "models/staging/d.sql", ["source.pkg.raw.raw__bootcamps__app__postgres__auth_user"]),
        _model(
            "dim_user",
            "models/dimensional/dim_user.sql",
            ["model.pkg.stg_mitxonline", "model.pkg.stg_xpro", "model.pkg.stg_emeritus", "model.pkg.stg_bootcamps"],
            dim_user_meta,
        ),
        _model("marts_user", "models/marts/marts_user.sql", ["model.pkg.dim_user"]),
    )


def _findings(registry: ManifestRegistry) -> list[tuple[str, str]]:
    report = ValidationReport()
    check_qa_contracts(registry, UNITS, report)
    assert all(issue.severity == Severity.ERROR for issue in report.issues)
    return [(issue.model, issue.message) for issue in report.issues]


class TestUpstreamUnits:
    def test_transitive_through_models_by_identifier(self) -> None:
        lineage = upstream_units(_dag(None), UNITS)
        assert lineage["marts_user"] == {"mitxonline/app_postgres", "xpro/app_postgres", "emeritus/bigquery"}

    def test_source_no_unit_declares_contributes_nothing(self) -> None:
        assert upstream_units(_dag(None), UNITS)["stg_bootcamps"] == set()


class TestCheckQaContracts:
    def test_undeclared_union_models_are_errors(self) -> None:
        findings = _findings(_dag(None))
        assert [model for model, _ in findings] == ["dim_user", "marts_user"]

    def test_suggestion_names_only_scoped_units(self) -> None:
        report = ValidationReport()
        check_qa_contracts(_dag(None), UNITS, report)
        assert "qa_branches: [mitxonline/app_postgres, xpro/app_postgres]" in report.issues[0].detail

    def test_staging_is_never_a_union(self) -> None:
        registry = _registry(
            _source("raw__mitxonline__app__postgres__users_user"),
            _source("raw__xpro__app__postgres__auth_user"),
            _model(
                "stg_both",
                "models/staging/both.sql",
                [
                    "source.pkg.raw.raw__mitxonline__app__postgres__users_user",
                    "source.pkg.raw.raw__xpro__app__postgres__auth_user",
                ],
            ),
        )
        assert _findings(registry) == []

    def test_declared_subset_passes(self) -> None:
        registry = _dag({"qa_branches": ["mitxonline/app_postgres"]})
        assert [model for model, _ in _findings(registry)] == ["marts_user"]

    def test_not_buildable_passes(self) -> None:
        registry = _dag({"qa_buildable": False})
        assert [model for model, _ in _findings(registry)] == ["marts_user"]

    def test_both_declarations_contradict(self) -> None:
        registry = _dag({"qa_buildable": False, "qa_branches": ["mitxonline/app_postgres"]})
        assert ("dim_user", "declares both qa_buildable: false and qa_branches") in _findings(registry)

    def test_empty_branches_beside_not_buildable_is_reported_once(self) -> None:
        registry = _dag({"qa_buildable": False, "qa_branches": []})
        assert [found for model, found in _findings(registry) if model == "dim_user"] == [
            "qa_branches is empty; a model with no QA branches declares `qa_buildable: false`"
        ]

    def test_branch_not_upstream(self) -> None:
        registry = _dag({"qa_branches": ["mitxonline/app_postgres", "mitlearn/app_postgres"]})
        assert (
            "dim_user",
            "qa_branches names mitlearn/app_postgres, which is not upstream of this model",
        ) in _findings(registry)

    def test_branch_not_in_inventory(self) -> None:
        registry = _dag({"qa_branches": ["mitxonline/app_postgres", "bootcamps/app_postgres"]})
        assert (
            "dim_user",
            "qa_branches names bootcamps/app_postgres, which is not an inventory unit",
        ) in _findings(registry)

    def test_branch_whose_unit_qa_omits_is_an_error(self) -> None:
        units = [
            _unit("mitxonline", "app_postgres", "scoped", "raw__mitxonline__app__postgres__users_user"),
            _unit("xpro", "app_postgres", "scoped", "raw__xpro__app__postgres__auth_user", qa="omit"),
        ]
        report = ValidationReport()
        check_qa_contracts(_dag({"qa_branches": ["mitxonline/app_postgres", "xpro/app_postgres"]}), units, report)
        dim_user = [i for i in report.issues if i.model == "dim_user"]
        assert [(i.severity, i.message) for i in dim_user] == [
            (Severity.ERROR, "qa_branches names xpro/app_postgres, whose strategies.qa is omit")
        ]

    def test_declared_mirror_branch_passes(self) -> None:
        registry = _dag({"qa_branches": ["mitxonline/app_postgres", "emeritus/bigquery"]})
        assert [model for model, _ in _findings(registry)] == ["marts_user"]

    def test_declaration_on_a_non_union_model_is_still_checked(self) -> None:
        registry = _dag(None)
        registry.nodes["model.pkg.stg_xpro"].meta = {"qa_branches": ["mitxonline/app_postgres"]}
        assert (
            "stg_xpro",
            "qa_branches names mitxonline/app_postgres, which is not upstream of this model",
        ) in _findings(registry)

    def test_explicit_null_is_not_an_absent_key(self) -> None:
        # dbt keeps `qa_branches:` with nothing under it in config.meta as None,
        # so on a model that unions nothing there is no missing-declaration
        # finding to catch it.
        registry = _dag(None)
        registry.nodes["model.pkg.stg_xpro"].meta = {"qa_branches": None, "qa_buildable": None}
        findings = _findings(registry)
        assert ("stg_xpro", "qa_branches must be a list of `deployment/layer` strings") in findings
        assert (
            "stg_xpro",
            "qa_buildable only takes `false`; a buildable model says so by declaring qa_branches",
        ) in findings

    @pytest.mark.parametrize(
        ("meta", "message"),
        [
            ({"qa_branches": "mitxonline/app_postgres"}, "qa_branches must be a list of `deployment/layer` strings"),
            ({"qa_branches": None}, "qa_branches must be a list of `deployment/layer` strings"),
            (
                {"qa_buildable": None},
                "qa_buildable only takes `false`; a buildable model says so by declaring qa_branches",
            ),
            (
                {"qa_branches": []},
                "qa_branches is empty; a model with no QA branches declares `qa_buildable: false`",
            ),
            (
                {"qa_branches": ["mitxonline"]},
                "qa_branches entry 'mitxonline' is not a `deployment/layer` unit key",
            ),
            (
                {"qa_branches": ["xpro/app_postgres", "xpro/app_postgres"]},
                "qa_branches lists a branch more than once",
            ),
            (
                {"qa_buildable": True},
                "qa_buildable only takes `false`; a buildable model says so by declaring qa_branches",
            ),
        ],
    )
    def test_malformed(self, meta: dict[str, Any], message: str) -> None:
        dim_user = [found for model, found in _findings(_dag(meta)) if model == "dim_user"]
        assert message in dim_user
        # A malformed declaration is still a declaration; reporting it as missing
        # too would send the author looking for a key they already wrote.
        assert not any("declares no QA contract" in found for found in dim_user)


def test_manifest_reads_identifier_and_config_meta() -> None:
    registry = registry_from_manifest(
        {
            "nodes": {
                "model.pkg.dim_user": {
                    "unique_id": "model.pkg.dim_user",
                    "name": "dim_user",
                    "resource_type": "model",
                    "config": {"meta": {"qa_buildable": False}},
                }
            },
            "sources": {
                "source.pkg.raw.users": {
                    "unique_id": "source.pkg.raw.users",
                    "name": "users",
                    "source_name": "raw",
                    "resource_type": "source",
                    "identifier": "raw__mitxonline__app__postgres__users_user",
                }
            },
        }
    )
    assert registry.nodes["model.pkg.dim_user"].meta == {"qa_buildable": False}
    assert registry.nodes["source.pkg.raw.users"].identifier == "raw__mitxonline__app__postgres__users_user"


class TestValidateWiring:
    def test_missing_manifest_warns(self, tmp_path: Path) -> None:
        report = ValidationReport()
        _check_qa_branch_contract(None, tmp_path, report)
        assert [(i.severity, i.message) for i in report.issues] == [
            (Severity.WARNING, "Skipped: QA branch contracts need manifest lineage")
        ]

    def test_missing_inventory_warns_instead_of_passing(self, tmp_path: Path) -> None:
        report = ValidationReport()
        _check_qa_branch_contract(_dag(None), tmp_path / "ingestion" / "inventory", report)
        assert len(report.issues) == 1
        assert report.issues[0].severity == Severity.WARNING
        assert report.issues[0].message.startswith("Skipped: no ingestion inventory units found under")


OBSERVED_AT = datetime(2026, 9, 18, tzinfo=UTC)
HAS_ROWS = TableState(present=True, iceberg=True, rows=10, snapshot_at=OBSERVED_AT - timedelta(days=1))
DECLARED = {"qa_branches": ["mitxonline/app_postgres", "xpro/app_postgres", "emeritus/bigquery"]}


def _observation(**tables: TableState) -> Observation:
    base = {
        "raw__mitxonline__app__postgres__users_user": HAS_ROWS,
        "raw__xpro__app__postgres__auth_user": HAS_ROWS,
        "raw__emeritus__bigquery__api_enrollments": HAS_ROWS,
    }
    return Observation(glue_database="ol_warehouse_qa_raw", observed_at=OBSERVED_AT, tables=base | tables)


def _gap_report(
    observation: Observation, baseline: set[str] | None = None, now: datetime = OBSERVED_AT
) -> list[tuple[Severity, str, str]]:
    report = ValidationReport()
    check_qa_gaps(_dag(DECLARED), UNITS, observation, baseline or set(), now, report)
    return [(i.severity, i.model, i.message) for i in report.issues]


def _inventory(tmp_path: Path, qa: str) -> Path:
    inventory = tmp_path / "inventory"
    (inventory / "units").mkdir(parents=True)
    (inventory / "units" / "xpro__app_postgres.yml").write_text(
        f"deployment: xpro\nlayer: app_postgres\nscope: scoped\nstrategies: {{qa: {qa}}}\n"
        "tables: [{raw_table: raw__xpro__app__postgres__auth_user}]\n"
    )
    return inventory


XPRO_ABSENT = {"raw__xpro__app__postgres__auth_user": TableState(present=False)}
XPRO_GAP = "xpro/app_postgres raw__xpro__app__postgres__auth_user: empty"


class TestQaGaps:
    @pytest.mark.parametrize(
        ("state", "reason"),
        [
            (TableState(present=False), "absent from QA raw"),
            (TableState(present=True), "not Iceberg (legacy JSON destination)"),
            (TableState(present=True, iceberg=True), "no current snapshot"),
            (TableState(present=True, iceberg=True, rows=0, snapshot_at=OBSERVED_AT), "no rows"),
        ],
    )
    def test_empty_conditions(self, state: TableState, reason: str) -> None:
        gaps = qa_gaps(_dag(DECLARED), UNITS, _observation(raw__xpro__app__postgres__auth_user=state))
        assert [(g.key, g.reason, g.models) for g in gaps] == [(XPRO_GAP, reason, ("dim_user",))]

    def test_stale_mirror(self) -> None:
        old = TableState(present=True, iceberg=True, rows=5, snapshot_at=OBSERVED_AT - timedelta(days=91))
        gaps = qa_gaps(_dag(DECLARED), UNITS, _observation(raw__emeritus__bigquery__api_enrollments=old))
        assert [g.key for g in gaps] == ["emeritus/bigquery raw__emeritus__bigquery__api_enrollments: stale"]

    def test_old_ingested_table_is_not_stale(self) -> None:
        # mirror_max_age_days bounds a copy of production; an ingested branch has no such bound.
        old = TableState(present=True, iceberg=True, rows=5, snapshot_at=OBSERVED_AT - timedelta(days=400))
        gaps = qa_gaps(_dag(DECLARED), UNITS, _observation(raw__xpro__app__postgres__auth_user=old))
        assert gaps == []

    def test_undeclared_branch_is_not_checked(self) -> None:
        registry = _dag({"qa_branches": ["mitxonline/app_postgres"]})
        assert qa_gaps(registry, UNITS, _observation(**XPRO_ABSENT)) == []

    def test_omitted_branch_is_left_to_the_inventory_half(self) -> None:
        units = [UNITS[0], _unit("xpro", "app_postgres", "scoped", "raw__xpro__app__postgres__auth_user", qa="omit")]
        assert qa_gaps(_dag(DECLARED), units, _observation(**XPRO_ABSENT)) == []

    def test_unobserved_table_is_a_gap(self) -> None:
        # A branch declared or ingested after the observation was taken must not pass on a warning.
        observation = _observation()
        del observation.tables["raw__xpro__app__postgres__auth_user"]
        assert _gap_report(observation) == [
            (Severity.ERROR, "xpro/app_postgres", "1 table(s) that declaring models read are unobserved in QA")
        ]
        baseline = {"xpro/app_postgres raw__xpro__app__postgres__auth_user: unobserved"}
        assert [severity for severity, _, _ in _gap_report(observation, baseline)] == [Severity.INFO]

    def test_observation_of_another_database_errors(self) -> None:
        observation = Observation(
            glue_database="ol_warehouse_production_raw", observed_at=OBSERVED_AT, tables=_observation().tables
        )
        expected = [
            (
                Severity.ERROR,
                "(qa observation)",
                "The QA observation was taken from ol_warehouse_production_raw, not ol_warehouse_qa_raw",
            )
        ]
        assert _gap_report(observation) == expected
        # Stops there: production holding the table must not read as a resolved QA gap.
        assert _gap_report(observation, {XPRO_GAP}) == expected

    def test_new_gap_errors_per_branch(self) -> None:
        assert _gap_report(_observation(**XPRO_ABSENT)) == [
            (Severity.ERROR, "xpro/app_postgres", "1 table(s) that declaring models read are empty in QA")
        ]

    def test_baselined_gap_is_info(self) -> None:
        assert _gap_report(_observation(**XPRO_ABSENT), {XPRO_GAP}) == [
            (Severity.INFO, "(qa baseline)", "1 known QA gap(s) tolerated by baseline")
        ]

    def test_resolved_baseline_entry_is_info(self) -> None:
        assert _gap_report(_observation(), {XPRO_GAP}) == [
            (Severity.INFO, "(qa baseline)", f"Resolved baseline entry: {XPRO_GAP}")
        ]

    def test_old_observation_warns(self) -> None:
        assert _gap_report(_observation(), now=OBSERVED_AT + timedelta(days=31)) == [
            (Severity.WARNING, "(qa observation)", "The QA observation is 31 days old")
        ]

    def test_baseline_round_trip(self, tmp_path: Path) -> None:
        gaps = qa_gaps(_dag(DECLARED), UNITS, _observation(**XPRO_ABSENT))
        path = tmp_path / "qa_branch_baseline.txt"
        path.write_text(render_qa_baseline(gaps))
        assert load_baseline(path) == {XPRO_GAP}


class TestObservation:
    def test_round_trip(self, tmp_path: Path) -> None:
        observation = _observation(**XPRO_ABSENT)
        path = tmp_path / "qa_observation.json"
        path.write_text(render_observation(observation))
        assert load_observation(path) == observation

    def test_observed_tables_skip_omitted_units(self) -> None:
        units = [*UNITS, _unit("mailgun", "api", "scoped", "raw__mailgun__api__events", qa="omit")]
        tables = observed_tables(units)
        assert "raw__mailgun__api__events" not in tables
        assert "raw__emeritus__bigquery__api_enrollments" in tables

    def test_current_snapshot_reads_the_current_one(self) -> None:
        metadata = {
            "current-snapshot-id": 2,
            "snapshots": [
                {"snapshot-id": 1, "timestamp-ms": 0, "summary": {"total-records": "5"}},
                {"snapshot-id": 2, "timestamp-ms": 1_758_153_600_000, "summary": {"total-records": "0"}},
            ],
        }
        assert _current_snapshot(metadata) == (0, datetime(2025, 9, 18, tzinfo=UTC))

    def test_no_current_snapshot(self) -> None:
        assert _current_snapshot({"current-snapshot-id": -1, "snapshots": []}) == (None, None)

    def test_missing_total_records_fails_instead_of_reading_as_empty(self) -> None:
        metadata = {
            "location": "s3://lake/raw/t",
            "current-snapshot-id": 1,
            "snapshots": [{"snapshot-id": 1, "timestamp-ms": 0, "summary": {"operation": "append"}}],
        }
        with pytest.raises(ValueError, match="has no total-records statistic"):
            _current_snapshot(metadata)


class TestValidateGapWiring:
    def test_missing_observation_skips_only_the_gap_half(self, tmp_path: Path) -> None:
        report = ValidationReport()
        _check_qa_branch_contract(_dag({"qa_branches": ["xpro/app_postgres"]}), _inventory(tmp_path, "omit"), report)
        messages = [i.message for i in report.issues]
        assert any(m.startswith("Skipped the QA gap half") for m in messages)
        assert "qa_branches names xpro/app_postgres, whose strategies.qa is omit" in messages

    def test_observation_and_baseline_are_read_from_the_inventory_dir(self, tmp_path: Path) -> None:
        inventory = _inventory(tmp_path, "ingest")
        (inventory / OBSERVATION_FILENAME).write_text(render_observation(_observation(**XPRO_ABSENT)))
        (inventory / "qa_branch_baseline.txt").write_text(f"{XPRO_GAP}\n")
        report = ValidationReport()
        _check_qa_branch_contract(_dag({"qa_branches": ["xpro/app_postgres"]}), inventory, report, now=OBSERVED_AT)
        assert not report.errors
        assert [i.message for i in report.issues if i.model == "(qa baseline)"] == [
            "1 known QA gap(s) tolerated by baseline"
        ]

    def test_update_qa_baseline_rejects_a_non_qa_observation(self, tmp_path: Path) -> None:
        inventory = _inventory(tmp_path, "ingest")
        observation = Observation(
            glue_database="ol_warehouse_production_raw", observed_at=OBSERVED_AT, tables=_observation().tables
        )
        (inventory / OBSERVATION_FILENAME).write_text(render_observation(observation))
        baseline = inventory / "qa_branch_baseline.txt"
        baseline.write_text(f"{XPRO_GAP}\n")
        with pytest.raises(SystemExit):
            _update_qa_baseline(_dag({"qa_branches": ["xpro/app_postgres"]}), inventory)
        assert baseline.read_text() == f"{XPRO_GAP}\n"

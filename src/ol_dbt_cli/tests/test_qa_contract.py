"""Tests for the per-model QA branch contract (RFC 12711 step 4)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ol_dbt_cli.commands.validate import _check_qa_branch_contract
from ol_dbt_cli.lib.inventory import Unit
from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry, registry_from_manifest
from ol_dbt_cli.lib.qa_contract import check_qa_contracts, upstream_units
from ol_dbt_cli.lib.validation import Severity, ValidationReport


def _unit(deployment: str, layer: str, scope: str, *tables: str) -> Unit:
    return Unit(
        path=Path(f"{deployment}__{layer}.yml"),
        data={
            "deployment": deployment,
            "layer": layer,
            "scope": scope,
            "tables": [{"raw_table": t} for t in tables],
        },
    )


UNITS = [
    _unit("mitxonline", "app_postgres", "scoped", "raw__mitxonline__app__postgres__users_user"),
    _unit("xpro", "app_postgres", "scoped", "raw__xpro__app__postgres__auth_user"),
    _unit("emeritus", "bigquery", "singleton", "raw__emeritus__bigquery__api_enrollments"),
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

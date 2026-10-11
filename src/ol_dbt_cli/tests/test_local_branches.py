"""Tests for the branch report a build on the local lake prints."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ol_dbt_cli.lib.inventory import Unit
from ol_dbt_cli.lib.local_branches import local_branches, raw_tables_read, render_local_branches
from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry

MITXONLINE_USERS = "raw__mitxonline__app__postgres__users_user"
MITXONLINE_PROFILES = "raw__mitxonline__app__postgres__users_userprofile"
XPRO_USERS = "raw__xpro__app__postgres__auth_user"
EDXORG_USERS = "raw__edxorg__s3__auth_user"


def _unit(deployment: str, layer: str, *tables: str, local: str = "fixture") -> Unit:
    data: dict[str, Any] = {
        "deployment": deployment,
        "layer": layer,
        "strategies": {"qa": "ingest", "local": local},
        "tables": [{"raw_table": table} for table in tables],
    }
    return Unit(path=Path(f"{deployment}__{layer}.yml"), data=data)


UNITS = [
    _unit("mitxonline", "app_postgres", MITXONLINE_USERS, MITXONLINE_PROFILES),
    _unit("xpro", "app_postgres", XPRO_USERS),
    _unit("edxorg", "s3", EDXORG_USERS, local="omit"),
]
FIXTURE_UNITS = {"mitxonline/app_postgres"}


def _node(name: str, path: str, depends_on: list[str], resource_type: str = "model") -> ManifestModel:
    return ManifestModel(
        unique_id=f"{resource_type}.pkg.{name}",
        name=name,
        resource_type=resource_type,
        original_file_path=path,
        schema="",
        database="",
        depends_on=depends_on,
        identifier=name if resource_type == "source" else "",
    )


def _manifest() -> ManifestRegistry:
    """dim_user unions three units through staging; int_mitxonline reads one."""
    nodes = [
        *(
            _node(table, "models/staging/_sources.yml", [], "source")
            for table in (MITXONLINE_USERS, MITXONLINE_PROFILES, XPRO_USERS, EDXORG_USERS)
        ),
        _node(
            "stg_mitxonline",
            "models/staging/a.sql",
            [f"source.pkg.{MITXONLINE_USERS}", f"source.pkg.{MITXONLINE_PROFILES}"],
        ),
        _node("stg_xpro", "models/staging/b.sql", [f"source.pkg.{XPRO_USERS}"]),
        _node("stg_edxorg", "models/staging/c.sql", [f"source.pkg.{EDXORG_USERS}"]),
        _node("stg_everything", "models/staging/d.sql", [f"source.pkg.{MITXONLINE_USERS}", f"source.pkg.{XPRO_USERS}"]),
        _node("int_mitxonline", "models/intermediate/a.sql", ["model.pkg.stg_mitxonline"]),
        _node(
            "dim_user",
            "models/dimensional/dim_user.sql",
            ["model.pkg.int_mitxonline", "model.pkg.stg_xpro", "model.pkg.stg_edxorg"],
        ),
    ]
    registry = ManifestRegistry()
    registry.nodes = {node.unique_id: node for node in nodes}
    return registry


def _branches(rows: dict[str, int], models: list[str] | None = None) -> dict[str, dict[str, str]]:
    report = local_branches(_manifest(), UNITS, models or ["dim_user"], rows, FIXTURE_UNITS)
    return {model: {branch.unit: branch.state for branch in branches} for model, branches in report.items()}


def test_every_branch_present() -> None:
    rows = {MITXONLINE_USERS: 3, MITXONLINE_PROFILES: 3, XPRO_USERS: 2, EDXORG_USERS: 1}

    assert _branches(rows) == {
        "dim_user": {"edxorg/s3": "present", "mitxonline/app_postgres": "present", "xpro/app_postgres": "present"}
    }


def test_a_table_the_lake_lacks_and_a_table_with_no_rows_both_count_against_a_branch() -> None:
    """A dbt build on an empty raw table succeeds, so no rows is as partial as no table."""
    rows = {MITXONLINE_USERS: 3, MITXONLINE_PROFILES: 0, XPRO_USERS: 0}

    assert _branches(rows) == {
        "dim_user": {"edxorg/s3": "absent", "mitxonline/app_postgres": "partial", "xpro/app_postgres": "absent"}
    }


def test_only_unions_above_staging_are_reported() -> None:
    """One unit cannot be partial across branches, and staging is a single source by convention."""
    assert _branches({}, ["int_mitxonline", "stg_everything", "stg_xpro"]) == {}


def test_a_model_named_twice_is_reported_once() -> None:
    assert _branches({}, ["dim_user", "dim_user"]).keys() == {"dim_user"}


def test_remedy_says_what_would_fill_the_branch() -> None:
    rows = {MITXONLINE_USERS: 3, MITXONLINE_PROFILES: 0}
    (branches,) = local_branches(_manifest(), UNITS, ["dim_user"], rows, FIXTURE_UNITS).values()
    remedy = {branch.unit: branch.remedy for branch in branches}

    assert remedy == {
        "edxorg/s3": "strategies.local is omit",
        "mitxonline/app_postgres": "the tables hold no rows",
        "xpro/app_postgres": "no fixture committed for this unit",
    }


def test_remedy_for_a_committed_fixture_that_was_not_loaded() -> None:
    (branches,) = local_branches(_manifest(), UNITS, ["dim_user"], {}, FIXTURE_UNITS).values()
    remedy = {branch.unit: branch.remedy for branch in branches}

    assert remedy["mitxonline/app_postgres"] == "run `ol-dbt fixtures load`, or add the tables to the fixture"


def test_raw_tables_read_is_the_lineage_of_the_named_models() -> None:
    assert raw_tables_read(_manifest(), UNITS, ["int_mitxonline", "stg_xpro"]) == {
        MITXONLINE_USERS,
        MITXONLINE_PROFILES,
        XPRO_USERS,
    }


def test_render_lists_the_partial_unions_and_counts_the_rest() -> None:
    rows = {MITXONLINE_USERS: 3, MITXONLINE_PROFILES: 0, XPRO_USERS: 2}
    report = local_branches(_manifest(), UNITS, ["dim_user"], rows, FIXTURE_UNITS)

    assert render_local_branches(report, "ol_warehouse_local_raw") == [
        "Ingestion branches in ol_warehouse_local_raw: 0 of 1 union model(s) built with every branch present.",
        "  dim_user: 1 of 3 branches present",
        "    edxorg/s3: absent (1 table(s)); strategies.local is omit",
        "    mitxonline/app_postgres: partial (1 of 2 table(s)); the tables hold no rows",
        f"      {MITXONLINE_PROFILES} (no rows)",
        "    xpro/app_postgres: present",
    ]


def test_render_is_one_line_when_nothing_is_partial_and_nothing_with_no_union() -> None:
    rows = {MITXONLINE_USERS: 3, MITXONLINE_PROFILES: 3, XPRO_USERS: 2, EDXORG_USERS: 1}
    report = local_branches(_manifest(), UNITS, ["dim_user"], rows, FIXTURE_UNITS)

    assert render_local_branches(report, "ol_warehouse_local_raw") == [
        "Ingestion branches in ol_warehouse_local_raw: 1 of 1 union model(s) built with every branch present."
    ]
    assert render_local_branches({}, "ol_warehouse_local_raw") == []


def test_render_caps_the_tables_it_names() -> None:
    tables = [f"raw__mitxonline__app__postgres__t{n}" for n in range(8)]
    units = [_unit("mitxonline", "app_postgres", *tables), _unit("xpro", "app_postgres", XPRO_USERS)]
    registry = ManifestRegistry()
    nodes = [
        *(_node(table, "models/staging/_sources.yml", [], "source") for table in [*tables, XPRO_USERS]),
        _node("int_all", "models/intermediate/a.sql", [f"source.pkg.{table}" for table in [*tables, XPRO_USERS]]),
    ]
    registry.nodes = {node.unique_id: node for node in nodes}
    report = local_branches(registry, units, ["int_all"], {tables[0]: 1, XPRO_USERS: 1}, set())

    lines = render_local_branches(report, "ol_warehouse_local_raw")

    assert lines[2] == "    mitxonline/app_postgres: partial (7 of 8 table(s)); no fixture committed for this unit"
    assert lines[3].count("(absent)") == 5
    assert lines[3].endswith(", and 2 more")

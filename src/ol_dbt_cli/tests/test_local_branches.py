"""Tests for the branch report a build on the local lake prints."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ol_dbt_cli.lib.inventory import Unit
from ol_dbt_cli.lib.local_branches import (
    Branch,
    local_branches,
    parents_not_built,
    render_local_branches,
    union_tables,
)
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


def _report(rows: dict[str, int], models: list[str] | None = None) -> dict[str, list[Branch]]:
    unions = union_tables(_manifest(), UNITS, models or ["dim_user"])
    return local_branches(unions, UNITS, rows, FIXTURE_UNITS)


def _branches(rows: dict[str, int], models: list[str] | None = None) -> dict[str, dict[str, str]]:
    report = _report(rows, models)
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
    (branches,) = _report(rows).values()
    remedy = {branch.unit: branch.remedy for branch in branches}

    assert remedy == {
        "edxorg/s3": "strategies.local is omit",
        "mitxonline/app_postgres": "the tables hold no rows",
        "xpro/app_postgres": "no fixture committed for this unit",
    }


def test_remedy_for_a_committed_fixture_that_was_not_loaded() -> None:
    (branches,) = _report({}).values()
    remedy = {branch.unit: branch.remedy for branch in branches}

    assert remedy["mitxonline/app_postgres"] == "run `ol-dbt fixtures load`, or add the tables to the fixture"


def test_union_tables_groups_the_raw_tables_a_union_reads_by_unit() -> None:
    assert union_tables(_manifest(), UNITS, ["dim_user", "int_mitxonline"]) == {
        "dim_user": {
            "edxorg/s3": [EDXORG_USERS],
            "mitxonline/app_postgres": [MITXONLINE_USERS, MITXONLINE_PROFILES],
            "xpro/app_postgres": [XPRO_USERS],
        }
    }


def test_parents_not_built_counts_the_upstream_models_the_run_left_alone() -> None:
    """A union rebuilt alone reads parents built from an earlier raw schema."""
    manifest = _manifest()

    assert parents_not_built(manifest, ["dim_user"], {"dim_user"}) == {"dim_user": 4}
    assert parents_not_built(manifest, ["dim_user"], {"dim_user", "int_mitxonline", "stg_xpro"}) == {"dim_user": 2}
    everything = {"dim_user", "int_mitxonline", "stg_mitxonline", "stg_xpro", "stg_edxorg"}
    assert parents_not_built(manifest, ["dim_user"], everything) == {}


def test_render_lists_the_partial_unions_and_counts_the_rest() -> None:
    rows = {MITXONLINE_USERS: 3, MITXONLINE_PROFILES: 0, XPRO_USERS: 2}
    report = _report(rows)

    assert render_local_branches(report, "ol_warehouse_local_raw", {}) == [
        "Ingestion branches in ol_warehouse_local_raw: 0 of 1 union model(s) built have rows in every branch.",
        "  dim_user: 1 of 3 branches present",
        "    edxorg/s3: absent (1 table(s)); strategies.local is omit",
        "    mitxonline/app_postgres: partial (1 of 2 table(s)); the tables hold no rows",
        f"      {MITXONLINE_PROFILES} (no rows)",
        "    xpro/app_postgres: present",
    ]


def test_render_is_one_line_when_nothing_is_partial_and_nothing_with_no_union() -> None:
    rows = {MITXONLINE_USERS: 3, MITXONLINE_PROFILES: 3, XPRO_USERS: 2, EDXORG_USERS: 1}
    report = _report(rows)

    assert render_local_branches(report, "ol_warehouse_local_raw", {}) == [
        "Ingestion branches in ol_warehouse_local_raw: 1 of 1 union model(s) built have rows in every branch."
    ]
    assert render_local_branches({}, "ol_warehouse_local_raw", {}) == []


def test_render_caps_the_tables_it_names() -> None:
    tables = [f"raw__mitxonline__app__postgres__t{n}" for n in range(8)]
    units = [_unit("mitxonline", "app_postgres", *tables), _unit("xpro", "app_postgres", XPRO_USERS)]
    registry = ManifestRegistry()
    nodes = [
        *(_node(table, "models/staging/_sources.yml", [], "source") for table in [*tables, XPRO_USERS]),
        _node("int_all", "models/intermediate/a.sql", [f"source.pkg.{table}" for table in [*tables, XPRO_USERS]]),
    ]
    registry.nodes = {node.unique_id: node for node in nodes}
    report = local_branches(union_tables(registry, units, ["int_all"]), units, {tables[0]: 1, XPRO_USERS: 1}, set())

    lines = render_local_branches(report, "ol_warehouse_local_raw", {})

    assert lines[2] == "    mitxonline/app_postgres: partial (7 of 8 table(s)); no fixture committed for this unit"
    assert lines[3].count("(absent)") == 5
    assert lines[3].endswith(", and 2 more")


def test_render_says_which_unions_were_built_on_parents_the_run_did_not_rebuild() -> None:
    """Their row counts describe the raw schema now, which is not what the union read."""
    rows = {MITXONLINE_USERS: 3, MITXONLINE_PROFILES: 3, XPRO_USERS: 2, EDXORG_USERS: 1}

    lines = render_local_branches(_report(rows), "ol_warehouse_local_raw", {"dim_user": 4})

    assert lines == [
        "Ingestion branches in ol_warehouse_local_raw: 1 of 1 union model(s) built have rows in every branch.",
        "  1 of them were built on parents this run did not rebuild, so their rows may predate these counts: "
        "dim_user. A `+` before the model in --select rebuilds its parents too.",
    ]


def _snapshot_manifest() -> ManifestRegistry:
    """dim_user reads xPRO through a snapshot, which `dbt run` does not rebuild."""
    manifest = _manifest()
    snapshot = _node("snapshot_xpro", "snapshots/xpro.sql", ["model.pkg.stg_xpro"], "snapshot")
    manifest.nodes[snapshot.unique_id] = snapshot
    manifest.nodes["model.pkg.dim_user"].depends_on = [
        "model.pkg.int_mitxonline",
        "snapshot.pkg.snapshot_xpro",
        "model.pkg.stg_edxorg",
    ]
    return manifest


def test_a_snapshot_the_run_left_alone_counts_as_a_parent_not_built() -> None:
    """Every model above it can be rebuilt while the snapshot's table stays as it was."""
    every_model = {"dim_user", "int_mitxonline", "stg_mitxonline", "stg_xpro", "stg_edxorg"}

    assert parents_not_built(_snapshot_manifest(), ["dim_user"], every_model) == {"dim_user": 1}
    assert parents_not_built(_snapshot_manifest(), ["dim_user"], every_model | {"snapshot_xpro"}) == {}


def test_models_behind_a_snapshot_are_parents_too() -> None:
    built = {"dim_user", "int_mitxonline", "stg_mitxonline", "stg_edxorg", "snapshot_xpro"}

    assert parents_not_built(_snapshot_manifest(), ["dim_user"], built) == {"dim_user": 1}


def test_remedy_for_an_ingest_unit_names_the_ingest_command() -> None:
    def branch(*, has_fixture: bool, absent: tuple[str, ...]) -> Branch:
        return Branch(
            unit="mitxonline/app_postgres",
            strategy="ingest",
            tables=(MITXONLINE_USERS,),
            absent=absent,
            empty=tuple({MITXONLINE_USERS} - set(absent)),
            has_fixture=has_fixture,
        )

    ingest = "run `ol-dbt ingest local --unit mitxonline/app_postgres` with the app running"
    assert branch(has_fixture=False, absent=(MITXONLINE_USERS,)).remedy == ingest
    assert branch(has_fixture=True, absent=(MITXONLINE_USERS,)).remedy == f"{ingest}, or `ol-dbt fixtures load`"
    assert branch(has_fixture=True, absent=()).remedy == ingest

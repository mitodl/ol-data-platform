"""Unit tests for the ingestion Dagster translators (pure, no pipeline runs).

These assert the asset specs produced by RawDataDltTranslator /
EdxorgDltTranslator via the build_ingest_assets factory. They run under the
default dev profile (filesystem storage).
"""

import pytest
from dagster import AssetKey, AssetsDefinition, AssetSpec
from data_loading.defs.ingestion import assets, translators
from ol_orchestrate.lib.constants import EDXORG_DB_TABLES


def _specs_by_key(assets_def: AssetsDefinition) -> dict[str, AssetSpec]:
    return {spec.key.to_user_string(): spec for spec in assets_def.specs}


def test_simple_source_spec_conventions() -> None:
    spec = _specs_by_key(assets.oll_assets)[
        "ol_warehouse_raw_data/raw__oll__google_sheets__courses"
    ]
    assert spec.key == AssetKey(
        ["ol_warehouse_raw_data", "raw__oll__google_sheets__courses"]
    )
    assert spec.group_name == "oll"  # scoped by source system
    assert "dlt" in spec.kinds
    assert "filesystem" in spec.kinds  # dev profile
    # Root extract: no orphaned phantom upstream dep.
    assert list(spec.deps) == []
    # Glue fully-qualified table_name metadata is set from the dataset name.
    table_name = spec.metadata["dagster/table_name"]
    table_name = getattr(table_name, "value", table_name)
    assert table_name.endswith(".raw__oll__google_sheets__courses")


def test_all_simple_sources_have_no_deps() -> None:
    for assets_def in (
        assets.oll_assets,
        assets.mitpe_assets,
        assets.mit_climate_assets,
        assets.mit_edx_programs_assets,
        assets.podcast_rss_assets,
    ):
        for spec in assets_def.specs:
            assert list(spec.deps) == [], spec.key


def _edxorg_table_asset(table_name: str) -> AssetsDefinition:
    for assets_def in assets.edxorg_s3_table_assets:
        if any(
            spec.key.path[-1] == f"raw__edxorg__s3__tables__{table_name}"
            for spec in assets_def.specs
        ):
            return assets_def
    msg = f"no edxorg_s3 asset found for table {table_name}"
    raise AssertionError(msg)


def test_edxorg_spec_has_upstream_archive_dep() -> None:
    spec = _specs_by_key(_edxorg_table_asset("auth_user"))[
        "ol_warehouse_raw_data/raw__edxorg__s3__tables__auth_user"
    ]
    assert spec.key == AssetKey(
        ["ol_warehouse_raw_data", "raw__edxorg__s3__tables__auth_user"]
    )
    assert [d.asset_key for d in spec.deps] == [
        AssetKey(["edxorg", "raw_data", "db_table", "auth_user"])
    ]
    assert "dlt" in spec.kinds
    assert spec.group_name == "edxorg"  # scoped by source system


def test_edxorg_tables_are_separate_ops() -> None:
    """Each table is its own op (not one op looping over every table).

    This is what lets Dagster's step executor run tables concurrently instead
    of one giant table head-of-line-blocking every smaller table behind it.
    """
    assert len(assets.edxorg_s3_table_assets) == len(EDXORG_DB_TABLES)
    op_names = {a.op.name for a in assets.edxorg_s3_table_assets}
    assert len(op_names) == len(EDXORG_DB_TABLES)  # every op name is distinct


def test_edxorg_tables_share_a_concurrency_pool() -> None:
    """All edxorg_s3 table ops share one pool so concurrency is centrally bounded.

    They all run inside the same Dagster run pod (K8sRunLauncher launches one
    pod per run, not per step), which has a fixed CPU/memory budget -- the pool
    lets that budget be tuned centrally via the Dagster instance UI.
    """
    pools = {a.op.pool for a in assets.edxorg_s3_table_assets}
    assert pools == {"edxorg_s3"}


def test_edxorg_programs_grouped_with_edxorg() -> None:
    # edxorg programs (discovery API) shares the edxorg source-system group with
    # the edxorg S3 tables.
    spec = _specs_by_key(assets.mit_edx_programs_assets)[
        "ol_warehouse_raw_data/raw__edxorg__discovery__api__programs"
    ]
    assert spec.group_name == "edxorg"


def test_storage_kind_tracks_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(translators.config, "active_table_format", lambda: "iceberg")
    assert translators._storage_kind() == "iceberg"
    monkeypatch.setattr(translators.config, "active_table_format", lambda: "native")
    assert translators._storage_kind() == "filesystem"

"""Unit tests for ol_orchestrate.lib.iceberg_maintenance."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest
from ol_orchestrate.lib.iceberg_maintenance import (
    AIRBYTE_STAGING_BRANCH,
    RAW_LAYER_GROUP_CONFIGS,
    TableMaintenanceConfig,
    expire_snapshots,
    load_maintenance_configs_from_manifest,
    maintenance_failure_threshold,
    non_dbt_singleton_tables,
    partition_by_catalog_presence,
    raw_config_for_table,
    scope_schema_to_env,
    stale_branches,
    warehouse_env_for,
)
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRef, SnapshotRefType

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_manifest(nodes: dict[str, object]) -> dict[str, object]:
    """Wrap a nodes dict in the minimal manifest.json envelope."""
    return {"metadata": {}, "nodes": nodes, "sources": {}, "exposures": {}}


def _iceberg_meta(
    *,
    enabled: bool = True,
    snapshot_retention_days: int = 7,
    optimize_after_every_n_runs: int = 1,
    analyze_after_every_n_runs: int = 7,
) -> dict[str, object]:
    """Return a fully-specified iceberg_maintenance meta dict."""
    return {
        "enabled": enabled,
        "snapshot_retention_days": snapshot_retention_days,
        "optimize_after_every_n_runs": optimize_after_every_n_runs,
        "analyze_after_every_n_runs": analyze_after_every_n_runs,
    }


def _model_node(
    unique_id: str,
    *,
    schema: str = "ol_warehouse_production_mart",
    config_schema: str = "mart",
    materialized: str = "table",
    iceberg_meta: dict[str, object] | None = None,
) -> dict[str, object]:
    """Build a minimal manifest model node dict."""
    meta: dict[str, object] = {"required_docs": True}
    if iceberg_meta is not None:
        meta["iceberg_maintenance"] = iceberg_meta
    return {
        "unique_id": unique_id,
        "resource_type": "model",
        "schema": schema,
        "database": "ol_data_lake_production",
        "name": unique_id.rsplit(".", maxsplit=1)[-1],
        "config": {
            "materialized": materialized,
            "schema": config_schema,
            "meta": meta,
        },
    }


# ── load_maintenance_configs_from_manifest ────────────────────────────────────


class TestLoadMaintenanceConfigsFromManifest:
    """Tests for load_maintenance_configs_from_manifest."""

    def test_returns_model_with_full_iceberg_meta(self, tmp_path: Path) -> None:
        """A model with iceberg_maintenance in compiled meta is included."""
        manifest = _make_manifest(
            {
                "model.proj.mart__revenue": _model_node(
                    "model.proj.mart__revenue",
                    schema="ol_warehouse_production_mart",
                    config_schema="mart",
                    iceberg_meta=_iceberg_meta(snapshot_retention_days=14),
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="production"
        )

        assert len(configs) == 1
        cfg = configs[0]
        assert cfg.model_name == "mart__revenue"
        assert cfg.schema_name == "ol_warehouse_production_mart"
        assert cfg.snapshot_retention_days == 14
        assert cfg.optimize_after_every_n_runs == 1
        assert cfg.analyze_after_every_n_runs == 7
        assert cfg.asset_key == ["mart", "mart__revenue"]

    def test_layer_comes_from_node_schema_not_config_schema(
        self, tmp_path: Path
    ) -> None:
        """The layer is taken from node['schema'], not rebuilt from config.schema."""
        manifest = _make_manifest(
            {
                "model.proj.dim_user": _model_node(
                    "model.proj.dim_user",
                    schema="ol_warehouse_production_dimensional",
                    config_schema="dimensional",
                    iceberg_meta=_iceberg_meta(snapshot_retention_days=14),
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="production"
        )

        assert len(configs) == 1
        assert configs[0].schema_name == "ol_warehouse_production_dimensional"

    def test_a_qa_load_never_returns_a_production_schema(self, tmp_path: Path) -> None:
        """The DAGSTER-R bug: one manifest, compiled against production, shipped
        to every environment.

        QA read ``ol_warehouse_production_intermediate`` straight out of the
        manifest and issued OPTIMIZE and ANALYZE against it. The only thing that
        stopped it was data-lake-query-engine-role-qa lacking glue:GetTable --
        an IAM denial standing in for a scoping rule that was never written.
        """
        manifest = _make_manifest(
            {
                "model.proj.int_enrollments": _model_node(
                    "model.proj.int_enrollments",
                    schema="ol_warehouse_production_intermediate",
                    config_schema="intermediate",
                    iceberg_meta=_iceberg_meta(),
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="qa"
        )

        assert configs[0].schema_name == "ol_warehouse_qa_intermediate"

    def test_model_without_iceberg_meta_is_skipped(self, tmp_path: Path) -> None:
        """Models without iceberg_maintenance in compiled meta are excluded."""
        manifest = _make_manifest(
            {
                "model.proj.stg_users": _model_node(
                    "model.proj.stg_users",
                    schema="ol_warehouse_production_staging",
                    config_schema="staging",
                    iceberg_meta=None,
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="production"
        )

        assert configs == []

    def test_disabled_model_is_skipped(self, tmp_path: Path) -> None:
        """Models with iceberg_maintenance.enabled=false are excluded."""
        manifest = _make_manifest(
            {
                "model.proj.external_legacy": _model_node(
                    "model.proj.external_legacy",
                    iceberg_meta=_iceberg_meta(enabled=False),
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="production"
        )

        assert configs == []

    def test_view_and_ephemeral_models_are_skipped(self, tmp_path: Path) -> None:
        """Non-table/incremental materializations are always excluded."""
        manifest = _make_manifest(
            {
                "model.proj.view_model": _model_node(
                    "model.proj.view_model",
                    materialized="view",
                    iceberg_meta=_iceberg_meta(),
                ),
                "model.proj.ephemeral_model": _model_node(
                    "model.proj.ephemeral_model",
                    materialized="ephemeral",
                    iceberg_meta=_iceberg_meta(),
                ),
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="production"
        )

        assert configs == []

    def test_incremental_model_is_included(self, tmp_path: Path) -> None:
        """Incremental materialization is treated the same as table."""
        manifest = _make_manifest(
            {
                "model.proj.dim_enrollment": _model_node(
                    "model.proj.dim_enrollment",
                    schema="ol_warehouse_production_dimensional",
                    config_schema="dimensional",
                    materialized="incremental",
                    iceberg_meta=_iceberg_meta(snapshot_retention_days=14),
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="production"
        )

        assert len(configs) == 1
        assert configs[0].materialized == "incremental"

    def test_non_model_nodes_are_skipped(self, tmp_path: Path) -> None:
        """Source and seed nodes are excluded regardless of meta."""
        manifest = _make_manifest(
            {
                "source.proj.raw.users": {
                    "unique_id": "source.proj.raw.users",
                    "resource_type": "source",
                    "schema": "ol_warehouse_production_raw",
                    "name": "users",
                    "config": {"meta": {}},
                },
                "seed.proj.country_codes": {
                    "unique_id": "seed.proj.country_codes",
                    "resource_type": "seed",
                    "schema": "ol_warehouse_production_staging",
                    "name": "country_codes",
                    "config": {
                        "materialized": "seed",
                        "schema": "staging",
                        "meta": {"iceberg_maintenance": _iceberg_meta()},
                    },
                },
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="production"
        )

        assert configs == []

    def test_asset_key_uses_config_schema_suffix(self, tmp_path: Path) -> None:
        """asset_key[0] is the bare config.schema suffix, not the full schema name.

        This matches DbtAutomationTranslator.get_group_name which returns
        config.schema (e.g. "mart"), not "ol_warehouse_production_mart".
        """
        manifest = _make_manifest(
            {
                "model.proj.fct_enrollments": _model_node(
                    "model.proj.fct_enrollments",
                    schema="ol_warehouse_production_mart",
                    config_schema="mart",
                    iceberg_meta=_iceberg_meta(),
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(
            manifest_path, warehouse_env="production"
        )

        assert configs[0].asset_key == ["mart", "fct_enrollments"]


# ── raw_config_for_table ──────────────────────────────────────────────────────


class TestRawConfigForTable:
    """Tests for raw_config_for_table prefix matching."""

    def test_high_frequency_mitxonline_app(self) -> None:
        """A table under raw__mitxonline__app gets the 3-day retention config."""
        cfg = raw_config_for_table("raw__mitxonline__app__postgres__auth_user")
        assert cfg.snapshot_retention_days == 3

    def test_high_frequency_xpro_app(self) -> None:
        """A table under raw__xpro__app gets the 3-day retention config."""
        cfg = raw_config_for_table("raw__xpro__app__postgres__ecommerce_order")
        assert cfg.snapshot_retention_days == 3

    def test_third_party_salesforce(self) -> None:
        """A Salesforce table gets the 14-day retention config."""
        cfg = raw_config_for_table(
            "raw__thirdparty__salesforce___destination_v2__account"
        )
        assert cfg.snapshot_retention_days == 14

    def test_third_party_zendesk(self) -> None:
        """A Zendesk table gets the 14-day retention config."""
        cfg = raw_config_for_table("raw__thirdparty__zendesk_support__ticket")
        assert cfg.snapshot_retention_days == 14

    def test_unknown_prefix_returns_default(self) -> None:
        """A table not matching any prefix returns the _default config (7 days)."""
        cfg = raw_config_for_table("raw__edxorg__course_structure__data")
        assert cfg == RAW_LAYER_GROUP_CONFIGS["_default"]
        assert cfg.snapshot_retention_days == 7

    def test_non_matching_mitxonline_prefix(self) -> None:
        """raw__mitxonline__openedx does not start with raw__mitxonline__app."""
        cfg = raw_config_for_table("raw__mitxonline__openedx__api__course_blocks")
        assert cfg == RAW_LAYER_GROUP_CONFIGS["_default"]

    def test_empty_string_returns_default(self) -> None:
        """Empty table name returns the default config."""
        cfg = raw_config_for_table("")
        assert cfg == RAW_LAYER_GROUP_CONFIGS["_default"]

    def test_default_config_values(self) -> None:
        """The _default sentinel has the expected retention values."""
        default = RAW_LAYER_GROUP_CONFIGS["_default"]
        assert default.snapshot_retention_days == 7


# ── Environment scoping ───────────────────────────────────────────────────────


class TestEnvironmentScoping:
    """Which warehouse a given Dagster environment is allowed to touch."""

    @pytest.mark.parametrize(
        ("dagster_env", "warehouse_env"),
        [
            ("production", "production"),
            ("qa", "qa"),
            # dev targets the dev_production dbt profile, so a developer is
            # already reading real production tables.
            ("dev", "production"),
            ("ci", "qa"),
        ],
    )
    def test_known_environments_map_to_their_warehouse(
        self, dagster_env: str, warehouse_env: str
    ) -> None:
        assert warehouse_env_for(dagster_env) == warehouse_env

    def test_an_unknown_environment_falls_back_to_qa(self) -> None:
        """A typo'd or new DAGSTER_ENV must not inherit production."""
        assert warehouse_env_for("staging") == "qa"

    @pytest.mark.parametrize(
        ("schema", "expected"),
        [
            ("ol_warehouse_production_intermediate", "ol_warehouse_qa_intermediate"),
            ("ol_warehouse_production_mart", "ol_warehouse_qa_mart"),
            ("ol_warehouse_qa_raw", "ol_warehouse_qa_raw"),
            # The layer keeps its own underscores.
            ("ol_warehouse_production_raw_data", "ol_warehouse_qa_raw_data"),
        ],
    )
    def test_the_env_segment_is_rewritten_and_the_layer_kept(
        self, schema: str, expected: str
    ) -> None:
        assert scope_schema_to_env(schema, "qa") == expected

    def test_a_schema_with_no_env_segment_is_rejected(self) -> None:
        """Passing it through would be a silent cross-environment write."""
        with pytest.raises(ValueError, match="ol_warehouse_<env>_<layer>"):
            scope_schema_to_env("information_schema", "qa")

    def test_singletons_are_scoped_to_the_calling_environment(self) -> None:
        """The hand-written list hardcoded production for every environment."""
        assert [t.schema_name for t in non_dbt_singleton_tables("qa")] == [
            "ol_warehouse_qa_reporting",
            "ol_warehouse_qa_intermediate",
            "ol_warehouse_qa_intermediate",
            "ol_warehouse_qa_intermediate",
        ]


# ── Failure threshold ─────────────────────────────────────────────────────────


class TestMaintenanceFailureThreshold:
    """The asset's contract is "fail if more than 5% of tables failed".

    Getting that boundary wrong in the tripping direction turns nightly
    maintenance into a nightly false alarm, which is how an alerting channel
    stops being read.
    """

    @pytest.mark.parametrize(
        ("tables_attempted", "expected"),
        [
            # The two boundaries that were wrong under max(1, int(5%)): one
            # failure in 21 is 4.8%, and exactly five in 100 is 5% -- neither is
            # *more* than 5%.
            (21, 2),
            (100, 6),
            # A single failure is over the line for any small set.
            (1, 1),
            (20, 2),
            # The real fleet size.
            (628, 32),
        ],
    )
    def test_the_first_count_over_five_percent(
        self, tables_attempted: int, expected: int
    ) -> None:
        assert maintenance_failure_threshold(tables_attempted) == expected

    @pytest.mark.parametrize("tables_attempted", [1, 20, 21, 99, 100, 628, 1300])
    def test_the_threshold_is_always_genuinely_over_five_percent(
        self, tables_attempted: int
    ) -> None:
        """The property behind the table above, stated directly."""
        threshold = maintenance_failure_threshold(tables_attempted)

        assert threshold / tables_attempted > 0.05
        assert (threshold - 1) / tables_attempted <= 0.05, (
            "and it is the *first* such count"
        )

    def test_a_single_failure_never_slips_through(self) -> None:
        """The floor of one is load-bearing: an empty run must not report a
        threshold of zero and fail on nothing.
        """
        assert maintenance_failure_threshold(0) == 1


# ── Catalog reconciliation ────────────────────────────────────────────────────


class TestPartitionByCatalogPresence:
    """The manifest is compiled against production and shipped everywhere.

    QA holds views under names the manifest calls tables, and holds nothing at
    all under others. Both are unmaintainable and both were being attempted:
    401 of 628 configs failed in a single QA run on ``ALTER TABLE EXECUTE is
    not supported for views`` and ``TABLE_NOT_FOUND``.
    """

    @staticmethod
    def _cfg(schema: str, model: str) -> TableMaintenanceConfig:
        return TableMaintenanceConfig(
            model_name=model,
            schema_name=schema,
            materialized="table",
            asset_key=[schema.rsplit("_", 1)[-1], model],
        )

    def test_a_config_the_catalog_calls_a_table_is_kept(self) -> None:
        cfg = self._cfg("ol_warehouse_qa_intermediate", "int__mitx__enrollments")
        present, unbacked = partition_by_catalog_presence(
            [cfg], {("ol_warehouse_qa_intermediate", "int__mitx__enrollments")}
        )
        assert present == [cfg]
        assert unbacked == []

    def test_a_view_is_dropped_and_named(self) -> None:
        """A view is in the catalog but not in the BASE TABLE set."""
        cfg = self._cfg("ol_warehouse_qa_intermediate", "int__mitx__users")
        present, unbacked = partition_by_catalog_presence([cfg], set())
        assert present == []
        assert unbacked == ["ol_warehouse_qa_intermediate.int__mitx__users"]

    def test_a_table_missing_from_the_environment_is_dropped(self) -> None:
        cfg = self._cfg("ol_warehouse_qa_intermediate", "int__mitx__courses")
        present, unbacked = partition_by_catalog_presence(
            [cfg], {("ol_warehouse_qa_intermediate", "something_else")}
        )
        assert present == []
        assert unbacked == ["ol_warehouse_qa_intermediate.int__mitx__courses"]

    def test_the_match_is_schema_qualified(self) -> None:
        """A production table of the same name must not vouch for the QA one.

        Matching on model name alone would let production's build satisfy QA's
        config, which is the cross-environment confusion scope_schema_to_env
        exists to prevent.
        """
        cfg = self._cfg("ol_warehouse_qa_intermediate", "int__mitx__users")
        present, unbacked = partition_by_catalog_presence(
            [cfg], {("ol_warehouse_production_intermediate", "int__mitx__users")}
        )
        assert present == []
        assert unbacked == ["ol_warehouse_qa_intermediate.int__mitx__users"]

    def test_order_is_preserved_for_the_kept_configs(self) -> None:
        first = self._cfg("ol_warehouse_qa_mart", "a")
        second = self._cfg("ol_warehouse_qa_mart", "b")
        third = self._cfg("ol_warehouse_qa_mart", "c")
        present, unbacked = partition_by_catalog_presence(
            [first, second, third],
            {("ol_warehouse_qa_mart", "a"), ("ol_warehouse_qa_mart", "c")},
        )
        assert present == [first, third]
        assert unbacked == ["ol_warehouse_qa_mart.b"]

    def test_an_empty_config_list_yields_nothing(self) -> None:
        assert partition_by_catalog_presence([], {("s", "t")}) == ([], [])


class TestExpireSnapshots:
    """Expiry against a real pyiceberg catalog, with Airbyte-style branches.

    A retention of 0 days puts the cutoff at "now", so every snapshot written
    before the call is past retention. ``main``'s head is the fourth snapshot.
    """

    STALE = "airbyte_staging_927f86be_fdb5_3838_a59d_d1786601233e"
    DATABASE = "raw"
    TABLE = "raw__mitlearn__app__postgres__users_user"

    @pytest.fixture
    def catalog(self, tmp_path: Path) -> SqlCatalog:
        catalog = SqlCatalog(
            "test",
            uri=f"sqlite:///{tmp_path}/catalog.db",
            warehouse=f"file://{tmp_path}/warehouse",
        )
        catalog.create_namespace(self.DATABASE)
        rows = pa.table({"id": pa.array([1], type=pa.int64())})
        table = catalog.create_table(f"{self.DATABASE}.{self.TABLE}", rows.schema)
        for _ in range(4):
            table.append(rows)
        first, second, _third, _head = (s.snapshot_id for s in table.snapshots())
        (
            table.manage_snapshots()
            .create_branch(first, self.STALE)
            .create_branch(second, "airbyte_staging")
            .commit()
        )
        return catalog

    def _table(self, catalog: SqlCatalog) -> Table:
        return catalog.load_table(f"{self.DATABASE}.{self.TABLE}")

    def test_a_stale_staging_branch_goes_and_takes_its_snapshot_with_it(
        self, catalog: SqlCatalog
    ) -> None:
        before = [s.snapshot_id for s in self._table(catalog).snapshots()]

        result = expire_snapshots(
            catalog,
            self.DATABASE,
            self.TABLE,
            retention_days=0,
            stale_branch_pattern=AIRBYTE_STAGING_BRANCH,
        )

        table = self._table(catalog)
        assert result["skipped"] is False
        assert result["stale_branch_count"] == 1
        assert set(table.metadata.refs) == {"main", "airbyte_staging"}
        # The bare airbyte_staging head and main's head are all that is left.
        assert [s.snapshot_id for s in table.snapshots()] == [before[1], before[3]]
        assert result["eligible_count"] == len(before) - len(table.snapshots())

    def test_without_a_pattern_a_branch_head_is_neither_expired_nor_counted(
        self, catalog: SqlCatalog
    ) -> None:
        before = [s.snapshot_id for s in self._table(catalog).snapshots()]

        result = expire_snapshots(catalog, self.DATABASE, self.TABLE, retention_days=0)

        table = self._table(catalog)
        assert self.STALE in table.metadata.refs
        assert [s.snapshot_id for s in table.snapshots()] == [
            before[0],
            before[1],
            before[3],
        ]
        assert result["eligible_count"] == 1
        assert result["stale_branch_count"] == 0

    def test_a_branch_inside_the_retention_window_is_kept(
        self, catalog: SqlCatalog
    ) -> None:
        result = expire_snapshots(
            catalog,
            self.DATABASE,
            self.TABLE,
            retention_days=1,
            stale_branch_pattern=AIRBYTE_STAGING_BRANCH,
        )

        assert result["skipped"] is True
        assert result["reason"] == "no eligible snapshots"
        assert self.STALE in self._table(catalog).metadata.refs

    def test_a_dry_run_reports_and_changes_nothing(self, catalog: SqlCatalog) -> None:
        before = self._table(catalog).metadata

        result = expire_snapshots(
            catalog,
            self.DATABASE,
            self.TABLE,
            retention_days=0,
            dry_run=True,
            stale_branch_pattern=AIRBYTE_STAGING_BRANCH,
        )

        assert result["dry_run"] is True
        assert result["eligible_count"] == 2
        assert result["stale_branch_count"] == 1
        assert self._table(catalog).metadata == before

    def test_a_table_that_cannot_be_loaded_raises(self, catalog: SqlCatalog) -> None:
        with pytest.raises(NoSuchTableError):
            expire_snapshots(catalog, self.DATABASE, "missing", retention_days=0)

    @pytest.mark.parametrize(
        "name",
        [
            "main",
            "airbyte_staging",
            "airbyte_staging_backup",
            "audit_2026",
            f"{STALE}_extra",
            f"{STALE}\n",
        ],
    )
    def test_only_uuid_suffixed_staging_branches_match(self, name: str) -> None:
        assert AIRBYTE_STAGING_BRANCH.fullmatch(name) is None
        assert AIRBYTE_STAGING_BRANCH.fullmatch(self.STALE)

    def test_a_branch_whose_head_is_missing_is_left_alone(
        self, catalog: SqlCatalog
    ) -> None:
        metadata = self._table(catalog).metadata
        dangling = "airbyte_staging_00000000_0000_0000_0000_000000000000"
        metadata = metadata.model_copy(
            update={
                "refs": {
                    **metadata.refs,
                    dangling: SnapshotRef(
                        snapshot_id=123, snapshot_ref_type=SnapshotRefType.BRANCH
                    ),
                }
            }
        )
        cutoff_ms = max(s.timestamp_ms for s in metadata.snapshots) + 1

        assert stale_branches(metadata, cutoff_ms, AIRBYTE_STAGING_BRANCH) == [
            self.STALE
        ]

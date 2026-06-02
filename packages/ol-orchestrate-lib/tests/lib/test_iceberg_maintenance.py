"""Unit tests for ol_orchestrate.lib.iceberg_maintenance."""

from __future__ import annotations

import json
from pathlib import Path

from ol_orchestrate.lib.iceberg_maintenance import (
    RAW_LAYER_GROUP_CONFIGS,
    load_maintenance_configs_from_manifest,
    raw_config_for_table,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_manifest(nodes: dict[str, object]) -> dict[str, object]:
    """Wrap a nodes dict in the minimal manifest.json envelope."""
    return {"metadata": {}, "nodes": nodes, "sources": {}, "exposures": {}}


def _iceberg_meta(
    *,
    enabled: bool = True,
    snapshot_retention_days: int = 7,
    orphan_retention_days: int = 7,
    optimize_after_every_n_runs: int = 1,
    analyze_after_every_n_runs: int = 7,
) -> dict[str, object]:
    """Return a fully-specified iceberg_maintenance meta dict."""
    return {
        "enabled": enabled,
        "snapshot_retention_days": snapshot_retention_days,
        "orphan_retention_days": orphan_retention_days,
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

        configs = load_maintenance_configs_from_manifest(manifest_path)

        assert len(configs) == 1
        cfg = configs[0]
        assert cfg.model_name == "mart__revenue"
        assert cfg.schema_name == "ol_warehouse_production_mart"
        assert cfg.snapshot_retention_days == 14
        assert cfg.orphan_retention_days == 7
        assert cfg.optimize_after_every_n_runs == 1
        assert cfg.analyze_after_every_n_runs == 7
        assert cfg.asset_key == ["mart", "mart__revenue"]

    def test_schema_name_from_node_schema_not_config_schema(
        self, tmp_path: Path
    ) -> None:
        """node['schema'] is used directly; config.schema is not reconstructed."""
        manifest = _make_manifest(
            {
                "model.proj.dim_user": _model_node(
                    "model.proj.dim_user",
                    schema="ol_warehouse_production_dimensional",
                    config_schema="dimensional",
                    iceberg_meta=_iceberg_meta(
                        snapshot_retention_days=14, orphan_retention_days=14
                    ),
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(manifest_path)

        assert len(configs) == 1
        assert configs[0].schema_name == "ol_warehouse_production_dimensional"

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

        configs = load_maintenance_configs_from_manifest(manifest_path)

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

        configs = load_maintenance_configs_from_manifest(manifest_path)

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

        configs = load_maintenance_configs_from_manifest(manifest_path)

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
                    iceberg_meta=_iceberg_meta(
                        snapshot_retention_days=14, orphan_retention_days=14
                    ),
                )
            }
        )
        manifest_path = tmp_path / "manifest.json"
        manifest_path.write_text(json.dumps(manifest))

        configs = load_maintenance_configs_from_manifest(manifest_path)

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

        configs = load_maintenance_configs_from_manifest(manifest_path)

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

        configs = load_maintenance_configs_from_manifest(manifest_path)

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
        assert default.orphan_retention_days == 7

"""Tests for the QA mirror assets.

Rendering is tested in `src/ol_dbt_cli/tests/test_qa_mirror.py`. What is left
here is the Dagster side: the statements run in an order that leaves no stale
copy behind, and nothing but a person can trigger a refresh.
"""

from pathlib import Path
from typing import Any

import pytest
from dagster import Failure, build_asset_context
from lakehouse.assets.qa_mirror import _mirror_asset, build_qa_mirror_assets
from ol_dbt_cli.lib.qa_mirror import MirrorDeclarationError, MirrorTable

TABLE = MirrorTable(
    unit="emeritus/bigquery",
    raw_table="raw__emeritus__bigquery__api_enrollments",
    columns={"_airbyte_extracted_at": "copy", "email": "hash"},
    where=None,
)
TARGET = (
    "ol_data_lake_qa.ol_warehouse_qa_raw.`raw__emeritus__bigquery__api_enrollments`"
)


class FakeStarRocks:
    def __init__(self, described: list[dict[str, str]]) -> None:
        self.described = described
        self.statements: list[str] = []
        self.non_idempotent: list[str] = []

    def fetch(self, sql: str) -> list[dict[str, Any]]:
        self.statements.append(sql)
        if sql.startswith("DESCRIBE"):
            return self.described
        return [{"row_count": 42}]

    def execute(self, sql: str, *, idempotent: bool = True) -> None:
        self.statements.append(sql)
        if not idempotent:
            self.non_idempotent.append(sql)


PRODUCTION = [
    {"Field": "_airbyte_extracted_at", "Type": "bigint"},
    {"Field": "email", "Type": "varchar(1048576)"},
    {"Field": "first_name", "Type": "varchar(1048576)"},
]


def _materialise(
    starrocks: FakeStarRocks, tables: list[MirrorTable] | None = None
) -> Any:
    asset = _mirror_asset(TABLE.unit, tables or [TABLE])
    return asset(build_asset_context(), starrocks)


def test_describes_drops_then_copies() -> None:
    starrocks = FakeStarRocks(PRODUCTION)
    result = _materialise(starrocks)

    describe, drop, ctas, count = starrocks.statements
    assert describe.startswith("DESCRIBE ol_data_lake_production.")
    assert drop == f"DROP TABLE IF EXISTS {TARGET} FORCE"
    assert ctas.startswith(f"CREATE TABLE {TARGET}\nAS SELECT")
    assert count.endswith(f"FROM {TARGET}")
    # A retried CTAS that died after creating the table would fail on "already
    # exists" and hide the real error.
    assert starrocks.non_idempotent == [ctas]
    copied = result.metadata["tables"].data["raw__emeritus__bigquery__api_enrollments"]
    assert copied == {
        "rows": 42,
        "masked": {"email": "hash"},
        "dropped": ["first_name"],
        "where": None,
    }


def test_a_stale_declaration_fails_before_any_qa_copy_in_the_unit_is_dropped() -> None:
    stale = MirrorTable(
        unit=TABLE.unit,
        raw_table="raw__emeritus__bigquery__other",
        columns={"gone": "copy"},
        where=None,
    )
    starrocks = FakeStarRocks(PRODUCTION)
    with pytest.raises(MirrorDeclarationError):
        _materialise(starrocks, [TABLE, stale])
    assert [s.split()[0] for s in starrocks.statements] == ["DESCRIBE", "DESCRIBE"]


def test_the_real_inventory_builds_one_manual_asset_per_mirrored_unit() -> None:
    assets = build_qa_mirror_assets()
    keys = {key.to_user_string() for asset in assets for key in asset.keys}
    assert "qa_mirror/zendesk/api" in keys
    assert "qa_mirror/edxorg/tracking_logs" in keys
    for asset in assets:
        # QA_DATA_TOPOLOGY_SPEC.md §1: a refresh copies production PII into QA,
        # so each one has to be something a person asked for.
        assert asset.partitions_def is None
        assert all(
            condition is None
            for condition in asset.automation_conditions_by_key.values()
        )


def test_a_missing_inventory_is_a_failing_asset_not_an_empty_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("lakehouse.assets.qa_mirror.INVENTORY_DIR", tmp_path)
    (asset,) = build_qa_mirror_assets()
    assert asset.key.to_user_string() == "qa_mirror/inventory_missing"
    with pytest.raises(Failure, match="No inventory units found"):
        asset()

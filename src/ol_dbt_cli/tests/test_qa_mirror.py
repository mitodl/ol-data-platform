"""Tests for rendering the QA mirror's CREATE TABLE AS SELECT statements."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from ol_dbt_cli.lib.inventory import Unit, load_units
from ol_dbt_cli.lib.qa_mirror import (
    MirrorDeclarationError,
    MirrorTable,
    mirror_tables,
    render_mirror,
)

REAL_INVENTORY = Path(__file__).resolve().parents[3] / "ingestion" / "inventory"

PRODUCTION_TYPES = {
    "_airbyte_extracted_at": "bigint",
    "user id": "varchar(1048576)",
    "email": "varchar(1048576)",
    "birth_year": "int",
    "last_name": "varchar(1048576)",
}


def _table(columns: dict[str, str], where: str | None = None) -> MirrorTable:
    return MirrorTable(unit="edxorg/s3", raw_table="raw__edxorg__Report", columns=columns, where=where)


def _unit(qa: str, tables: list[dict[str, Any]]) -> Unit:
    return Unit(
        path=Path("x.yml"), data={"deployment": "edxorg", "layer": "s3", "strategies": {"qa": qa}, "tables": tables}
    )


class TestRenderMirror:
    def test_copies_masks_and_drops(self) -> None:
        statement = render_mirror(
            _table({"_airbyte_extracted_at": "copy", "user id": "copy", "email": "hash", "birth_year": "nullify"}),
            PRODUCTION_TYPES,
        )
        assert statement.sql.startswith(
            "CREATE TABLE ol_data_lake_qa.ol_warehouse_qa_raw.`raw__edxorg__report`\nAS SELECT /*+ SET_VAR("
        )
        assert "`user id`,\n" in statement.sql
        assert "sha2(nullif(`email`, ''), 256) AS `email`" in statement.sql
        # Typed by the dead branch, so QA keeps production's column type.
        assert "CASE WHEN FALSE THEN `birth_year` END AS `birth_year`" in statement.sql
        assert statement.sql.endswith("FROM ol_data_lake_production.ol_warehouse_production_raw.`raw__edxorg__report`")
        assert statement.dropped == ["last_name"]

    def test_where_is_relative_to_the_production_table(self) -> None:
        where = "_airbyte_extracted_at >= (SELECT max(_airbyte_extracted_at) FROM {source}) - 86400000"
        statement = render_mirror(_table({"_airbyte_extracted_at": "copy"}, where), PRODUCTION_TYPES)
        assert statement.sql.endswith(
            "\nWHERE (_airbyte_extracted_at >= (SELECT max(_airbyte_extracted_at) FROM "
            "ol_data_lake_production.ol_warehouse_production_raw.`raw__edxorg__report`) - 86400000)"
        )

    def test_a_column_production_lacks_fails(self) -> None:
        with pytest.raises(MirrorDeclarationError, match="production does not have"):
            render_mirror(_table({"_file_modified_at": "copy"}), PRODUCTION_TYPES)

    @pytest.mark.parametrize("mode", ["hash", "redact"])
    def test_string_modes_refuse_a_non_string_column(self, mode: str) -> None:
        # Either would turn an int into a string under staging models that cast it.
        with pytest.raises(MirrorDeclarationError, match="need string columns"):
            render_mirror(_table({"birth_year": mode}), PRODUCTION_TYPES)

    def test_redact_keeps_nulls_null(self) -> None:
        # So a not_null test on the column fails in QA exactly when it would in production.
        statement = render_mirror(_table({"last_name": "redact"}), PRODUCTION_TYPES)
        assert "CASE WHEN `last_name` IS NULL THEN NULL ELSE 'redacted' END AS `last_name`" in statement.sql

    def test_column_names_match_case_insensitively(self) -> None:
        statement = render_mirror(_table({"Email": "hash"}), {"email": "VARCHAR(65533)"})
        assert "sha2(nullif(`Email`, ''), 256)" in statement.sql


class TestMirrorTables:
    def test_only_mirror_units_and_declared_tables(self) -> None:
        declared = {"raw_table": "raw__edxorg__a", "mirror": {"columns": {"id": "copy"}}}
        undeclared = {"raw_table": "raw__edxorg__b"}
        grouped = mirror_tables([_unit("mirror", [declared, undeclared]), _unit("omit", [declared])])
        assert list(grouped) == ["edxorg/s3"]
        assert [t.raw_table for t in grouped["edxorg/s3"]] == ["raw__edxorg__a"]

    def test_every_real_mirror_declaration_renders_its_own_columns(self) -> None:
        # Against a production schema holding exactly the allowlisted columns as
        # strings: catches a declaration the renderer cannot express at all.
        for tables in mirror_tables(load_units(REAL_INVENTORY)).values():
            for table in tables:
                types = dict.fromkeys(table.columns, "varchar(65533)")
                assert render_mirror(table, types).dropped == []

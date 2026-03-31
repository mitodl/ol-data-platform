"""Tests for commands/generate.py — source/staging scaffold helpers."""

from __future__ import annotations

import pytest

from ol_dbt_cli.commands.generate import (
    _adjust_source_schema_pattern,
    _extract_domain,
    _merge_sources_content,
)


class TestExtractDomain:
    def test_standard_prefix(self) -> None:
        assert _extract_domain("raw__mitlearn__app__postgres__users") == "mitlearn"

    def test_learn_ai_prefix(self) -> None:
        assert _extract_domain("raw__learn_ai__app__postgres__oauth2_grant") == "learn_ai"

    def test_two_part_prefix(self) -> None:
        assert _extract_domain("raw__edxorg") == "edxorg"

    def test_single_segment_returns_empty(self) -> None:
        """Prefix without __ delimiter — second part doesn't exist."""
        assert _extract_domain("raw") == ""

    def test_empty_string_returns_empty(self) -> None:
        assert _extract_domain("") == ""


class TestAdjustSourceSchemaPattern:
    _BASE = "version: 2\n\nsources:\n- name: ol_warehouse_production_raw\n  tables:\n  - name: my_table\n"

    def test_replaces_production_schema_name(self) -> None:
        result = _adjust_source_schema_pattern(self._BASE)
        assert "ol_warehouse_raw_data" in result
        assert "ol_warehouse_production_raw" not in result

    def test_injects_loader_airbyte(self) -> None:
        result = _adjust_source_schema_pattern(self._BASE)
        assert "loader: airbyte" in result

    def test_injects_database_jinja(self) -> None:
        result = _adjust_source_schema_pattern(self._BASE)
        assert "{{ target.database }}" in result

    def test_injects_schema_jinja(self) -> None:
        result = _adjust_source_schema_pattern(self._BASE)
        assert "schema_suffix" in result
        assert "_raw" in result

    def test_custom_original_schema(self) -> None:
        content = "sources:\n- name: my_custom_schema\n  tables:\n  - name: foo\n"
        result = _adjust_source_schema_pattern(content, original_schema="my_custom_schema")
        assert "ol_warehouse_raw_data" in result
        assert "my_custom_schema" not in result

    def test_does_not_duplicate_loader_if_already_present(self) -> None:
        """Idempotent: running twice doesn't insert loader/database/schema twice."""
        first = _adjust_source_schema_pattern(self._BASE)
        second = _adjust_source_schema_pattern(first)
        assert second.count("loader: airbyte") == 1


class TestMergeSourcesContent:
    """Tests for _merge_sources_content — incremental YAML merge logic."""

    _EXISTING = """\
version: 2

sources:
- name: ol_warehouse_raw_data
  loader: airbyte
  database: '{{ target.database }}'
  schema: '{{ target.schema.replace(var("schema_suffix", ""), "").rstrip("_") }}_raw'
  tables:
  - name: existing_table
    columns:
    - name: id
      data_type: bigint
    - name: created_at
      data_type: timestamp
"""

    _NEW_TABLE = """\
version: 2

sources:
- name: ol_warehouse_production_raw
  tables:
  - name: new_table
    columns:
    - name: record_id
      data_type: varchar
    - name: updated_at
      data_type: timestamp
"""

    def test_adds_new_table_to_existing_source(self) -> None:
        result = _merge_sources_content(self._EXISTING, self._NEW_TABLE)
        assert "new_table" in result
        assert "existing_table" in result

    def test_preserves_existing_table_columns(self) -> None:
        result = _merge_sources_content(self._EXISTING, self._NEW_TABLE)
        assert "id" in result
        assert "created_at" in result

    def test_new_table_columns_present(self) -> None:
        result = _merge_sources_content(self._EXISTING, self._NEW_TABLE)
        assert "record_id" in result

    def test_merges_new_column_into_existing_table(self) -> None:
        """New column on an existing table should be added."""
        new_with_extra_col = """\
version: 2

sources:
- name: ol_warehouse_production_raw
  tables:
  - name: existing_table
    columns:
    - name: id
      data_type: bigint
    - name: brand_new_col
      data_type: boolean
"""
        result = _merge_sources_content(self._EXISTING, new_with_extra_col)
        assert "brand_new_col" in result
        assert "id" in result

    def test_preserves_column_with_description(self) -> None:
        """Existing column with a description should not have its description overwritten."""
        existing_with_desc = """\
version: 2

sources:
- name: ol_warehouse_raw_data
  loader: airbyte
  tables:
  - name: existing_table
    columns:
    - name: id
      data_type: bigint
      description: "Primary key"
"""
        new_changing_type = """\
version: 2

sources:
- name: ol_warehouse_production_raw
  tables:
  - name: existing_table
    columns:
    - name: id
      data_type: varchar
"""
        result = _merge_sources_content(existing_with_desc, new_changing_type)
        # Description should be preserved
        assert "Primary key" in result
        # Data type should NOT be overwritten when description exists
        assert "bigint" in result

    def test_updates_data_type_when_no_description(self) -> None:
        """Column without a description should have its data_type updated from new content."""
        new_updated_type = """\
version: 2

sources:
- name: ol_warehouse_production_raw
  tables:
  - name: existing_table
    columns:
    - name: created_at
      data_type: timestamp with time zone
"""
        result = _merge_sources_content(self._EXISTING, new_updated_type)
        assert "timestamp with time zone" in result

    def test_invalid_existing_yaml_falls_back_to_new_content(self) -> None:
        result = _merge_sources_content("not: valid: yaml: [{", self._NEW_TABLE)
        assert "new_table" in result

    def test_invalid_new_yaml_returns_existing_content(self) -> None:
        result = _merge_sources_content(self._EXISTING, "not: valid: yaml: [{")
        assert "existing_table" in result

    def test_source_not_in_existing_is_appended(self) -> None:
        """New source with a different name is appended, not merged."""
        new_different_source = """\
version: 2

sources:
- name: a_different_source
  tables:
  - name: some_table
    columns: []
"""
        result = _merge_sources_content(self._EXISTING, new_different_source)
        assert "a_different_source" in result
        assert "ol_warehouse_raw_data" in result

    @pytest.mark.parametrize(
        ("prefix", "expected_domain"),
        [
            ("raw__mitlearn__app__postgres__users", "mitlearn"),
            ("raw__learn_ai__app__postgres__grant", "learn_ai"),
            ("raw__edxorg__tracking", "edxorg"),
        ],
    )
    def test_extract_domain_parametrized(self, prefix: str, expected_domain: str) -> None:
        assert _extract_domain(prefix) == expected_domain

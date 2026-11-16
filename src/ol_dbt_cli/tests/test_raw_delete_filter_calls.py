"""Tests for the raw_delete_filter validate check."""

from __future__ import annotations

from pathlib import Path

import pytest

from ol_dbt_cli.lib.raw_delete_filter_calls import (
    RAW_DELETE_FILTER_CHECK,
    check_raw_delete_filter,
    delete_filtered_tables,
)
from tests.source_checks import error_messages

REPO_ROOT = Path(__file__).resolve().parents[3]
FILTERED = "raw__xpro__app__postgres__ecommerce_order"
MAP = (
    "{% macro raw_delete_filter_map() %}\n"
    "    {% do return({\n"
    "        'raw__xpro__app__postgres__ecommerce_order': {'kind': 'keyset', "
    "'loader_time_column': '_airbyte_extracted_at', 'key': 'id', 'max_age_hours': 24},\n"
    "    }) %}\n"
    "{% endmacro %}\n"
)
SOURCE_READ = "select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_order') }}"
LIVE_READ = "{{ live_raw_rows('raw__xpro__app__postgres__ecommerce_order') }}"
OTHER_READ = "select * from {{ source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__courses_course') }}"


@pytest.fixture
def staging(dbt_dir: Path) -> Path:
    (dbt_dir / "macros" / "_raw_delete_filters.sql").write_text(MAP)
    path = dbt_dir / "models" / "staging"
    path.mkdir()
    return path


def _messages(dbt_dir: Path) -> list[str]:
    return error_messages(check_raw_delete_filter, RAW_DELETE_FILTER_CHECK, dbt_dir)


def test_reads_the_tables_from_the_generated_map(staging: Path) -> None:
    assert delete_filtered_tables(staging.parents[1]) == {FILTERED}


def test_reports_a_plain_source_read_of_a_filtered_table(staging: Path) -> None:
    (staging / "stg_order.sql").write_text(SOURCE_READ)
    assert _messages(staging.parents[1]) == [
        f"models/staging/stg_order.sql reads {FILTERED} through source() without live_raw_rows"
    ]


@pytest.mark.parametrize(
    "sql",
    [
        LIVE_READ,
        "{% set rel = source('ol_warehouse_raw_data', 'raw__xpro__app__postgres__ecommerce_order') %}\n"
        "{{ live_raw_rows('raw__xpro__app__postgres__ecommerce_order', rel) }}",
        OTHER_READ,
        "{# was: " + SOURCE_READ + " #}\n" + LIVE_READ,
    ],
)
def test_passes_a_filtered_read_and_any_read_of_another_table(staging: Path, sql: str) -> None:
    (staging / "stg.sql").write_text(sql)
    assert _messages(staging.parents[1]) == []


@pytest.mark.parametrize("comment", ["{# " + LIVE_READ + " #}", "-- " + LIVE_READ])
def test_a_live_raw_rows_call_in_a_comment_does_not_count(staging: Path, comment: str) -> None:
    (staging / "stg.sql").write_text(comment + "\n" + SOURCE_READ)
    assert len(_messages(staging.parents[1])) == 1


def test_an_unfiltered_read_beside_a_filtered_one_is_reported(staging: Path) -> None:
    (staging / "stg.sql").write_text(
        "with live as (" + LIVE_READ + ")\n, everything as (" + SOURCE_READ + ")\nselect * from live"
    )
    assert len(_messages(staging.parents[1])) == 1


def test_models_outside_staging_are_not_checked(staging: Path) -> None:
    external = staging.parent / "external"
    external.mkdir()
    (external / "export.sql").write_text(SOURCE_READ)
    assert _messages(staging.parents[1]) == []


def test_no_generated_map_means_nothing_to_check(dbt_dir: Path) -> None:
    (dbt_dir / "models" / "stg.sql").write_text(SOURCE_READ)
    assert _messages(dbt_dir) == []


def test_the_project_reads_every_filtered_table_through_live_raw_rows() -> None:
    dbt_dir = REPO_ROOT / "src" / "ol_dbt"
    assert delete_filtered_tables(dbt_dir)
    assert _messages(dbt_dir) == []

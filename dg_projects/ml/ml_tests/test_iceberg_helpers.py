"""Tests for ml.lib.iceberg_helpers, against a real SQLite-backed Iceberg catalog."""

import polars as pl
import pytest
from ml.lib.iceberg_helpers import upsert_chunk
from pyiceberg.catalog.sql import SqlCatalog

TABLE = "some_db.checkpoints"
OLD_SCHEMA = {"pk": pl.String, "model": pl.String, "value": pl.String}
SCHEMA = {**OLD_SCHEMA, "key": pl.String}


def _chunk(*rows: tuple[str, str, str]) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"pk": pk, "model": "m", "value": value, "key": key}
            for pk, value, key in rows
        ],
        schema=SCHEMA,
    )


def _stored(catalog: SqlCatalog) -> list[dict[str, object]]:
    return catalog.load_table(TABLE).scan().to_polars().sort("pk").to_dicts()


def _table_without_key(catalog: SqlCatalog) -> None:
    """Create a table from before the key column existed, holding pks a and b."""
    existing = pl.DataFrame(
        [{"pk": pk, "model": "m", "value": "old"} for pk in ("a", "b")],
        schema=OLD_SCHEMA,
    )
    catalog.create_table(TABLE, schema=existing.to_arrow().schema).append(
        existing.to_arrow()
    )


def test_upsert_chunk_creates_the_table_on_the_first_write(
    iceberg_catalog: SqlCatalog,
) -> None:
    upsert_chunk(iceberg_catalog, TABLE, _chunk(("a", "new", "k")), ["pk"])

    assert _stored(iceberg_catalog) == [
        {"pk": "a", "model": "m", "value": "new", "key": "k"}
    ]


def test_upsert_chunk_skips_an_empty_chunk_without_creating_the_table(
    iceberg_catalog: SqlCatalog,
) -> None:
    upsert_chunk(iceberg_catalog, TABLE, _chunk(), ["pk"])

    assert not iceberg_catalog.table_exists(TABLE)


def test_upsert_chunk_replaces_stored_rows_after_adding_a_column(
    iceberg_catalog: SqlCatalog,
) -> None:
    """Adding a column commits a schema with no snapshot. Table.upsert then read
    the stored rows at the old snapshot's schema and failed on every chunk that
    held a stored key.
    """
    _table_without_key(iceberg_catalog)

    upsert_chunk(iceberg_catalog, TABLE, _chunk(("a", "new", "k-a")), ["pk"])
    upsert_chunk(
        iceberg_catalog, TABLE, _chunk(("b", "new", "k-b"), ("c", "new", "k-c")), ["pk"]
    )

    assert _stored(iceberg_catalog) == [
        {"pk": "a", "model": "m", "value": "new", "key": "k-a"},
        {"pk": "b", "model": "m", "value": "new", "key": "k-b"},
        {"pk": "c", "model": "m", "value": "new", "key": "k-c"},
    ]


def test_upsert_chunk_returns_the_chunk_in_the_tables_column_order(
    iceberg_catalog: SqlCatalog,
) -> None:
    _table_without_key(iceberg_catalog)
    chunk = _chunk(("a", "new", "k")).select("key", "value", "model", "pk")

    written = upsert_chunk(iceberg_catalog, TABLE, chunk, ["pk"])

    assert written.columns == ["pk", "model", "value", "key"]
    assert iceberg_catalog.load_table(TABLE).schema().column_names == written.columns


def test_upsert_chunk_replaces_only_rows_matching_every_join_column(
    iceberg_catalog: SqlCatalog,
) -> None:
    upsert_chunk(iceberg_catalog, TABLE, _chunk(("a", "one", "k1")), ["pk", "model"])
    other_model = _chunk(("a", "two", "k2")).with_columns(pl.lit("n").alias("model"))

    upsert_chunk(iceberg_catalog, TABLE, other_model, ["pk", "model"])
    upsert_chunk(iceberg_catalog, TABLE, _chunk(("a", "three", "k3")), ["pk", "model"])

    stored = sorted(_stored(iceberg_catalog), key=lambda row: str(row["model"]))
    assert [(row["model"], row["value"]) for row in stored] == [
        ("m", "three"),
        ("n", "two"),
    ]


def test_upsert_chunk_refuses_duplicate_keys_before_writing(
    iceberg_catalog: SqlCatalog,
) -> None:
    _table_without_key(iceberg_catalog)
    with pytest.raises(ValueError, match="duplicate rows"):
        upsert_chunk(
            iceberg_catalog, TABLE, _chunk(("a", "one", "k"), ("a", "two", "k")), ["pk"]
        )
    assert [row["value"] for row in _stored(iceberg_catalog)] == ["old", "old"]

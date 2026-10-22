"""Materialize the Learn embedding assets against an in-memory warehouse and a fake
embedder.

Tables live in a dict keyed by table name. The IO manager replaces a partition's
rows, as the Iceberg IO manager's overwrite of a partition does, so a second
materialization reads what the first wrote. The embedder counts its calls and
returns a vector made from the text, so a test can tell which texts were sent.
"""

import uuid
from collections.abc import Sequence
from typing import Any

import polars as pl
import pytest
from dagster import (
    DagsterInstance,
    Definitions,
    IOManager,
    MultiPartitionKey,
    build_sensor_context,
    materialize,
)
from ml.assets.learn_embeddings import (
    CONTENT_FILE_CHUNKS_KEY,
    CONTENT_FILE_TEXT_KEY,
    CONTENT_FILE_VECTORS_KEY,
    TOPIC_CHUNKS_KEY,
    TOPIC_VECTORS_KEY,
    content_file_embedding_chunks,
    content_file_embedding_vectors,
    embedding_strategies,
    learn_content_file_text,
    topic_embedding_chunks,
    topic_embedding_vectors,
)
from ml.lib import warehouse
from ml.lib.learn_embeddings import STRATEGIES_PATH, TOPICS_RELATION, strategies
from ml.sensors.learn_embeddings import learn_content_file_text_sensor
from ol_llm import LLMUnavailable
from ol_orchestrate.lib.embeddings.documents import md5_hex
from ol_orchestrate.lib.embeddings.learn import LEARN_V1_CONTENT_FILES
from ol_orchestrate.lib.embeddings.models import EmbeddingBatch, EmbeddingModelSpec
from ol_orchestrate.lib.embeddings.pipeline import chunk_key
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy
from ol_orchestrate.resources.embeddings import EmbeddingsResource
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic_ai.embeddings.result import EmbedInputType
from pyiceberg.exceptions import NoSuchTableError

RUN = "course-v1:MITxT+18.01x+1T2026"
RESOURCE = "course-v1:MITxT+18.01x"
PARTITION = MultiPartitionKey({"etl_source": "mitxonline", "run": RUN})
VECTOR_LENGTH = 3072
TOPIC_UUID = "8b9d6f0a-3c1e-4e7a-9a52-1f0d2c3b4a59"


# ---------------------------------------------------------------- fake warehouse
class Warehouse(IOManager):
    """Replace a partition's rows, or the whole table when unpartitioned."""

    def __init__(self, tables: dict[str, pl.DataFrame]) -> None:
        self.tables = tables

    def handle_output(self, context, obj: pl.DataFrame) -> None:
        table = context.asset_key.path[-1]
        previous = self.tables.get(table)
        if context.has_partition_key and previous is not None:
            dimensions = context.partition_key.keys_by_dimension
            previous = previous.filter(
                ~(
                    (pl.col("etl_source") == dimensions["etl_source"])
                    & (pl.col("run_readable_id") == dimensions["run"])
                )
            )
            obj = pl.concat([previous, obj], how="diagonal")
        self.tables[table] = obj

    def load_input(self, context) -> None:
        raise NotImplementedError


@pytest.fixture
def tables(monkeypatch) -> dict[str, pl.DataFrame]:
    store: dict[str, pl.DataFrame] = {}

    def read(database_name: str, table_name: str) -> pl.LazyFrame:
        assert database_name.startswith("ol_warehouse_production_test_")
        if table_name not in store:
            raise NoSuchTableError(table_name)
        return store[table_name].lazy()

    monkeypatch.setattr(warehouse, "get_dbt_model_as_dataframe", read)
    return store


# ------------------------------------------------------------------ fake embedder
class FakeEmbedder:
    """Return ``[len(text), 1, 0, ...]`` and record each call's texts.

    A text in ``failing`` makes its request raise LLMUnavailable.
    """

    def __init__(
        self, spec: EmbeddingModelSpec, calls: list[list[str]], failing: set[str]
    ) -> None:
        self.spec = spec
        self.calls = calls
        self.failing = failing

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        assert input_type == "document"
        self.calls.append(list(texts))
        if self.failing & set(texts):
            msg = "provider down"
            raise LLMUnavailable(msg, use_case="test", model=self.spec.model)
        return EmbeddingBatch(
            vectors=[
                [float(len(text)), 1.0] + [0.0] * (VECTOR_LENGTH - 2) for text in texts
            ],
            input_tokens=len(texts),
        )


CALLS: list[list[str]] = []
FAILING: set[str] = set()


class FakeEmbeddings(EmbeddingsResource):
    def setup_for_execution(self, context) -> None:
        pass

    def embedder(self, strategy: EmbeddingStrategy) -> FakeEmbedder:  # type: ignore[override,unused-ignore]
        return FakeEmbedder(strategy.model, CALLS, FAILING)


@pytest.fixture(autouse=True)
def calls() -> list[list[str]]:
    CALLS.clear()
    FAILING.clear()
    return CALLS


def fake_embeddings() -> FakeEmbeddings:
    return FakeEmbeddings(
        vault=Vault(vault_addr="https://vault.example.com", vault_auth_type="token"),
        environment="test",
    )


# ------------------------------------------------------------------------ fixtures
def content_files(**contents: str | None) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "etl_source": "mitxonline",
                "run_readable_id": RUN,
                "key": key,
                "content": content,
                "checksum": md5_hex(content) if content else None,
                "file_type": None,
                "file_extension": ".html",
            }
            for key, content in contents.items()
        ]
    )


def runs() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {"readable_id": RESOURCE, "run_id": RUN},
            {"readable_id": "course-v1:MITxT+other", "run_id": "other-run"},
        ]
    )


def load_sources(tables: dict[str, pl.DataFrame], **contents: str | None) -> None:
    tables["integrations__learn__content_files"] = content_files(**contents)
    tables["integrations__learn__mitxonline_runs"] = runs()


def instance() -> DagsterInstance:
    created = DagsterInstance.ephemeral()
    created.add_dynamic_partitions("learn_content_file_run", [RUN])
    return created


def build_content_files(tables, dagster_instance: DagsterInstance | None = None):
    return materialize(
        [content_file_embedding_chunks, content_file_embedding_vectors],
        partition_key=PARTITION,
        instance=dagster_instance or instance(),
        resources={
            "learn_embeddings_io_manager": Warehouse(tables),
            "embeddings": fake_embeddings(),
        },
        raise_on_error=False,
    )


def learn_point_id(*parts: str) -> str:
    """Learn's content-file point id, written out from vector_point_key."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, ".".join(parts)))


# ---------------------------------------------------------------------- strategies
def test_learn_v1_content_files_is_the_library_strategy():
    assert strategies()["learn_v1_content_files"] == LEARN_V1_CONTENT_FILES


def test_learn_v1_topics_is_learns_topic_configuration():
    topics = strategies()["learn_v1_topics"]

    assert topics.doc_kind == "topic"
    assert topics.role == "serving"
    assert topics.model.model_id == "openai:text-embedding-3-large@3072"
    assert topics.context_template is None
    assert topics.qdrant is not None
    assert (topics.qdrant.dense_vector_name, topics.qdrant.sparse_vector_name) == (
        "text-embedding-3-large",
        "bm25",
    )


def test_strategy_file_ships_with_the_package():
    assert STRATEGIES_PATH.is_file()


# -------------------------------------------------------------------- chunk keys
def test_content_file_chunks_carry_learns_point_ids(tables):
    load_sources(tables, **{"block-v1:a": "Limits and derivatives.", "empty": None})

    result = build_content_files(tables)

    assert result.success
    chunks = tables[CONTENT_FILE_CHUNKS_KEY.path[-1]]
    assert chunks.height == 1
    row = chunks.row(0, named=True)
    doc_key = f"mitxonline.{RESOURCE}.{RUN}.block-v1:a"
    assert row["doc_key"] == doc_key
    assert row["point_id"] == learn_point_id(
        "mitxonline", RESOURCE, RUN, "block-v1:a", "0"
    )
    assert row["chunk_key"] == chunk_key(
        doc_key, LEARN_V1_CONTENT_FILES.chunker.chunker_id, 0
    )
    # Learn's chunk-0 payload checksum is the file's checksum, the MD5 of content.
    assert row["text_checksum"] == md5_hex("Limits and derivatives.")
    assert (row["etl_source"], row["run_readable_id"]) == ("mitxonline", RUN)


def test_long_files_keep_learns_chunk_numbers(tables):
    text = " ".join(f"word{number}" for number in range(2000))
    load_sources(tables, **{"block-v1:long": text})

    build_content_files(tables)

    chunks = tables[CONTENT_FILE_CHUNKS_KEY.path[-1]].sort("chunk_number")
    assert chunks.height > 1
    assert chunks["point_id"].to_list() == [
        learn_point_id("mitxonline", RESOURCE, RUN, "block-v1:long", str(number))
        for number in chunks["chunk_number"]
    ]


def test_files_without_a_resource_are_not_chunked(tables):
    load_sources(tables, **{"block-v1:a": "text"})
    tables["integrations__learn__mitxonline_runs"] = runs().filter(
        pl.col("run_id") != RUN
    )

    result = build_content_files(tables)

    assert result.success
    assert tables[CONTENT_FILE_CHUNKS_KEY.path[-1]].is_empty()
    assert CALLS == []


def test_topic_point_id_is_the_topic_uuid(tables):
    tables[TOPICS_RELATION] = pl.DataFrame(
        [
            {
                "learningresourcetopic_id": 1,
                "learningresourcetopic_name": "Calculus",
                "learningresourcetopic_parent_id": None,
                "learningresourcetopic_uuid": TOPIC_UUID.upper(),
            }
        ]
    )

    result = materialize(
        [topic_embedding_chunks, topic_embedding_vectors],
        resources={"io_manager": Warehouse(tables), "embeddings": fake_embeddings()},
    )

    assert result.success
    chunk = tables[TOPIC_CHUNKS_KEY.path[-1]].row(0, named=True)
    assert (chunk["point_id"], chunk["chunk_text"]) == (TOPIC_UUID, "Calculus")
    vectors = tables[TOPIC_VECTORS_KEY.path[-1]]
    assert vectors["strategy_id"].to_list() == ["learn_v1_topics"]
    assert CALLS == [["Calculus"]]


# ----------------------------------------------------------------------- vectors
def test_unchanged_inputs_embed_nothing(tables, calls):
    load_sources(tables, **{"block-v1:a": "First file.", "block-v1:b": "Second."})
    dagster_instance = instance()

    assert build_content_files(tables, dagster_instance).success
    first = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
    assert calls == [["First file.", "Second."]]
    assert first["strategy_id"].unique().to_list() == ["learn_v1_content_files"]
    assert first["input_checksum"].sort().to_list() == sorted(
        [md5_hex("First file."), md5_hex("Second.")]
    )
    assert first.schema["vector"] == pl.List(pl.Float32)

    calls.clear()
    assert build_content_files(tables, dagster_instance).success

    assert calls == []
    second = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
    assert second.sort("chunk_key").equals(first.sort("chunk_key"))


def test_a_changed_chunk_alone_is_embedded(tables, calls):
    load_sources(tables, **{"block-v1:a": "First file.", "block-v1:b": "Second."})
    dagster_instance = instance()
    build_content_files(tables, dagster_instance)
    first = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]

    calls.clear()
    load_sources(tables, **{"block-v1:a": "First file.", "block-v1:b": "Changed."})
    assert build_content_files(tables, dagster_instance).success

    assert calls == [["Changed."]]
    second = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
    assert second.height == 2
    assert md5_hex("Second.") not in second["input_checksum"].to_list()
    unchanged = second.filter(pl.col("input_checksum") == md5_hex("First file."))
    assert (
        unchanged["run_id"].to_list()
        == first.filter(pl.col("input_checksum") == md5_hex("First file."))[
            "run_id"
        ].to_list()
    )


def test_a_text_stored_under_another_run_is_reused(tables, calls):
    load_sources(tables, **{"block-v1:a": "Shared syllabus."})
    tables[CONTENT_FILE_VECTORS_KEY.path[-1]] = pl.DataFrame(
        {
            "chunk_key": ["elsewhere"],
            "strategy_id": ["learn_v1_content_files"],
            "model_id": ["openai:text-embedding-3-large@3072"],
            "input_checksum": [md5_hex("Shared syllabus.")],
            "vector": [[9.0] * VECTOR_LENGTH],
            "embedded_at": [None],
            "run_id": ["earlier"],
            "etl_source": ["mitxonline"],
            "run_readable_id": ["other-run"],
        },
        schema_overrides={
            "vector": pl.List(pl.Float32),
            "embedded_at": pl.Datetime("us", "UTC"),
        },
    )

    assert build_content_files(tables).success

    assert calls == []
    vectors = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
    assert vectors.height == 2
    assert vectors.filter(pl.col("run_readable_id") == RUN)["vector"][0][0] == 9.0


def test_failed_requests_store_the_rest_and_fail_the_run(tables, monkeypatch):
    load_sources(tables, **{"block-v1:a": "Fine.", "block-v1:b": "Unlucky."})
    FAILING.add("Unlucky.")
    strategy = strategies()["learn_v1_content_files"]
    # One request per text, so a failure costs only its own chunk.
    one_per_request = strategy.model.model_copy(update={"batch_size": 1})
    monkeypatch.setitem(
        strategies(),
        "learn_v1_content_files",
        strategy.model_copy(update={"model": one_per_request}),
    )

    result = build_content_files(tables)

    assert not result.success
    vectors = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
    assert vectors["input_checksum"].to_list() == [md5_hex("Fine.")]


def test_candidate_vectors_survive_a_serving_run(tables):
    load_sources(tables, **{"block-v1:a": "Text."})
    dagster_instance = instance()
    build_content_files(tables, dagster_instance)
    candidate = tables[CONTENT_FILE_VECTORS_KEY.path[-1]].with_columns(
        pl.lit("exp_candidate").alias("strategy_id")
    )
    tables[CONTENT_FILE_VECTORS_KEY.path[-1]] = pl.concat(
        [tables[CONTENT_FILE_VECTORS_KEY.path[-1]], candidate]
    )

    assert build_content_files(tables, dagster_instance).success

    assert sorted(tables[CONTENT_FILE_VECTORS_KEY.path[-1]]["strategy_id"]) == [
        "exp_candidate",
        "learn_v1_content_files",
    ]


# ------------------------------------------------------------------------ sensor
def test_sensor_registers_runs_then_observes_them(tables):
    load_sources(tables, **{"block-v1:a": "Text."})
    tables["integrations__learn__xpro_runs"] = pl.DataFrame(
        schema={"readable_id": pl.String, "run_id": pl.String}
    )
    tables["integrations__learn__ocw_content_files"] = content_files(none="").clear()
    tables["integrations__learn__ocw_courses"] = pl.DataFrame(
        schema={"readable_id": pl.String, "run_id": pl.String}
    )
    dagster_instance = DagsterInstance.ephemeral()

    first: Any = learn_content_file_text_sensor(
        build_sensor_context(instance=dagster_instance)
    )
    assert first.dynamic_partitions_requests[0].partition_keys == [RUN]
    assert first.asset_events == []

    dagster_instance.add_dynamic_partitions("learn_content_file_run", [RUN])
    second: Any = learn_content_file_text_sensor(
        build_sensor_context(instance=dagster_instance, cursor=first.cursor)
    )
    (observation,) = second.asset_events
    assert observation.asset_key == CONTENT_FILE_TEXT_KEY
    assert observation.partition == PARTITION


# ------------------------------------------------------------------- definitions
def test_learn_embedding_definitions_load():
    Definitions(
        assets=[
            learn_content_file_text,
            content_file_embedding_chunks,
            content_file_embedding_vectors,
            topic_embedding_chunks,
            topic_embedding_vectors,
            embedding_strategies,
        ],
        sensors=[learn_content_file_text_sensor],
        resources={
            "io_manager": Warehouse({}),
            "learn_embeddings_io_manager": Warehouse({}),
            "embeddings": fake_embeddings(),
        },
    ).resolve_all_job_defs()


def test_strategy_table_has_a_row_per_strategy(tables):
    result = materialize(
        [embedding_strategies], resources={"io_manager": Warehouse(tables)}
    )

    assert result.success
    assert sorted(tables["embedding_strategies"]["strategy_id"]) == [
        "learn_v1_content_files",
        "learn_v1_topics",
    ]

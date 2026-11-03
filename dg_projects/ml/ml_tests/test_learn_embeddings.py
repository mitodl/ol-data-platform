"""Materialize the Learn embedding assets against an in-memory warehouse and a fake
embedder.

Tables live in a dict keyed by table name. The IO manager replaces a partition's
rows, as the Iceberg IO manager's overwrite of a partition does, so a second
materialization reads what the first wrote. The embedder counts its calls and
returns a vector made from the text, so a test can tell which texts were sent.
"""

import uuid
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

import polars as pl
import pytest
from dagster import (
    AssetCheckSeverity,
    AssetSelection,
    DagsterInstance,
    Definitions,
    IOManager,
    MultiPartitionKey,
    build_sensor_context,
    materialize,
)
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from ml.assets.learn_embedding_checks import (
    content_file_vectors_match_learn,
    learn_parity_definitions,
    topic_vectors_match_learn,
)
from ml.assets.learn_embeddings import (
    CONTENT_FILE_CHUNKS_KEY,
    CONTENT_FILE_TEXT_KEY,
    CONTENT_FILE_VECTORS_KEY,
    EMPTY_TEXT_VERSION,
    TOPIC_CHUNKS_KEY,
    TOPIC_VECTORS_KEY,
    content_file_embedding_chunks,
    content_file_embedding_vectors,
    content_file_text_versions,
    embedding_strategies,
    learn_content_file_text,
    topic_embedding_chunks,
    topic_embedding_vectors,
)
from ml.lib import warehouse
from ml.lib.learn_embeddings import (
    STRATEGIES_PATH,
    TOPICS_RELATION,
    known_vectors,
    strategies,
)
from ml.lib.learn_parity import LearnPoint, compare_with_learn
from ml.sensors import learn_embeddings as sensors
from ml.sensors.learn_embeddings import (
    SAMPLE_RUNS_PER_SOURCE,
    in_sample,
    learn_content_file_text_sensor,
)
from ol_llm import LLMRejected, LLMUnavailable
from ol_orchestrate.lib.embeddings.documents import md5_hex
from ol_orchestrate.lib.embeddings.learn import LEARN_V1_CONTENT_FILES
from ol_orchestrate.lib.embeddings.models import EmbeddingBatch, EmbeddingModelSpec
from ol_orchestrate.lib.embeddings.pipeline import chunk_key
from ol_orchestrate.lib.embeddings.strategy import EmbeddingStrategy
from ol_orchestrate.resources.embeddings import EmbeddingsResource, QdrantResource
from ol_orchestrate.resources.secrets.vault import Vault
from pydantic_ai.embeddings.result import EmbedInputType
from pyiceberg.exceptions import NoSuchTableError
from qdrant_client import QdrantClient, models

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

    def read_matching(database_name, table_name, columns, equals) -> pl.DataFrame:
        rows = read(database_name, table_name).filter(
            *(pl.col(column) == value for column, value in equals.items())
        )
        return rows.select(columns).collect()

    monkeypatch.setattr(warehouse, "get_dbt_model_as_dataframe", read)
    monkeypatch.setattr(warehouse, "load_matching_rows", read_matching)
    return store


# ------------------------------------------------------------------ fake embedder
class FakeEmbedder:
    """Return ``[len(text), 1, 0, ...]`` and record each call's texts.

    A text in ``failing`` makes its request raise LLMUnavailable, and one in
    ``rejected`` makes the provider reject it with a 400.
    """

    def __init__(
        self,
        spec: EmbeddingModelSpec,
        calls: list[list[str]],
        failing: set[str],
        rejected: set[str],
    ) -> None:
        self.spec = spec
        self.calls = calls
        self.failing = failing
        self.rejected = rejected

    def embed(
        self, texts: Sequence[str], *, input_type: EmbedInputType
    ) -> EmbeddingBatch:
        assert input_type == "document"
        self.calls.append(list(texts))
        if self.failing & set(texts):
            msg = "provider down"
            raise LLMUnavailable(msg, use_case="test", model=self.spec.model)
        if self.rejected & set(texts):
            msg = "invalid input"
            raise LLMRejected(
                msg, use_case="test", model=self.spec.model, status_code=400
            )
        return EmbeddingBatch(
            vectors=[
                [float(len(text)), 1.0] + [0.0] * (VECTOR_LENGTH - 2) for text in texts
            ],
            input_tokens=len(texts),
        )


CALLS: list[list[str]] = []
FAILING: set[str] = set()
REJECTED: set[str] = set()


class FakeEmbeddings(EmbeddingsResource):
    def setup_for_execution(self, context) -> None:
        pass

    def embedder(self, strategy: EmbeddingStrategy) -> FakeEmbedder:  # type: ignore[override,unused-ignore]
        return FakeEmbedder(strategy.model, CALLS, FAILING, REJECTED)


@pytest.fixture(autouse=True)
def calls() -> list[list[str]]:
    CALLS.clear()
    FAILING.clear()
    REJECTED.clear()
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
    content_files = strategies()["learn_v1_content_files"]

    # Learn calls OpenAI; the platform calls Azure OpenAI's deployment of the
    # same model, in the same vector space.
    assert content_files.model.model == "azure:text-embedding-3-large"
    assert LEARN_V1_CONTENT_FILES.model.model == "openai:text-embedding-3-large"
    assert content_files.model.vector_space_id == "text-embedding-3-large@3072"
    assert (
        content_files.model.vector_space_id
        == LEARN_V1_CONTENT_FILES.model.vector_space_id
    )
    assert (
        content_files.model_copy(
            update={
                "model": content_files.model.model_copy(
                    update={"model": LEARN_V1_CONTENT_FILES.model.model}
                )
            }
        )
        == LEARN_V1_CONTENT_FILES
    )


def test_learn_v1_topics_is_learns_topic_configuration():
    topics = strategies()["learn_v1_topics"]

    assert topics.doc_kind == "topic"
    assert topics.role == "serving"
    assert topics.model.model_id == "azure:text-embedding-3-large@3072"
    assert topics.model.vector_space_id == "text-embedding-3-large@3072"
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


def test_ocw_chunks_take_the_course_as_resource(tables):
    run = "3a6b5c0f1e2d4c5b8a7f6e5d4c3b2a10"  # pragma: allowlist secret
    course = "18.01+fall_2006"
    key = "courses/18-01-single-variable-calculus-fall-2006/resources/notes/"
    tables["integrations__learn__ocw_content_files"] = content_files(
        **{key: "Lecture notes."}
    ).with_columns(
        pl.lit("ocw").alias("etl_source"), pl.lit(run).alias("run_readable_id")
    )
    tables["integrations__learn__ocw_courses"] = pl.DataFrame(
        [{"readable_id": course, "run_id": run}]
    )
    dagster_instance = DagsterInstance.ephemeral()
    dagster_instance.add_dynamic_partitions("learn_content_file_run", [run])

    result = materialize(
        [content_file_embedding_chunks],
        partition_key=MultiPartitionKey({"etl_source": "ocw", "run": run}),
        instance=dagster_instance,
        resources={"learn_embeddings_io_manager": Warehouse(tables)},
    )

    assert result.success
    row = tables[CONTENT_FILE_CHUNKS_KEY.path[-1]].row(0, named=True)
    assert row["point_id"] == learn_point_id("ocw", course, run, key, "0")
    assert row["source_relation"] == "integrations__learn__ocw_content_files"


XPRO_RUN = "course-v1:xPRO+EMX1+R1"
XPRO_COURSE = "course-v1:xPRO+EMX1"
XPRO_PARTITION = MultiPartitionKey({"etl_source": "xpro", "run": XPRO_RUN})


def load_xpro(tables: dict[str, pl.DataFrame], platform_code: str | None) -> None:
    tables["integrations__learn__content_files"] = content_files(
        **{"block-v1:a": "Program management."}
    ).with_columns(
        pl.lit("xpro").alias("etl_source"), pl.lit(XPRO_RUN).alias("run_readable_id")
    )
    tables["integrations__learn__xpro_runs"] = pl.DataFrame(
        [
            {
                "readable_id": XPRO_COURSE,
                "run_id": XPRO_RUN,
                "platform_code": platform_code,
            }
        ],
        schema={
            "readable_id": pl.String,
            "run_id": pl.String,
            "platform_code": pl.String,
        },
    )


def build_xpro_chunks(tables) -> Any:
    dagster_instance = DagsterInstance.ephemeral()
    dagster_instance.add_dynamic_partitions("learn_content_file_run", [XPRO_RUN])
    return materialize(
        [content_file_embedding_chunks],
        partition_key=XPRO_PARTITION,
        instance=dagster_instance,
        resources={"learn_embeddings_io_manager": Warehouse(tables)},
    )


def test_xpro_chunks_take_the_runs_platform_code(tables):
    load_xpro(tables, "emeritus")

    assert build_xpro_chunks(tables).success

    row = tables[CONTENT_FILE_CHUNKS_KEY.path[-1]].row(0, named=True)
    assert row["point_id"] == learn_point_id(
        "emeritus", XPRO_COURSE, XPRO_RUN, "block-v1:a", "0"
    )


def test_an_xpro_run_without_a_platform_code_is_not_chunked(tables):
    load_xpro(tables, None)

    assert build_xpro_chunks(tables).success

    assert tables[CONTENT_FILE_CHUNKS_KEY.path[-1]].is_empty()
    # Not offered to the sensor, and emptied once registered.
    assert content_file_text_versions("xpro").is_empty()
    assert content_file_text_versions("xpro", registered=[XPRO_RUN])[
        "text_version"
    ].to_list() == [EMPTY_TEXT_VERSION]


def test_a_platform_change_moves_the_text_version(tables):
    load_xpro(tables, "xpro")
    before = content_file_text_versions("xpro")["text_version"].to_list()
    load_xpro(tables, "simplilearn")

    after = content_file_text_versions("xpro")["text_version"].to_list()

    assert len(before) == len(after) == 1
    assert before != after


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

    def build():
        return materialize(
            [topic_embedding_chunks, topic_embedding_vectors],
            resources={
                "io_manager": Warehouse(tables),
                "embeddings": fake_embeddings(),
            },
        )

    assert build().success
    chunk = tables[TOPIC_CHUNKS_KEY.path[-1]].row(0, named=True)
    assert (chunk["point_id"], chunk["chunk_text"]) == (TOPIC_UUID, "Calculus")
    vectors = tables[TOPIC_VECTORS_KEY.path[-1]]
    assert vectors["strategy_id"].to_list() == ["learn_v1_topics"]
    assert CALLS == [["Calculus"]]

    CALLS.clear()
    assert build().success
    assert CALLS == []
    assert tables[TOPIC_VECTORS_KEY.path[-1]].equals(vectors)


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


def stored_vector(text: str, model_id: str, vector_space: str) -> pl.DataFrame:
    """Return a vector row of another run, as the vector table stores it."""
    return pl.DataFrame(
        {
            "chunk_key": ["elsewhere"],
            "strategy_id": ["learn_v1_content_files"],
            "model_id": [model_id],
            "vector_space": [vector_space],
            "input_checksum": [md5_hex(text)],
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


def test_a_text_stored_under_another_run_is_reused(tables, calls):
    load_sources(tables, **{"block-v1:a": "Shared syllabus."})
    strategy = strategies()["learn_v1_content_files"]
    tables[CONTENT_FILE_VECTORS_KEY.path[-1]] = stored_vector(
        "Shared syllabus.", strategy.model.model_id, strategy.model.vector_space_id
    )

    assert build_content_files(tables).success

    assert calls == []
    vectors = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
    assert vectors.height == 2
    assert vectors.filter(pl.col("run_readable_id") == RUN)["vector"][0][0] == 9.0


def test_another_models_vector_is_reused_only_in_a_declared_space(tables, calls):
    load_sources(tables, **{"block-v1:a": "Shared syllabus."})
    # Learn's own model, as a vector imported from its collection would carry.
    learn_model = LEARN_V1_CONTENT_FILES.model.model_id
    tables[CONTENT_FILE_VECTORS_KEY.path[-1]] = stored_vector(
        "Shared syllabus.", learn_model, "text-embedding-3-large@3072"
    )

    assert build_content_files(tables).success
    assert calls == []
    row = tables[CONTENT_FILE_VECTORS_KEY.path[-1]].filter(
        pl.col("run_readable_id") == RUN
    )
    assert row["vector"][0][0] == 9.0
    # The row records the model that produced the vector, not the one it calls.
    assert strategies()["learn_v1_content_files"].model.model.startswith("azure:")
    assert row["model_id"][0] == "openai:text-embedding-3-large@3072"

    tables[CONTENT_FILE_VECTORS_KEY.path[-1]] = stored_vector(
        "Shared syllabus.", learn_model, learn_model
    )
    assert build_content_files(tables).success
    assert calls == [["Shared syllabus."]]


def test_a_rerun_keeps_its_rows_when_another_model_stored_the_text(tables, calls):
    load_sources(tables, **{"block-v1:a": "Shared syllabus."})
    dagster_instance = instance()
    assert build_content_files(tables, dagster_instance).success
    first = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
    # An earlier row of the same text and space from Learn's model, which the
    # space-wide lookup prefers.
    learn_row = stored_vector(
        "Shared syllabus.",
        LEARN_V1_CONTENT_FILES.model.model_id,
        "text-embedding-3-large@3072",
    ).with_columns(pl.lit(datetime(2026, 1, 1, tzinfo=UTC)).alias("embedded_at"))
    tables[CONTENT_FILE_VECTORS_KEY.path[-1]] = pl.concat([first, learn_row])
    calls.clear()

    for _ in range(2):
        assert build_content_files(tables, dagster_instance).success
        rows = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
        assert rows.filter(pl.col("run_readable_id") == RUN).equals(first)
    assert calls == []


def test_the_space_wide_lookup_takes_the_earliest_row():
    def row(model_id: str, value: float, day: int) -> pl.DataFrame:
        return stored_vector(
            "Text.", model_id, "text-embedding-3-large@3072"
        ).with_columns(
            pl.lit([value] * VECTOR_LENGTH, dtype=pl.List(pl.Float32)).alias("vector"),
            pl.lit(datetime(2026, 1, day, tzinfo=UTC)).alias("embedded_at"),
        )

    rows = pl.concat(
        [
            row("azure:text-embedding-3-large@3072", 2.0, 2),
            row("openai:text-embedding-3-large@3072", 1.0, 1),
        ]
    )

    for frame in (rows, rows.reverse()):
        (found,) = known_vectors(
            frame.lazy(), "text-embedding-3-large@3072", [md5_hex("Text.")]
        ).values()
        assert (found.model_id, found.vector[0]) == (
            "openai:text-embedding-3-large@3072",
            1.0,
        )


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


def test_a_rejected_request_is_split_so_only_the_bad_text_fails(tables, calls):
    load_sources(tables, **{"block-v1:a": "Fine.", "block-v1:b": "Malformed."})
    REJECTED.add("Malformed.")

    result = build_content_files(tables)

    assert not result.success
    assert calls == [["Fine.", "Malformed."], ["Fine."], ["Malformed."]]
    vectors = tables[CONTENT_FILE_VECTORS_KEY.path[-1]]
    assert vectors["input_checksum"].to_list() == [md5_hex("Fine.")]


def test_an_unavailable_provider_is_not_split(tables, calls):
    load_sources(tables, **{"block-v1:a": "Fine.", "block-v1:b": "Unlucky."})
    FAILING.add("Unlucky.")

    result = build_content_files(tables)

    assert not result.success
    assert calls == [["Fine.", "Unlucky."]]
    assert tables[CONTENT_FILE_VECTORS_KEY.path[-1]].is_empty()


def test_a_later_strategy_of_the_same_model_reuses_this_runs_vectors(
    tables, calls, monkeypatch
):
    load_sources(tables, **{"block-v1:a": "Text."})
    strategy = strategies()["learn_v1_content_files"]
    monkeypatch.setitem(
        strategies(),
        "exp_same_model",
        strategy.model_copy(update={"strategy_id": "exp_same_model"}),
    )

    assert build_content_files(tables).success

    assert calls == [["Text."]]
    assert sorted(tables[CONTENT_FILE_VECTORS_KEY.path[-1]]["strategy_id"]) == [
        "exp_same_model",
        "learn_v1_content_files",
    ]


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
def load_empty_xpro_and_ocw(tables: dict[str, pl.DataFrame]) -> None:
    tables["integrations__learn__xpro_runs"] = pl.DataFrame(
        schema={
            "readable_id": pl.String,
            "run_id": pl.String,
            "platform_code": pl.String,
        }
    )
    tables["integrations__learn__ocw_content_files"] = content_files(none="").clear()
    tables["integrations__learn__ocw_courses"] = pl.DataFrame(
        schema={"readable_id": pl.String, "run_id": pl.String}
    )


def test_sensor_registers_runs_then_observes_them(tables):
    load_sources(tables, **{"block-v1:a": "Text."})
    load_empty_xpro_and_ocw(tables)
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


def load_mitxonline_runs(tables: dict[str, pl.DataFrame], count: int) -> list[str]:
    """Load one file for each of ``count`` mitxonline runs; return them by hash."""
    runs_by_hash = sorted(
        (f"course-v1:MITxT+{number}x+1T2026" for number in range(count)), key=md5_hex
    )
    tables["integrations__learn__content_files"] = pl.concat(
        [
            content_files(**{"block-v1:a": "Text."}).with_columns(
                pl.lit(run).alias("run_readable_id")
            )
            for run in runs_by_hash
        ]
    )
    tables["integrations__learn__mitxonline_runs"] = pl.DataFrame(
        [{"readable_id": f"resource-{run}", "run_id": run} for run in runs_by_hash]
    )
    load_empty_xpro_and_ocw(tables)
    return runs_by_hash


def test_the_sensor_registers_only_the_sample(tables):
    runs_by_hash = load_mitxonline_runs(tables, 8)

    result: Any = learn_content_file_text_sensor(
        build_sensor_context(instance=DagsterInstance.ephemeral())
    )

    assert result.dynamic_partitions_requests[0].partition_keys == sorted(
        runs_by_hash[:SAMPLE_RUNS_PER_SOURCE]
    )


def test_widening_the_sample_registers_the_rest_without_a_change(tables, monkeypatch):
    runs_by_hash = load_mitxonline_runs(tables, 8)
    dagster_instance = DagsterInstance.ephemeral()

    def tick(cursor: str | None) -> Any:
        return learn_content_file_text_sensor(
            build_sensor_context(instance=dagster_instance, cursor=cursor)
        )

    first: Any = tick(None)
    sampled = runs_by_hash[:SAMPLE_RUNS_PER_SOURCE]
    assert first.dynamic_partitions_requests[0].partition_keys == sorted(sampled)
    dagster_instance.add_dynamic_partitions("learn_content_file_run", sampled)
    second: Any = tick(first.cursor)
    assert len(second.asset_events) == SAMPLE_RUNS_PER_SOURCE
    unchanged: Any = tick(second.cursor)
    assert unchanged.asset_events == []
    assert unchanged.dynamic_partitions_requests == []

    monkeypatch.setattr(sensors, "SAMPLE_RUNS_PER_SOURCE", len(runs_by_hash))
    widened: Any = tick(second.cursor)
    rest = runs_by_hash[SAMPLE_RUNS_PER_SOURCE:]
    assert widened.dynamic_partitions_requests[0].partition_keys == sorted(rest)
    dagster_instance.add_dynamic_partitions("learn_content_file_run", rest)
    observed: Any = tick(widened.cursor)
    assert {
        event.partition.keys_by_dimension["run"] for event in observed.asset_events
    } == set(runs_by_hash)


def test_the_sample_takes_the_first_runs_by_hash_and_the_named_ones():
    named = "course-v1:MITxT+7.05x+2T2026"
    others = [f"course-v1:MITxT+{number}x+1T2026" for number in range(8)]
    # Ahead of every other run by hash, but in orgs whose courses Learn does not
    # list.
    unlisted = [
        run
        for run in (
            f"course-v1:{org}{number}+UAI.1+1T2026"
            for number in range(300)
            for org in ("UAI_", "HZN_", "UCE")
        )
        if md5_hex(run) < min(map(md5_hex, others))
    ][:6]
    versions = pl.DataFrame(
        {
            "etl_source": ["mitxonline"] * (9 + len(unlisted)) + ["xpro"],
            "run_readable_id": [*others, named, *unlisted, "course-v1:xPRO+one+R1"],
            "text_version": ["v"] * (10 + len(unlisted)),
        }
    )

    sampled = in_sample(versions)

    assert len({run.split("+")[0].rstrip("0123456789") for run in unlisted}) > 1
    first = sorted([*others, named], key=md5_hex)[:SAMPLE_RUNS_PER_SOURCE]
    assert set(sampled["run_readable_id"]) == {
        *first,
        named,
        "course-v1:xPRO+one+R1",
    }


def test_a_run_that_loses_its_last_file_is_emptied(tables):
    load_sources(tables, **{"block-v1:a": "Text."})
    load_empty_xpro_and_ocw(tables)
    dagster_instance = instance()
    assert build_content_files(tables, dagster_instance).success
    assert not tables[CONTENT_FILE_VECTORS_KEY.path[-1]].is_empty()

    load_sources(tables, **{"block-v1:a": "Text."})
    tables["integrations__learn__content_files"] = content_files(none="").clear()
    result: Any = learn_content_file_text_sensor(
        build_sensor_context(instance=dagster_instance)
    )

    # other-run is in the runs model too, but was never registered.
    (observation,) = result.asset_events
    assert observation.partition == PARTITION
    assert observation.tags[DATA_VERSION_TAG] == EMPTY_TEXT_VERSION
    assert content_file_text_versions("mitxonline", RUN, registered=[RUN])[
        "text_version"
    ].to_list() == [EMPTY_TEXT_VERSION]

    assert build_content_files(tables, dagster_instance).success
    assert tables[CONTENT_FILE_CHUNKS_KEY.path[-1]].is_empty()
    assert tables[CONTENT_FILE_VECTORS_KEY.path[-1]].is_empty()


# ------------------------------------------------------------------- smokecheck
LEARN_BASE = "mitlearn-test"
AZURE_MODEL = "azure:text-embedding-3-large@3072"
DENSE = "text-embedding-3-large"
LEARN_CLIENTS: list[QdrantClient] = []


class FakeQdrant(QdrantResource):
    def get_client(self) -> QdrantClient:
        return LEARN_CLIENTS[0]


@pytest.fixture
def learn_qdrant() -> QdrantClient:
    client = QdrantClient(":memory:")
    for collection in ("content_files", "topics"):
        client.create_collection(
            f"{LEARN_BASE}.{collection}",
            vectors_config={
                DENSE: models.VectorParams(
                    size=VECTOR_LENGTH, distance=models.Distance.COSINE
                )
            },
        )
    LEARN_CLIENTS[:] = [client]
    return client


def fake_qdrant() -> FakeQdrant:
    return FakeQdrant(
        vault=Vault(vault_addr="https://vault.example.com", vault_auth_type="token"),
        vault_path="pipelines/qdrant",
        base_collection_name=LEARN_BASE,
    )


def fake_vector(text: str, *, skew: float = 0.0) -> list[float]:
    """Return FakeEmbedder's vector for a text, turned away from it by ``skew``."""
    return [float(len(text)), 1.0, skew] + [0.0] * (VECTOR_LENGTH - 3)


def learn_chunk(
    key: str, text: str, *, skew: float = 0.0, **payload: Any
) -> models.PointStruct:
    return models.PointStruct(
        id=learn_point_id("mitxonline", RESOURCE, RUN, key, "0"),
        vector={DENSE: fake_vector(text, skew=skew)},
        payload={
            "chunk_content": text,
            "chunk_number": 0,
            "checksum": md5_hex(text),
            "run_readable_id": RUN,
            "resource_readable_id": RESOURCE,
            "platform": {"code": "mitxonline"},
            "key": key,
            "file_type": None,
        }
        | payload,
    )


def check_content_files(
    tables,
    selection: AssetSelection | None = None,
    partition: MultiPartitionKey = PARTITION,
) -> Any:
    dagster_instance = DagsterInstance.ephemeral()
    dagster_instance.add_dynamic_partitions(
        "learn_content_file_run", [partition.keys_by_dimension["run"]]
    )
    result = materialize(
        [
            content_file_embedding_chunks,
            content_file_embedding_vectors,
            content_file_vectors_match_learn,
        ],
        partition_key=partition,
        instance=dagster_instance,
        resources={
            "learn_embeddings_io_manager": Warehouse(tables),
            "embeddings": fake_embeddings(),
            "learn_qdrant": fake_qdrant(),
        },
        selection=selection,
    )
    (evaluation,) = result.get_asset_check_evaluations()
    return evaluation


def test_vectors_that_match_learns_pass(tables, learn_qdrant):
    load_sources(tables, **{"block-v1:a": "First file.", "block-v1:b": "Second."})
    learn_qdrant.upsert(
        f"{LEARN_BASE}.content_files",
        [
            learn_chunk("block-v1:a", "First file."),
            learn_chunk("block-v1:b", "Second."),
            # Learn's metadata chunk for the course is not the platform's to build.
            models.PointStruct(
                id=str(uuid.uuid4()),
                vector={DENSE: fake_vector("metadata")},
                payload={
                    "run_readable_id": RUN,
                    "platform": {"code": "mitxonline"},
                    "file_type": "course_metadata",
                },
            ),
        ],
    )

    evaluation = check_content_files(tables)

    assert evaluation.passed
    metadata = {key: value.value for key, value in evaluation.metadata.items()}
    assert metadata["platform_chunks"] == 2
    assert metadata["learn_points"] == 2
    assert metadata["matched_ids"] == 2
    assert metadata["text_matches"] == 2
    assert metadata["checksum_matches"] == 2
    assert metadata["compared_vectors"] == 2
    assert metadata[f"{AZURE_MODEL}/cosine_min"] == pytest.approx(1.0)


def xpro_learn_chunk(key: str, text: str) -> models.PointStruct:
    return models.PointStruct(
        id=learn_point_id("emeritus", XPRO_COURSE, XPRO_RUN, key, "0"),
        vector={DENSE: fake_vector(text)},
        payload={
            "chunk_content": text,
            "chunk_number": 0,
            "checksum": md5_hex(text),
            "run_readable_id": XPRO_RUN,
            "resource_readable_id": XPRO_COURSE,
            "platform": {"code": "emeritus"},
            "key": key,
            "file_type": None,
        },
    )


def test_an_xpro_run_is_compared_under_its_platform_code(tables, learn_qdrant):
    load_xpro(tables, "emeritus")
    learn_qdrant.upsert(
        f"{LEARN_BASE}.content_files",
        [
            xpro_learn_chunk("block-v1:a", "Program management."),
            # Found only by listing the run's points under its platform code.
            xpro_learn_chunk("block-v1:gone", "A file the platform does not have."),
        ],
    )

    evaluation = check_content_files(tables, partition=XPRO_PARTITION)

    metadata = {key: value.value for key, value in evaluation.metadata.items()}
    assert metadata["matched_ids"] == 1
    assert metadata["compared_vectors"] == 1
    assert metadata["missing_on_platform"] == 1


def test_a_vector_unlike_learns_for_the_same_text_is_an_error(tables, learn_qdrant):
    load_sources(tables, **{"block-v1:a": "First file.", "block-v1:b": "Second."})
    learn_qdrant.upsert(
        f"{LEARN_BASE}.content_files",
        [
            learn_chunk("block-v1:a", "First file."),
            learn_chunk("block-v1:b", "Second.", skew=5.0),
        ],
    )

    evaluation = check_content_files(tables)

    assert not evaluation.passed
    assert evaluation.severity == AssetCheckSeverity.ERROR
    assert evaluation.metadata["below_min_cosine"].value == 1


def test_a_chunk_without_a_vector_warns(tables, learn_qdrant):
    load_sources(tables, **{"block-v1:a": "First file.", "block-v1:b": "Second."})
    learn_qdrant.upsert(
        f"{LEARN_BASE}.content_files",
        [
            learn_chunk("block-v1:a", "First file."),
            learn_chunk("block-v1:b", "Second."),
        ],
    )
    REJECTED.add("Second.")
    # The check is skipped when the vector step fails, so it sees a missing
    # vector when it is run without the vectors afterwards.
    assert not build_content_files(tables).success

    evaluation = check_content_files(
        tables,
        AssetSelection.assets(CONTENT_FILE_CHUNKS_KEY)
        | AssetSelection.checks(content_file_vectors_match_learn),
    )

    assert not evaluation.passed
    assert evaluation.severity == AssetCheckSeverity.WARN
    assert evaluation.metadata["platform_chunks_without_vector"].value == 1


def test_vectors_imported_from_learn_are_reported_apart(tables, learn_qdrant):
    load_sources(tables, **{"block-v1:a": "First file.", "block-v1:b": "Second."})
    # Learn's own vector of "Second.", imported before Learn re-embedded it.
    tables[CONTENT_FILE_VECTORS_KEY.path[-1]] = stored_vector(
        "Second.",
        LEARN_V1_CONTENT_FILES.model.model_id,
        "text-embedding-3-large@3072",
    ).with_columns(
        pl.lit(fake_vector("Second.", skew=5.0), dtype=pl.List(pl.Float32)).alias(
            "vector"
        )
    )
    learn_qdrant.upsert(
        f"{LEARN_BASE}.content_files",
        [
            learn_chunk("block-v1:a", "First file."),
            learn_chunk("block-v1:b", "Second."),
        ],
    )

    evaluation = check_content_files(tables)

    assert evaluation.passed
    metadata = {key: value.value for key, value in evaluation.metadata.items()}
    learn_model = LEARN_V1_CONTENT_FILES.model.model_id
    assert metadata[f"{learn_model}/below_min_cosine"] == 1
    assert metadata[f"{AZURE_MODEL}/cosine_min"] == pytest.approx(1.0)
    assert metadata["below_min_cosine"] == 0


def test_differing_points_and_texts_only_warn(tables, learn_qdrant):
    load_sources(tables, **{"block-v1:a": "Platform text.", "block-v1:b": "Second."})
    learn_qdrant.upsert(
        f"{LEARN_BASE}.content_files",
        [
            # Tika's text for the block, not the platform's, with its own vector.
            learn_chunk("block-v1:a", "Tika's text.", skew=5.0),
            learn_chunk("block-v1:gone", "A file the platform does not have."),
            # Another platform's run of the same id is not this run's.
            learn_chunk(
                "block-v1:elsewhere", "xPRO's file.", platform={"code": "xpro"}
            ).model_copy(update={"id": str(uuid.uuid4())}),
        ],
    )

    evaluation = check_content_files(tables)

    assert not evaluation.passed
    assert evaluation.severity == AssetCheckSeverity.WARN
    metadata = {key: value.value for key, value in evaluation.metadata.items()}
    assert (
        metadata["matched_ids"],
        metadata["missing_in_learn"],
        metadata["missing_on_platform"],
        metadata["text_matches"],
        metadata["checksum_matches"],
        metadata["compared_vectors"],
    ) == (1, 1, 1, 0, 0, 0)


def test_a_checksum_unlike_learns_warns(tables, learn_qdrant):
    load_sources(tables, **{"block-v1:a": "First file.", "block-v1:b": "Second."})
    learn_qdrant.upsert(
        f"{LEARN_BASE}.content_files",
        [
            learn_chunk("block-v1:a", "First file."),
            # The same chunk and vector, from a version of the file Learn read
            # before or after the platform's.
            learn_chunk("block-v1:b", "Second.", checksum=md5_hex("Second, edited.")),
        ],
    )

    evaluation = check_content_files(tables)

    assert not evaluation.passed
    assert evaluation.severity == AssetCheckSeverity.WARN
    metadata = {key: value.value for key, value in evaluation.metadata.items()}
    assert (
        metadata["text_matches"],
        metadata["checksum_matches"],
        metadata["below_min_cosine"],
    ) == (2, 1, 0)


def test_learn_points_are_read_a_batch_at_a_time():
    platform = pl.DataFrame(
        {
            "point_id": ["a", "b", "c"],
            "chunk_text": ["A.", "B.", "C."],
            "text_checksum": [md5_hex("A."), md5_hex("B."), md5_hex("C.")],
            "vector": [[1.0, 0.0], [0.0, 1.0], None],
            "model_id": [AZURE_MODEL, AZURE_MODEL, None],
        }
    )
    requests: list[list[str]] = []

    def fetch(ids):
        requests.append(list(ids))
        return {
            point_id: LearnPoint(
                text=f"{point_id.upper()}.", checksum=None, vector=[1.0, 0.0]
            )
            for point_id in ids
        }

    report = compare_with_learn(
        platform,
        fetch,
        ["a", "b", "c"],
        compare_checksums=False,
        reference_model_id=LEARN_V1_CONTENT_FILES.model.model_id,
        platform_vector_size=2,
        batch_size=2,
    )

    assert requests == [["a", "b"], ["c"]]
    stats = report.cosine_by_model[AZURE_MODEL]
    assert (stats.compared, stats.minimum, stats.below_threshold) == (2, 0.0, 1)
    assert (report.platform_chunks_without_vector, report.vectors_differ) == (1, True)


def test_a_learn_collection_of_another_size_is_an_error(tables, learn_qdrant):
    load_sources(tables, **{"block-v1:a": "First file."})
    collection = f"{LEARN_BASE}.content_files"
    learn_size = 8
    learn_qdrant.delete_collection(collection)
    learn_qdrant.create_collection(
        collection,
        vectors_config={
            DENSE: models.VectorParams(size=learn_size, distance=models.Distance.COSINE)
        },
    )
    point = learn_chunk("block-v1:a", "First file.")
    learn_qdrant.upsert(
        collection, [point.model_copy(update={"vector": {DENSE: [1.0] * learn_size}})]
    )

    evaluation = check_content_files(tables)

    assert not evaluation.passed
    assert evaluation.severity == AssetCheckSeverity.ERROR
    assert f"{learn_size} long" in evaluation.description
    metadata = {key: value.value for key, value in evaluation.metadata.items()}
    assert (
        metadata["learn_vector_size"],
        metadata["platform_vector_size"],
        metadata["text_matches"],
        metadata["vector_length_mismatches"],
        metadata["compared_vectors"],
    ) == (learn_size, VECTOR_LENGTH, 1, 1, 0)


def test_topics_are_compared_by_topic_uuid(tables, learn_qdrant):
    other_topic = "0f6c1a52-55f4-4b1e-9d2e-6a5b4c3d2e1f"
    tables[TOPICS_RELATION] = pl.DataFrame(
        [
            {
                "learningresourcetopic_id": number,
                "learningresourcetopic_name": name,
                "learningresourcetopic_parent_id": None,
                "learningresourcetopic_uuid": topic_uuid,
            }
            for number, (name, topic_uuid) in enumerate(
                [("Calculus", TOPIC_UUID), ("Algebra", other_topic)]
            )
        ]
    )
    learn_qdrant.upsert(
        f"{LEARN_BASE}.topics",
        [
            models.PointStruct(
                id=TOPIC_UUID,
                vector={DENSE: fake_vector("Calculus")},
                payload={"name": "Calculus"},
            ),
            # A topic whose name embedded to zeros is written with no vectors.
            models.PointStruct(id=other_topic, vector={}, payload={"name": "Algebra"}),
        ],
    )

    result = materialize(
        [topic_embedding_chunks, topic_embedding_vectors, topic_vectors_match_learn],
        resources={
            "io_manager": Warehouse(tables),
            "embeddings": fake_embeddings(),
            "learn_qdrant": fake_qdrant(),
        },
    )

    (evaluation,) = result.get_asset_check_evaluations()
    assert evaluation.passed
    metadata = {key: value.value for key, value in evaluation.metadata.items()}
    assert (
        metadata["matched_ids"],
        metadata["text_matches"],
        metadata["learn_points_without_vector"],
        metadata["compared_vectors"],
    ) == (2, 2, 1, 1)
    assert "checksum_matches" not in metadata


# ------------------------------------------------------------------- definitions
@pytest.mark.parametrize("dagster_env", ["dev", "qa"])
def test_learn_embedding_definitions_load(dagster_env, monkeypatch):
    monkeypatch.setenv("LEARN_QDRANT_COLLECTION_BASE", "mitlearn-qa")
    vault = Vault(vault_addr="https://vault.example.com", vault_auth_type="token")
    resources, checks = learn_parity_definitions(vault, dagster_env)

    # Dev has no Learn cluster, so its vector builds run without the checks.
    assert len(checks) == (0 if dagster_env == "dev" else 2)
    assert set(resources) == (set() if dagster_env == "dev" else {"learn_qdrant"})
    definitions = Definitions(
        assets=[
            learn_content_file_text,
            content_file_embedding_chunks,
            content_file_embedding_vectors,
            topic_embedding_chunks,
            topic_embedding_vectors,
            embedding_strategies,
        ],
        asset_checks=checks,
        sensors=[learn_content_file_text_sensor],
        resources={
            "io_manager": Warehouse({}),
            "learn_embeddings_io_manager": Warehouse({}),
            "embeddings": fake_embeddings(),
            **resources,
        },
    )
    definitions.resolve_all_job_defs()
    assert len(definitions.resolve_asset_graph().asset_check_keys) == len(checks)


def test_a_deployed_env_without_learns_collection_base_fails_to_load(monkeypatch):
    monkeypatch.delenv("LEARN_QDRANT_COLLECTION_BASE", raising=False)
    vault = Vault(vault_addr="https://vault.example.com", vault_auth_type="token")

    with pytest.raises(KeyError, match="LEARN_QDRANT_COLLECTION_BASE"):
        learn_parity_definitions(vault, "production")


def test_strategy_table_has_a_row_per_strategy(tables):
    result = materialize(
        [embedding_strategies], resources={"io_manager": Warehouse(tables)}
    )

    assert result.success
    assert sorted(tables["embedding_strategies"]["strategy_id"]) == [
        "learn_v1_content_files",
        "learn_v1_topics",
    ]

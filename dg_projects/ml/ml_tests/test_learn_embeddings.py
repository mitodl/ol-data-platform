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
from dagster._core.definitions.data_version import DATA_VERSION_TAG
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
from ml.lib.learn_embeddings import STRATEGIES_PATH, TOPICS_RELATION, strategies
from ml.sensors.learn_embeddings import learn_content_file_text_sensor
from ol_llm import LLMRejected, LLMUnavailable
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
    assert row["model_id"][0] == strategies()["learn_v1_content_files"].model.model_id

    tables[CONTENT_FILE_VECTORS_KEY.path[-1]] = stored_vector(
        "Shared syllabus.", learn_model, learn_model
    )
    assert build_content_files(tables).success
    assert calls == [["Shared syllabus."]]


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
        schema={"readable_id": pl.String, "run_id": pl.String}
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

"""Materialize the courseware assets against an in-memory warehouse and a fake model.

Tables live in a dict keyed by table name. The IO manager appends to it as the
Iceberg IO manager's append mode does, so a second materialization reads what the
first wrote. The model is an Anthropic client whose messages.stream answers each
tool the courseware library offers, so the library's own parsing and validation run.
"""

import re
from collections.abc import Iterator
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import polars as pl
import pytest
from anthropic import Anthropic
from dagster import (
    DagsterInstance,
    Definitions,
    IOManager,
    MultiPartitionKey,
    build_sensor_context,
    define_asset_job,
    materialize,
)
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager
from ml.assets import courseware_extraction as extraction_module
from ml.assets.courseware_extraction import (
    EXTRACTION_TEXT_KEY,
    EXTRACTOR_VERSION,
    courseware_extraction,
    courseware_extraction_text,
    extraction_text_version,
)
from ml.lib.courseware import warehouse
from ml.resources import opik_auth
from ml.resources.llm import LLMClientFactory
from ml.sensors import courseware as sensor_module
from ml.sensors.courseware import courseware_extraction_text_sensor
from ol_orchestrate.lib.failures import (
    RETRY_ON_ASSET_OR_OP_FAILURE_TAG,
    PermanentFailure,
    with_failure_hooks,
)
from ol_orchestrate.lib.glue_helper import FIXTURE_DIR_VARIABLE
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import CommitFailedException, NoSuchTableError

PLATFORM = "mitxonline"
COURSERUN = "course-v1:MITx+UAI.4+1T2026"
PARTITION = MultiPartitionKey({"platform": PLATFORM, "courserun": COURSERUN})
CHECKSUMS = "int__courseware__extraction_source_checksums"


def checksums(
    *courseruns: str, text_checksum: str = "abc", prompt_version: str | None = None
) -> pl.DataFrame:
    """Allowlisted runs, approved for this code's extractor version by default."""
    version = {**EXTRACTOR_VERSION}
    if prompt_version is not None:
        version["prompt_version"] = prompt_version
    return pl.DataFrame(
        [
            {
                "platform": PLATFORM,
                "courserun_readable_id": courserun,
                "source_count": 3,
                "text_checksum": text_checksum,
                **version,
            }
            for courserun in courseruns or (COURSERUN,)
        ]
    )


# ---------------------------------------------------------------- fake warehouse
class Warehouse(IOManager):
    def __init__(self, tables: dict[str, pl.DataFrame]) -> None:
        self.tables = tables

    def handle_output(self, context, obj: pl.DataFrame) -> None:
        table = context.asset_key.path[-1]
        previous = self.tables.get(table)
        self.tables[table] = (
            obj if previous is None else pl.concat([previous, obj], how="diagonal")
        )

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


# -------------------------------------------------------------------- fake model
def _response(tool: str, tool_input: dict[str, Any] | None) -> SimpleNamespace:
    content = (
        [SimpleNamespace(type="tool_use", name=tool, input=tool_input)]
        if tool_input is not None
        else []
    )
    return SimpleNamespace(
        stop_reason="tool_use" if content else "end_turn",
        stop_details=None,
        content=content,
        usage=SimpleNamespace(
            input_tokens=100,
            output_tokens=20,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
        ),
    )


def extraction_input(message: str, *, quote: bool = True) -> dict[str, Any] | None:
    """One taught concept per scope, quoting the first ten words of source S01."""
    text = message.split("=== SOURCE S01", 1)[1].split("===\n", 1)[1]
    scope = re.search(r"^SCOPE: (.+) \(kind=", message, re.MULTILINE)
    assert scope is not None
    title = scope[1]
    if title == "Broken scope":
        return None
    passage = " ".join(text.split()[:10]) if quote else "words that appear nowhere"
    return {
        "summary": f"About {title}.",
        "concepts": [
            {
                "label": f"{title.lower()} concept",
                "description": f"What {title} teaches.",
                "concept_kind": "concept",
                "teaching_status": "taught",
                "evidence": [{"source_id": "S01", "passage": passage}],
            }
        ],
        "skills": [],
        "assumed_knowledge": [],
        "assessment_expectations": [],
        "learning_outcomes": [],
        "declared_prerequisites": [],
        "audience": [],
        "notes": [],
    }


class FakeMessages:
    def __init__(self, calls: list[str], *, quote: bool) -> None:
        self.calls = calls
        self.quote = quote

    def stream(self, **request: Any) -> Iterator[SimpleNamespace]:
        tool = request["tools"][0]["name"]
        self.calls.append(tool)
        content = request["messages"][0]["content"]
        tool_input = {
            "record_extraction": lambda: extraction_input(content, quote=self.quote),
        }[tool]()
        stream = MagicMock()
        stream.__enter__.return_value.get_final_message.return_value = _response(
            tool, tool_input
        )
        return stream


CALLS: list[str] = []


class FakeLLM(LLMClientFactory):
    quote: bool = True

    def get_client(self) -> Anthropic:
        client = Anthropic(api_key="test")  # pragma: allowlist secret
        client.messages = FakeMessages(CALLS, quote=self.quote)
        return client


@pytest.fixture(autouse=True)
def calls() -> list[str]:
    CALLS.clear()
    return CALLS


# -------------------------------------------------------------------- extraction
def sources(*scope_titles: str) -> pl.DataFrame:
    rows = [
        {
            "platform": PLATFORM,
            "courserun_readable_id": COURSERUN,
            "course_title": "Supervised Learning",
            "scope_id": f"seq-{position}",
            "scope_kind": "unit",
            "scope_title": title,
            "scope_position": position,
            "source_position": 1,
            "source_ref": f"block-{position}",
            "source_kind": "html",
            "source_title": None,
            "source_text": (
                f"In {title} we study how a model learns from labelled examples "
                "and how to measure the error it makes on new data."
            ),
        }
        for position, title in enumerate(scope_titles, 1)
    ]
    return pl.DataFrame(rows)


def extract(
    tables,
    *,
    llm: LLMClientFactory | None = None,
    instance: DagsterInstance | None = None,
):
    if instance is None:
        instance = DagsterInstance.ephemeral()
        instance.add_dynamic_partitions("courseware_courserun", [COURSERUN])
    tables.setdefault(CHECKSUMS, checksums())
    # With the hooks definitions.py attaches, so stop_run_retries runs.
    (hooked,) = with_failure_hooks([courseware_extraction])
    return materialize(
        [hooked],
        partition_key=PARTITION,
        instance=instance,
        resources={
            "courseware_io_manager": Warehouse(tables),
            "llm": llm or FakeLLM(),
        },
        raise_on_error=False,
    )


def check_result(result, name: str):
    return next(
        evaluation
        for evaluation in result.get_asset_check_evaluations()
        if evaluation.check_name == name
    )


@pytest.fixture
def catalog(monkeypatch) -> MagicMock:
    fake = MagicMock()
    monkeypatch.setattr(extraction_module, "get_glue_catalog", lambda: fake)
    return fake


def test_extraction_writes_each_scope_once(tables, calls, catalog):
    tables["int__courseware__extraction_sources"] = sources("Lecture 1", "Lecture 2")

    first = extract(tables)

    assert first.success
    assert tables["courseware_claim_extraction"].height == 2
    claims = tables["courseware_claim"]
    assert set(claims["courserun_readable_id"]) == {COURSERUN}
    assert set(claims["platform"]) == {PLATFORM}
    assert set(tables["courseware_claim_evidence"]["verification_status"]) == {"exact"}
    check = check_result(first, "evidence_verification_rate")
    assert check.passed
    assert check.metadata["exact_share"].value == 1.0
    # leftovers of an earlier attempt under the new keys are cleared first
    assert catalog.load_table.return_value.delete.call_count == 2

    second = extract(tables)

    assert second.success
    assert calls == ["record_extraction", "record_extraction"]
    assert tables["courseware_claim_extraction"].height == 2


@pytest.mark.usefixtures("catalog")
def test_extraction_skips_a_scope_stored_under_the_old_key_scheme(tables, calls):
    tables["int__courseware__extraction_sources"] = sources("Lecture 1")
    assert extract(tables).success
    # as stored before keys left out the course header: another key, no header
    tables["courseware_claim_extraction"] = (
        tables["courseware_claim_extraction"]
        .with_columns(extraction_key=pl.lit("old-key"))
        .drop("header_version")
    )

    assert extract(tables).success

    assert calls == ["record_extraction"]


def test_extraction_writes_partitioned_iceberg_tables(calls, tmp_path, monkeypatch):
    """Through the Iceberg IO manager the deployment configures, on a local catalog."""
    properties = {
        "type": "sql",
        "uri": f"sqlite:///{tmp_path}/catalog.db",
        "warehouse": f"file://{tmp_path}",
    }
    catalog = SqlCatalog("default", **properties)
    database = warehouse.warehouse_database("intermediate")
    catalog.create_namespace(database)
    monkeypatch.setattr(extraction_module, "get_glue_catalog", lambda: catalog)
    source_rows = sources("Lecture 1", "Lecture 2")

    def read(database_name: str, table_name: str) -> pl.LazyFrame:
        if table_name == "int__courseware__extraction_sources":
            return source_rows.lazy()
        if table_name == CHECKSUMS:
            return checksums().lazy()
        table = catalog.load_table(f"{database_name}.{table_name}")
        return pl.DataFrame(table.scan().to_arrow()).lazy()

    monkeypatch.setattr(warehouse, "get_dbt_model_as_dataframe", read)
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("courseware_courserun", [COURSERUN])
    io_manager = PolarsIcebergIOManager(
        name="default",
        config=IcebergCatalogConfig(properties=properties),
        db_io_manager="custom",
    )

    for _ in range(2):
        result = materialize(
            [courseware_extraction],
            partition_key=PARTITION,
            instance=instance,
            resources={"courseware_io_manager": io_manager, "llm": FakeLLM()},
        )
        assert result.success

    assert calls == ["record_extraction", "record_extraction"]
    for table in ("courseware_claim_extraction", "courseware_claim"):
        written = catalog.load_table(f"{database}.{table}")
        assert written.scan().to_arrow().num_rows == 2
        assert [field.name for field in written.spec().fields] == [
            "part_courserun_readable_id",
            "part_platform",
        ]


@pytest.mark.usefixtures("catalog")
def test_extraction_check_fails_on_unquoted_evidence(tables):
    tables["int__courseware__extraction_sources"] = sources("Lecture 1")

    result = extract(tables, llm=FakeLLM(quote=False))

    assert result.success
    check = check_result(result, "evidence_verification_rate")
    assert not check.passed
    assert check.metadata["unverified"].value == 1


@pytest.mark.usefixtures("catalog")
def test_extraction_stores_successes_and_fails_the_run(tables, calls):
    tables["int__courseware__extraction_sources"] = sources("Lecture 1", "Broken scope")

    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("courseware_courserun", [COURSERUN])

    result = extract(tables, instance=instance)

    assert not result.success
    failure = next(
        event.step_failure_data.error
        for event in result.all_events
        if event.is_step_failure
    )
    assert failure is not None
    assert failure.cls_name == "ScopeExtractionFailure"
    # permanent, so run_retries does not re-run the partition and pay again
    run = instance.get_run_by_id(result.run_id)
    assert run is not None
    assert run.tags[RETRY_ON_ASSET_OR_OP_FAILURE_TAG] == "false"
    assert tables["courseware_claim_extraction"]["scope_title"].to_list() == [
        "Lecture 1"
    ]
    # the broken scope was asked for twice (one retry), the good one once
    assert calls.count("record_extraction") == 3

    extract(tables)

    assert calls.count("record_extraction") == 5


def test_extraction_fails_before_any_call_when_the_catalog_fails(
    tables, calls, catalog
):
    tables["int__courseware__extraction_sources"] = sources("Lecture 1")
    catalog.load_table.side_effect = OSError("Glue unreachable")

    result = extract(tables)

    assert not result.success
    assert calls == []


def test_extraction_retries_a_lost_delete_commit(tables, catalog):
    tables["int__courseware__extraction_sources"] = sources("Lecture 1")
    table = catalog.load_table.return_value
    table.delete.side_effect = [CommitFailedException("concurrent write"), None, None]

    result = extract(tables)

    assert result.success
    assert table.delete.call_count == 3
    assert table.refresh.call_count == 1


def _fail_publish(_name: str, _template: str) -> str:
    msg = "Opik is down"
    raise RuntimeError(msg)


@pytest.mark.usefixtures("catalog")
def test_extraction_publishes_its_prompt_before_any_call(tables, calls, monkeypatch):
    tables["int__courseware__extraction_sources"] = sources("Lecture 1")
    monkeypatch.setattr(extraction_module, "publish_prompt_version", _fail_publish)

    result = extract(tables)

    assert not result.success
    assert calls == []
    assert "courseware_claim" not in tables


def test_publish_prompt_version_never_raises(monkeypatch):
    # a Prompt without the attributes this SDK version has
    client = SimpleNamespace(create_prompt=lambda **_: object())
    monkeypatch.setattr(opik_auth, "get_opik_client", lambda: client)

    assert opik_auth.publish_prompt_version("name", "template") == (
        opik_auth.LOCAL_PROMPT_VERSION
    )


def test_reads_refuse_a_dev_redirect(monkeypatch, tmp_path):
    monkeypatch.setenv(FIXTURE_DIR_VARIABLE, str(tmp_path))
    with pytest.raises(PermanentFailure, match=FIXTURE_DIR_VARIABLE):
        warehouse.read_table("intermediate", "courseware_claim_extraction")


def test_an_empty_schema_suffix_is_refused(monkeypatch):
    monkeypatch.setenv("DBT_SCHEMA_SUFFIX", "")
    with pytest.raises(ValueError, match="DBT_SCHEMA_SUFFIX"):
        warehouse.warehouse_database("intermediate")


def test_extraction_refuses_a_run_not_approved_for_this_version(tables, calls):
    tables["int__courseware__extraction_sources"] = sources("Lecture 1")
    tables[CHECKSUMS] = checksums(prompt_version="0ld0ld0ld0ld")

    result = extract(tables)

    assert not result.success
    (failure,) = [
        event.step_failure_data.user_failure_data
        for event in result.all_events
        if event.is_step_failure
    ]
    assert "approves courseware_claims_v1-0ld0ld0ld0ld-" in failure.description
    assert failure.metadata["retryable"].value is False
    assert calls == []
    assert "courseware_claim_extraction" not in tables

    tables[CHECKSUMS] = checksums("course-v1:MITx+OTHER+1T")

    assert not extract(tables).success
    assert calls == []


def test_extraction_text_observes_one_run(tables):
    tables[CHECKSUMS] = checksums(COURSERUN, "course-v1:MITx+OTHER+1T")
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("courseware_courserun", [COURSERUN])

    job = Definitions(
        assets=[courseware_extraction_text],
        jobs=[define_asset_job("observe", selection=[EXTRACTION_TEXT_KEY])],
    ).resolve_job_def("observe")
    result = job.execute_in_process(instance=instance, partition_key=PARTITION)

    (observation,) = [
        event.asset_observation_data.asset_observation
        for event in result.all_events
        if event.is_asset_observation
    ]
    assert observation.partition == PARTITION
    assert observation.data_version == extraction_text_version("abc")


# ------------------------------------------------------------------------ sensor
def test_sensor_registers_then_observes_then_rests(tables):
    tables[CHECKSUMS] = checksums()
    instance = DagsterInstance.ephemeral()

    first = courseware_extraction_text_sensor(build_sensor_context(instance=instance))

    assert first.asset_events == []
    assert first.dynamic_partitions_requests[0].partition_keys == [COURSERUN]
    assert first.cursor is None

    instance.add_dynamic_partitions("courseware_courserun", [COURSERUN])
    second = courseware_extraction_text_sensor(
        build_sensor_context(instance=instance, cursor=first.cursor)
    )

    assert [
        (event.partition, event.tags[sensor_module.DATA_VERSION_TAG])
        for event in second.asset_events
    ] == [(PARTITION, extraction_text_version("abc"))]
    assert second.dynamic_partitions_requests == []

    third = courseware_extraction_text_sensor(
        build_sensor_context(instance=instance, cursor=second.cursor)
    )

    assert third.asset_events == []
    assert third.cursor is None


def observed(result) -> list[tuple[str, str]]:
    return [
        (
            event.partition.keys_by_dimension["courserun"],
            event.tags[sensor_module.DATA_VERSION_TAG],
        )
        for event in result.asset_events
    ]


def test_sensor_waits_for_a_run_to_be_promoted(tables, monkeypatch):
    lagging = "course-v1:MITx+OTHER+1T"
    tables[CHECKSUMS] = pl.concat(
        [checksums(), checksums(lagging, prompt_version="0ld0ld0ld0ld")]
    )
    instance = DagsterInstance.ephemeral()
    instance.add_dynamic_partitions("courseware_courserun", [COURSERUN, lagging])

    first = courseware_extraction_text_sensor(build_sensor_context(instance=instance))

    # A run approved for another version is not observed, so nothing extracts it.
    assert observed(first) == [(COURSERUN, extraction_text_version("abc"))]

    tables[CHECKSUMS] = checksums(COURSERUN, lagging)
    promoted = courseware_extraction_text_sensor(
        build_sensor_context(instance=instance, cursor=first.cursor)
    )

    assert observed(promoted) == [
        (lagging, extraction_text_version("abc")),
        (COURSERUN, extraction_text_version("abc")),
    ]

    # A new extractor version in code alone observes nothing: every row now lags.
    monkeypatch.setitem(EXTRACTOR_VERSION, "prompt_version", "n3wn3wn3wn3w")
    deployed = courseware_extraction_text_sensor(
        build_sensor_context(instance=instance, cursor=promoted.cursor)
    )

    assert observed(deployed) == []
    assert deployed.cursor not in {None, promoted.cursor}


def test_sensor_registers_only_approved_runs(tables):
    tables[CHECKSUMS] = pl.concat(
        [
            checksums(),
            checksums("course-v1:MITx+OTHER+1T", prompt_version="0ld0ld0ld0ld"),
        ]
    )

    result = courseware_extraction_text_sensor(
        build_sensor_context(instance=DagsterInstance.ephemeral())
    )

    assert result.dynamic_partitions_requests[0].partition_keys == [COURSERUN]


def test_courseware_definitions_load():
    Definitions(
        assets=[
            courseware_extraction_text,
            courseware_extraction,
        ],
        sensors=[courseware_extraction_text_sensor],
        resources={
            "io_manager": Warehouse({}),
            "courseware_io_manager": Warehouse({}),
            "llm": FakeLLM(),
        },
    ).resolve_all_job_defs()

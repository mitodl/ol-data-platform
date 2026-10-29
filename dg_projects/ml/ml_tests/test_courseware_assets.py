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
    AssetMaterialization,
    AssetSelection,
    AssetSpec,
    DagsterInstance,
    DagsterRun,
    DagsterRunStatus,
    Definitions,
    IOManager,
    MultiPartitionKey,
    build_sensor_context,
    define_asset_job,
    evaluate_automation_conditions,
    materialize,
)
from dagster._core.definitions.data_version import DATA_VERSION_TAG
from dagster._core.storage.tags import AUTOMATION_CONDITION_TAG
from dagster_iceberg.config import IcebergCatalogConfig
from dagster_iceberg.io_manager.polars import PolarsIcebergIOManager
from ml.assets import courseware_description as description_module
from ml.assets import courseware_extraction as extraction_module
from ml.assets import courseware_release as release_module
from ml.assets import courseware_resolution as resolution_module
from ml.assets.courseware_description import courseware_concept_description
from ml.assets.courseware_extraction import (
    EXTRACTION_TEXT_KEY,
    EXTRACTOR_VERSION,
    courseware_extraction,
    courseware_extraction_text,
    extraction_text_version,
)
from ml.assets.courseware_graph import courseware_concept_graph
from ml.assets.courseware_release import (
    courseware_knowledge_release,
    courseware_release_publication,
)
from ml.assets.courseware_resolution import (
    RUN_KEY,
    assignment_churn,
    courseware_resolution,
)
from ml.lib import warehouse
from ml.lib.courseware import graph
from ml.lib.courseware.describe import (
    DESCRIPTION_PROMPT_VERSION,
    current_input_checksums,
)
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

from .test_courseware_resolve import FakeEmbedder

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


def verdicts_input(listing: str, fail_label: str | None) -> dict[str, Any] | None:
    """Every candidate distinct, so each label becomes its own concept.

    A batch with fail_label among its queries gets no tool call, so it fails.
    """
    if fail_label and re.search(rf"^Q\d+: {fail_label} \|", listing, re.MULTILINE):
        return None
    queries: list[dict[str, Any]] = []
    for line in listing.splitlines():
        if line.startswith("Q"):
            queries.append({"query_id": len(queries), "verdicts": []})
        elif line.startswith("  c"):
            candidate = int(line.split(":", 1)[0].strip()[1:])
            queries[-1]["verdicts"].append(
                {"candidate_id": candidate, "verdict": "distinct"}
            )
    return {"queries": queries}


def senses_input(listing: str, fail_label: str | None) -> dict[str, Any] | None:
    """Every course run takes the label's first sense.

    A batch with fail_label among its labels gets no tool call, so it fails.
    """
    if fail_label and re.search(rf"^Q\d+: {fail_label}$", listing, re.MULTILINE):
        return None
    queries: list[dict[str, Any]] = []
    for line in listing.splitlines():
        if line.startswith("Q"):
            queries.append({"query_id": len(queries), "runs": []})
        elif line.startswith("  r"):
            run = int(line.split(":", 1)[0].strip()[1:])
            queries[-1]["runs"].append(
                {"run_id": run, "decision": "existing", "sense": 0}
            )
    return {"queries": queries}


def descriptions_input(listing: str, fail_label: str | None) -> dict[str, Any]:
    """Describe every entry except the one labelled fail_label."""
    ids = [
        int(index)
        for index, labels in re.findall(
            r"^(\d+) \| labels: (.+?) \|", listing, re.MULTILINE
        )
        if labels != fail_label
    ]
    return {
        "descriptions": [
            {
                "concept_id": concept_id,
                "short_description": "a thing to learn",
                "description": "A thing learners study.",
                "is_learner_facing": True,
            }
            for concept_id in ids
        ]
    }


class FakeMessages:
    def __init__(
        self, calls: list[str], *, quote: bool, fail_label: str | None
    ) -> None:
        self.calls = calls
        self.quote = quote
        self.fail_label = fail_label

    def stream(self, **request: Any) -> Iterator[SimpleNamespace]:
        tool = request["tools"][0]["name"]
        self.calls.append(tool)
        content = request["messages"][0]["content"]
        tool_input = {
            "record_extraction": lambda: extraction_input(content, quote=self.quote),
            "record_verdicts": lambda: verdicts_input(content, self.fail_label),
            "record_senses": lambda: senses_input(content, self.fail_label),
            "record_descriptions": lambda: descriptions_input(content, self.fail_label),
        }[tool]()
        stream = MagicMock()
        stream.__enter__.return_value.get_final_message.return_value = _response(
            tool, tool_input
        )
        return stream


CALLS: list[str] = []


class FakeLLM(LLMClientFactory):
    quote: bool = True
    fail_label: str | None = None

    def get_client(self) -> Anthropic:
        client = Anthropic(api_key="test")  # pragma: allowlist secret
        client.messages = FakeMessages(
            CALLS, quote=self.quote, fail_label=self.fail_label
        )
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


def test_description_publishes_its_prompt_before_any_call(tables, calls, monkeypatch):
    tables[DESCRIPTION_INPUTS] = description_inputs(concepts={"c1": "auc"})
    monkeypatch.setattr(description_module, "publish_prompt_version", _fail_publish)

    result = materialize(
        [courseware_concept_description],
        resources={"io_manager": Warehouse(tables), "llm": FakeLLM()},
        raise_on_error=False,
    )

    assert not result.success
    assert calls == []


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


def labels(*keys: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "concept_label_key": list(keys),
            "preferred_label": list(keys),
            "concept_kind": ["concept"] * len(keys),
            "description": [f"about {key}" for key in keys],
            "mention_count": [1] * len(keys),
            "courserun_count": [1] * len(keys),
            "taught_mention_count": [1] * len(keys),
        }
    )


def sense_inputs(
    keys: list[str], courseruns: tuple[str, ...] = (COURSERUN,)
) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "concept_label_key": key,
                "platform": PLATFORM,
                "courserun_readable_id": courserun,
                "course_title": f"Course {courserun}",
                "label": key,
                "concept_kind": "concept",
                "description": f"about {key}",
                "teaching_status": "taught",
            }
            for key in keys
            for courserun in courseruns
        ]
    )


def resolve(
    tables,
    *,
    llm: LLMClientFactory | None = None,
    rebuild: bool = False,
    instance: DagsterInstance | None = None,
    courseruns: tuple[str, ...] = (COURSERUN,),
):
    """Materialize resolution; every current label is used by courseruns."""
    tables["int__courseware__concept_sense_inputs"] = sense_inputs(
        tables["int__courseware__concept_labels"]["concept_label_key"].to_list(),
        courseruns,
    )
    (hooked,) = with_failure_hooks([courseware_resolution])
    return materialize(
        [hooked],
        instance=instance,
        resources={
            "io_manager": Warehouse(tables),
            "llm": llm or FakeLLM(),
        },
        run_config={"ops": {"courseware_resolution": {"config": {"rebuild": rebuild}}}},
        raise_on_error=False,
    )


def failure_class(result) -> str:
    error = next(
        event.step_failure_data.error
        for event in result.all_events
        if event.is_step_failure
    )
    assert error is not None
    return error.cls_name


@pytest.fixture
def embedder(monkeypatch) -> None:
    monkeypatch.setattr(resolution_module, "label_embedder", lambda **_: FakeEmbedder())


@pytest.mark.usefixtures("embedder")
def test_resolution_resolves_only_new_labels(tables, calls):
    tables["int__courseware__concept_labels"] = labels("auc", "accuracy", "bias")

    first = resolve(tables)

    assert check_result(first, "resolution_churn").passed
    assert tables["courseware_resolution_run"].height == 1
    assert tables["courseware_concept_assignment"].height == 3
    first_ids = dict(
        tables["courseware_concept_assignment"]
        .select("concept_label_key", "concept_id")
        .iter_rows()
    )

    # nothing new: no adjudication and no new run
    calls.clear()
    resolve(tables)
    assert calls == []
    assert tables["courseware_resolution_run"].height == 1

    tables["int__courseware__concept_labels"] = labels(
        "auc", "accuracy", "bias", "boosting"
    )
    third = resolve(tables)

    runs = tables["courseware_resolution_run"].sort("started_at")
    assert runs["resolved_label_count"].to_list() == [3, 1]
    latest = tables["courseware_concept_assignment"].filter(
        pl.col("resolution_run_id") == runs["resolution_run_id"][-1]
    )
    assert {
        key: concept_id
        for key, concept_id in latest.select(
            "concept_label_key", "concept_id"
        ).iter_rows()
        if key in first_ids
    } == first_ids
    churn = check_result(third, "resolution_churn")
    assert churn.passed
    assert churn.metadata["labels_in_both_runs"].value == 3
    assert churn.metadata["labels_with_changed_id"].value == 0


@pytest.mark.usefixtures("embedder")
def test_resolution_refuses_to_overlap_another_resolution_run(tables, calls):
    tables["int__courseware__concept_labels"] = labels("auc", "accuracy")
    instance = DagsterInstance.ephemeral()
    for run_id, status, selection in (
        ("other-resolution", DagsterRunStatus.STARTED, frozenset({RUN_KEY})),
        ("finished-resolution", DagsterRunStatus.SUCCESS, frozenset({RUN_KEY})),
        ("other-asset", DagsterRunStatus.STARTED, None),
    ):
        instance.add_run(
            DagsterRun(
                job_name="__ASSET_JOB",
                run_id=run_id,
                status=status,
                asset_selection=selection,
            )
        )

    result = resolve(tables, instance=instance)

    assert not result.success
    error = next(e for e in result.all_events if e.is_step_failure)
    message = error.step_failure_data.error.message
    assert "other-resolution" in message
    assert "finished-resolution" not in message
    assert calls == []
    assert "courseware_resolution_run" not in tables

    instance.delete_run("other-resolution")
    assert resolve(tables, instance=instance).success


@pytest.mark.usefixtures("embedder")
def test_resolution_stores_what_succeeded_and_fails_on_a_failed_batch(tables, calls):
    tables["int__courseware__concept_labels"] = labels("auc", "accuracy", "bias")
    resolve(tables)
    tables["int__courseware__concept_labels"] = labels(
        "auc", "accuracy", "bias", "boosting"
    )
    instance = DagsterInstance.ephemeral()

    failed = resolve(tables, llm=FakeLLM(fail_label="boosting"), instance=instance)

    assert not failed.success
    assert failure_class(failed) == "AdjudicationFailure"
    run = instance.get_run_by_id(failed.run_id)
    assert run is not None
    assert run.tags[RETRY_ON_ASSET_OR_OP_FAILURE_TAG] == "false"
    latest = tables["courseware_resolution_run"].sort("started_at").row(-1, named=True)
    assert (latest["failed_batch_count"], latest["failed_label_count"]) == (1, 1)
    assert "boosting" not in set(
        tables["courseware_concept_assignment"].filter(
            pl.col("resolution_run_id") == latest["resolution_run_id"]
        )["concept_label_key"]
    )
    materialization = next(
        event
        for event in failed.get_asset_materialization_events()
        if event.asset_key.path[-1] == "courseware_resolution_run"
    )
    assert materialization.materialization.metadata["unassigned_label_count"].value == 1

    # the retry asks only for the label still without a concept
    calls.clear()
    retried = resolve(tables)

    assert retried.success
    assert calls == ["record_verdicts"]
    latest = tables["courseware_resolution_run"].sort("started_at").row(-1, named=True)
    assert latest["resolved_label_count"] == 1


@pytest.mark.usefixtures("embedder")
def test_a_failed_rebuild_batch_keeps_concepts_and_leaves_none_unassigned(tables):
    tables["int__courseware__concept_labels"] = labels("auc", "accuracy", "bias")
    resolve(tables)

    rebuilt = resolve(tables, llm=FakeLLM(fail_label="auc"), rebuild=True)

    assert failure_class(rebuilt) == "AdjudicationFailure"
    materialization = next(
        event
        for event in rebuilt.get_asset_materialization_events()
        if event.asset_key.path[-1] == "courseware_resolution_run"
    )
    metadata = materialization.materialization.metadata
    assert metadata["failed_label_count"].value == 3
    # the failed labels kept their concepts, so nothing is left for a retry
    assert metadata["unassigned_label_count"].value == 0
    assert (
        check_result(rebuilt, "resolution_churn")
        .metadata["labels_with_changed_id"]
        .value
        == 0
    )


@pytest.mark.usefixtures("embedder")
def test_resolution_assigns_senses_for_new_course_runs_of_known_labels(tables, calls):
    tables["int__courseware__concept_labels"] = labels("kernel", "bias")
    resolve(tables)
    # one course run per label: each takes its only sense without a model call
    assert calls == ["record_verdicts"]
    assert tables["courseware_concept_sense_assignment"][
        "assignment_source"
    ].to_list() == ["single_sense", "single_sense"]
    assert tables["courseware_concept_alias"].height == 2
    concept_of = dict(
        tables["courseware_concept_assignment"]
        .select("concept_label_key", "concept_id")
        .iter_rows()
    )

    # no new label, but a second course run uses both: the run is not skipped,
    # and a failed sense batch (both labels share it) fails it with the new runs
    # left undecided
    calls.clear()
    other = "course-v1:MITx+6.036+1T2026"
    failed = resolve(
        tables, llm=FakeLLM(fail_label="kernel"), courseruns=(COURSERUN, other)
    )

    assert failure_class(failed) == "AdjudicationFailure"
    assert calls == ["record_senses"]
    materialization = next(
        event
        for event in failed.get_asset_materialization_events()
        if event.asset_key.path[-1] == "courseware_resolution_run"
    )
    assert (
        materialization.materialization.metadata["undecided_sense_unit_count"].value
        == 2
    )

    # the next run asks again about the runs still undecided
    calls.clear()
    retried = resolve(tables, courseruns=(COURSERUN, other))

    assert retried.success
    assert calls == ["record_senses"]
    latest = tables["courseware_resolution_run"].sort("started_at")[
        "resolution_run_id"
    ][-1]
    senses = tables["courseware_concept_sense_assignment"].filter(
        pl.col("resolution_run_id") == latest
    )
    assert {
        (key, courserun): concept_id
        for key, courserun, concept_id in senses.select(
            "concept_label_key", "courserun_readable_id", "concept_id"
        ).iter_rows()
    } == {
        (key, courserun): concept_of[key]
        for key in ("kernel", "bias")
        for courserun in (COURSERUN, other)
    }

    # every unit is decided now, so nothing runs
    calls.clear()
    resolve(tables, courseruns=(COURSERUN, other))
    assert calls == []
    assert tables["courseware_resolution_run"]["resolution_run_id"].n_unique() == 3


def test_assignment_churn_counts_changed_ids():
    previous = pl.DataFrame(
        {"concept_label_key": ["a", "b", "c"], "concept_id": ["1", "2", "3"]}
    )
    current = pl.DataFrame(
        {"concept_label_key": ["a", "b", "d"], "concept_id": ["1", "1", "4"]}
    )
    assert assignment_churn(previous, current, {"a", "b", "c", "d"}) == (2, 1)
    # a carried label that is no longer current is left out of both counts
    assert assignment_churn(previous, current, {"a", "d"}) == (1, 0)


@pytest.mark.usefixtures("embedder")
def test_resolution_ignores_carried_labels_that_dropped_out(tables, calls):
    tables["int__courseware__concept_labels"] = labels("auc", "accuracy", "bias")
    resolve(tables)
    # bias drops out; the next run carries its assignment
    tables["int__courseware__concept_labels"] = labels("auc", "accuracy", "boosting")
    resolve(tables)
    runs = tables["courseware_resolution_run"]
    assert runs.height == 2
    latest_id = runs.sort("started_at")["resolution_run_id"][-1]
    assert "bias" in set(
        tables["courseware_concept_assignment"].filter(
            pl.col("resolution_run_id") == latest_id
        )["concept_label_key"]
    )

    # every current label is assigned, so nothing runs despite the carried bias row
    calls.clear()
    skipped = resolve(tables)

    assert calls == []
    assert tables["courseware_resolution_run"].height == 2
    churn = check_result(skipped, "resolution_churn")
    assert churn.metadata["labels_in_both_runs"].value == 3

    # a failed batch leaves the new label unassigned, whatever bias carries
    tables["int__courseware__concept_labels"] = labels(
        "auc", "accuracy", "boosting", "cnn"
    )
    failed = resolve(tables, llm=FakeLLM(fail_label="cnn"))

    materialization = next(
        event
        for event in failed.get_asset_materialization_events()
        if event.asset_key.path[-1] == "courseware_resolution_run"
    )
    assert materialization.materialization.metadata["unassigned_label_count"].value == 1
    churn = check_result(failed, "resolution_churn")
    assert churn.metadata["labels_in_both_runs"].value == 3


# ------------------------------------------------------------------- description
DESCRIPTION_INPUTS = "int__courseware__concept_description_inputs"


def description_inputs(
    concepts: dict[str, str] | None = None,
    approved_prompt_version: str | None = DESCRIPTION_PROMPT_VERSION,
) -> pl.DataFrame:
    """int__courseware__concept_description_inputs, one context row per concept."""
    concepts = concepts or {"c1": "auc", "c2": "bias"}
    return pl.DataFrame(
        {
            "concept_id": list(concepts),
            "label": list(concepts.values()),
            "concept_kind": ["concept"] * len(concepts),
            "description": [f"what {label} means" for label in concepts.values()],
            "course_title": ["Supervised Learning"] * len(concepts),
            "context_rank": [1] * len(concepts),
            "approved_prompt_version": [approved_prompt_version] * len(concepts),
        },
        schema_overrides={"approved_prompt_version": pl.String},
    )


def describe(
    tables, llm: LLMClientFactory | None = None, tags: dict[str, str] | None = None
):
    (hooked,) = with_failure_hooks([courseware_concept_description])
    return materialize(
        [hooked],
        resources={"io_manager": Warehouse(tables), "llm": llm or FakeLLM()},
        raise_on_error=False,
        tags=tags,
    )


def step_failure(result):
    (failure,) = [
        event.step_failure_data.user_failure_data
        for event in result.all_events
        if event.is_step_failure
    ]
    return failure


@pytest.mark.parametrize("approved", ["0ld0ld0ld0ld", None])
def test_description_refuses_a_prompt_that_is_not_approved(
    tables, calls, monkeypatch, approved
):
    tables[DESCRIPTION_INPUTS] = description_inputs(approved_prompt_version=approved)
    monkeypatch.setattr(description_module, "publish_prompt_version", _fail_publish)

    result = describe(tables)

    assert not result.success
    failure = step_failure(result)
    assert f"approves {approved or 'no version'}" in failure.description
    assert failure.metadata["retryable"].value is False
    # before Opik or the model
    assert calls == []
    assert "courseware_concept_description" not in tables


def test_description_refuses_inputs_built_before_the_approval_column(tables, calls):
    # The ml image deployed before lakehouse rebuilt the inputs with the seed.
    tables[DESCRIPTION_INPUTS] = description_inputs().drop("approved_prompt_version")

    result = describe(tables)

    assert not result.success
    assert "approves no version" in step_failure(result).description
    assert calls == []
    skipped = describe(tables, tags={AUTOMATION_CONDITION_TAG: "true"})
    assert skipped.success
    assert skipped.get_asset_materialization_events() == []
    assert "courseware_concept_description" not in tables


def test_description_stamps_the_prompt_version_on_rows_written_before_it(tables, calls):
    tables[DESCRIPTION_INPUTS] = description_inputs()
    assert describe(tables).success
    stored = tables["courseware_concept_description"]
    # c1 as written before rows carried a prompt_version, and c2 under an older
    # prompt, which this prompt's version cannot vouch for
    tables["courseware_concept_description"] = pl.concat(
        [
            stored.filter(pl.col("concept_id") == "c1"),
            stored.filter(pl.col("concept_id") == "c2").with_columns(
                description_version=pl.lit("0ld0ld0ld0ld")
            ),
        ]
    ).drop("prompt_version")
    calls.clear()

    assert describe(tables).success
    assert describe(tables).success

    # c2 is described again, c1 only copied, and neither twice
    assert calls == ["record_descriptions"]
    rows = tables["courseware_concept_description"]
    assert rows.select("concept_id", "prompt_version").rows() == [
        ("c1", None),
        ("c2", None),
        ("c1", DESCRIPTION_PROMPT_VERSION),
        ("c2", DESCRIPTION_PROMPT_VERSION),
    ]
    c1 = rows.filter(pl.col("concept_id") == "c1")
    assert c1["description"].n_unique() == 1
    assert c1["described_at"].n_unique() == 1


def test_automation_skips_a_description_prompt_that_is_not_approved(
    tables, calls, monkeypatch
):
    tables[DESCRIPTION_INPUTS] = description_inputs(
        approved_prompt_version="0ld0ld0ld0ld"
    )
    monkeypatch.setattr(description_module, "publish_prompt_version", _fail_publish)

    result = describe(tables, tags={AUTOMATION_CONDITION_TAG: "true"})

    # Succeeds without output, so automation neither retries it nor reports it.
    assert result.success
    assert result.get_asset_materialization_events() == []
    assert calls == []
    assert "courseware_concept_description" not in tables


def test_description_prompt_change_waits_for_promotion(tables, calls, monkeypatch):
    tables[DESCRIPTION_INPUTS] = description_inputs()
    assert describe(tables).success
    assert calls == ["record_descriptions"]

    edited = "e0e0e0e0e0e0"
    monkeypatch.setattr(description_module, "DESCRIPTION_PROMPT_VERSION", edited)
    monkeypatch.setattr(
        "ml.lib.courseware.describe.DESCRIPTION_PROMPT", "A different prompt."
    )
    calls.clear()

    assert not describe(tables).success
    assert calls == []
    assert tables["courseware_concept_description"].height == 2

    # Promoted: every concept is described under the new prompt, once.
    tables[DESCRIPTION_INPUTS] = description_inputs(approved_prompt_version=edited)
    assert describe(tables).success
    assert describe(tables).success
    assert calls == ["record_descriptions"]
    assert tables["courseware_concept_description"]["prompt_version"].to_list() == [
        DESCRIPTION_PROMPT_VERSION,
        DESCRIPTION_PROMPT_VERSION,
        edited,
        edited,
    ]


def automation_requests(
    instance: DagsterInstance, prompt_version: str, cursor=None
) -> tuple[int, Any]:
    """Run one automation tick with the description prompt at prompt_version.

    :returns: how many description runs the tick requested, and its cursor
    """
    # The asset as deployed with DESCRIPTION_PROMPT_VERSION at prompt_version. One
    # without a code version keeps none.
    deployed = courseware_concept_description.map_asset_specs(
        lambda spec: spec.replace_attributes(
            code_version=spec.code_version and prompt_version
        )
    )
    result = evaluate_automation_conditions(
        defs=Definitions(
            assets=[deployed, AssetSpec(description_module.INPUTS_KEY)],
            resources={"io_manager": Warehouse({}), "llm": FakeLLM()},
        ),
        instance=instance,
        asset_selection=AssetSelection.assets(courseware_concept_description),
        cursor=cursor,
    )
    return result.get_num_requested(courseware_concept_description.key), result.cursor


def build_inputs(instance: DagsterInstance, data_version: str) -> None:
    instance.report_runless_asset_event(
        AssetMaterialization(
            description_module.INPUTS_KEY, tags={DATA_VERSION_TAG: data_version}
        )
    )


@pytest.mark.parametrize("seed_first", [True, False])
def test_a_promotion_requests_a_run_whichever_image_deploys_last(seed_first):
    # The seed promotes in the lakehouse image and the prompt changes in the ml
    # image. The first deploy's run stops as unapproved and writes nothing, so the
    # second deploy has to request another run.
    instance = DagsterInstance.ephemeral()
    build_inputs(instance, "approves-old")
    instance.report_runless_asset_event(
        AssetMaterialization(courseware_concept_description.key)
    )
    prompt_version = "0ld0ld0ld0ld"
    _, cursor = automation_requests(instance, prompt_version)

    for deploy in ["seed", "ml"] if seed_first else ["ml", "seed"]:
        if deploy == "seed":
            build_inputs(instance, "approves-new")
        else:
            prompt_version = "e0e0e0e0e0e0"
        requested, cursor = automation_requests(instance, prompt_version, cursor)
        assert requested == 1, deploy
        requested, cursor = automation_requests(instance, prompt_version, cursor)
        assert requested == 0, deploy


def test_description_describes_only_concepts_new_at_the_approved_version(tables, calls):
    tables[DESCRIPTION_INPUTS] = description_inputs(concepts={"c1": "auc"})
    assert describe(tables).success
    tables[DESCRIPTION_INPUTS] = description_inputs()
    calls.clear()

    assert describe(tables).success

    assert calls == ["record_descriptions"]
    assert tables["courseware_concept_description"]["concept_id"].to_list() == [
        "c1",
        "c2",
    ]


def test_description_describes_inputs_that_changed_back_again(tables, calls):
    # a release takes the newest description, which is of the inputs in between
    for label in ("auc", "area under the curve", "auc"):
        tables[DESCRIPTION_INPUTS] = description_inputs(concepts={"c1": label})
        assert describe(tables).success

    assert calls == ["record_descriptions"] * 3
    newest = tables["courseware_concept_description"].sort("described_at")[-1]
    assert newest["input_checksum"].item() == next(
        iter(current_input_checksums(tables[DESCRIPTION_INPUTS]).values())
    )


def test_description_describes_each_concept_once(tables, calls):
    tables[DESCRIPTION_INPUTS] = description_inputs()
    resources = {"io_manager": Warehouse(tables), "llm": FakeLLM()}

    materialize([courseware_concept_description], resources=resources)
    second = materialize([courseware_concept_description], resources=resources)

    assert calls == ["record_descriptions"]
    # nothing new, so no output and no empty snapshot
    assert second.get_asset_materialization_events() == []
    assert tables["courseware_concept_description"]["concept_id"].to_list() == [
        "c1",
        "c2",
    ]


def test_description_stores_what_succeeded_and_fails_on_missing_ones(tables, calls):
    tables[DESCRIPTION_INPUTS] = description_inputs()
    instance = DagsterInstance.ephemeral()
    (hooked,) = with_failure_hooks([courseware_concept_description])

    failed = materialize(
        [hooked],
        instance=instance,
        resources={
            "io_manager": Warehouse(tables),
            "llm": FakeLLM(fail_label="bias"),
        },
        raise_on_error=False,
    )

    assert failure_class(failed) == "DescriptionFailure"
    run = instance.get_run_by_id(failed.run_id)
    assert run is not None
    assert run.tags[RETRY_ON_ASSET_OR_OP_FAILURE_TAG] == "false"
    assert tables["courseware_concept_description"]["concept_id"].to_list() == ["c1"]

    calls.clear()
    materialize(
        [courseware_concept_description],
        resources={"io_manager": Warehouse(tables), "llm": FakeLLM()},
    )

    assert calls == ["record_descriptions"]
    assert tables["courseware_concept_description"]["concept_id"].to_list() == [
        "c1",
        "c2",
    ]


# ------------------------------------------------------------------------- graph
def graph_input(tables, edges: dict[tuple[str, str], str]) -> None:
    """dim_concept a, b, c and passing builds_on edges with their review_status."""
    names = ["a", "b", "c"]
    tables["dim_concept"] = pl.DataFrame(
        {
            "concept_pk": names,
            "preferred_label": names,
            "lifecycle_status": ["active"] * 3,
            "label_count": [1] * 3,
            "resolution_run_id": ["res1"] * 3,
        }
    )
    tables["bridge_concept_relation"] = pl.DataFrame(
        [
            {
                "from_concept_fk": source,
                "to_concept_fk": target,
                "relation": "builds_on",
                "courserun_support": 2,
                "direction_consistency": 1.0,
                "co_taught_lift": None,
                "is_adjudicated": False,
                "has_co_assessed_evidence": False,
                "passes_support_rule": True,
                "review_status": review_status,
            }
            for (source, target), review_status in edges.items()
        ],
        schema_overrides={"co_taught_lift": pl.Float64},
    )


def test_graph_reports_cycles_and_skips_unchanged_input(tables, monkeypatch):
    graph_input(
        tables,
        {("a", "b"): "proposed", ("b", "a"): "proposed", ("b", "c"): "proposed"},
    )
    resources = {"io_manager": Warehouse(tables)}

    first = materialize([courseware_concept_graph], resources=resources)

    check = check_result(first, "graph_cycles")
    assert not check.passed
    assert check.metadata["sequence_cycle_concepts"].value == ["a", "b"]
    assert tables["courseware_concept_graph_run"].height == 1
    # a cycle among observed edges is a finding; nothing is approved, so the
    # closure is written empty
    assert tables["courseware_concept_graph_prerequisite_closure"].is_empty()

    second = materialize([courseware_concept_graph], resources=resources)

    assert tables["courseware_concept_graph_run"].height == 1
    assert not check_result(second, "graph_cycles").passed

    # a new algorithm version over the same input is a new run
    monkeypatch.setattr(graph, "GRAPH_VERSION", "courseware_concept_graph_test")
    materialize([courseware_concept_graph], resources=resources)

    assert tables["courseware_concept_graph_run"]["graph_version"].to_list() == [
        "courseware_concept_graph_v1",
        "courseware_concept_graph_test",
    ]

    # a new resolution run over the same graph input is a new run, bound to it
    tables["dim_concept"] = tables["dim_concept"].with_columns(
        resolution_run_id=pl.lit("res2")
    )
    materialize([courseware_concept_graph], resources=resources)

    assert tables["courseware_concept_graph_run"]["resolution_run_id"].to_list() == [
        "res1",
        "res1",
        "res2",
    ]


# ----------------------------------------------------------------------- release
def manifest(
    concept_count: int = 1,
    resource_count: int = 1,
    resource_concept_count: int = 1,
    **overrides: Any,
):
    """Return a coherent int__courseware__release_manifest row, with overrides."""
    row = {
        "resolution_run_id": "res1",
        "resolver_id": "courseware_resolution_v2",
        "resolution_prompt_version": "p",
        "sense_assignment_version": "s1",
        "graph_run_id": "g1",
        "graph_version": "courseware_concept_graph_v1",
        "graph_resolution_run_id": "res1",
        "is_graph_current": True,
        "description_prompt_version": "d1",
        "undescribed_concept_count": 0,
        "extraction_selection_checksum": "x",
        "extraction_count": 3,
        "source_policy_version": "pol",
        **overrides,
    }
    return pl.DataFrame([row], schema=release_module.MANIFEST_SCHEMA).with_columns(
        concept_count=pl.lit(concept_count, dtype=pl.Int64),
        resource_count=pl.lit(resource_count, dtype=pl.Int64),
        resource_concept_count=pl.lit(resource_concept_count, dtype=pl.Int64),
    )


def description_inputs_for(*names: str) -> pl.DataFrame:
    """int__courseware__concept_description_inputs rows, one per release concept."""
    return pl.DataFrame(
        {
            "concept_id": [f"id-{name}" for name in names],
            "label": list(names),
            "concept_kind": ["concept"] * len(names),
            "description": [f"about {name}" for name in names],
            "course_title": ["Course A"] * len(names),
            "context_rank": [1] * len(names),
        },
        schema_overrides={"concept_id": pl.String, "label": pl.String},
    )


def release_concepts(
    *names: str, review_status: str = "accepted", stale: tuple[str, ...] = ()
) -> pl.DataFrame:
    """int__courseware__release_concepts rows, described from their current inputs.

    :param stale: names whose description was written from inputs since changed
    """
    checksums = current_input_checksums(description_inputs_for(*names))
    return pl.DataFrame(
        {
            "concept_id": [f"id-{name}" for name in names],
            "name": list(names),
            "is_learner_facing": [True] * len(names),
            "review_status": [review_status] * len(names),
            "description_prompt_version": ["d1"] * len(names),
            "description_input_checksum": [
                "old" if name in stale else checksums[f"id-{name}"] for name in names
            ],
        },
        schema_overrides={
            "concept_id": pl.String,
            "name": pl.String,
            "description_input_checksum": pl.String,
        },
    )


def release_resources(*concepts: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "etl_source": ["mitxonline"],
            "readable_id": ["course-v1:X+A"],
            "concepts_taught": [list(concepts)],
            "context_document": [f"# Course A {' '.join(concepts)}"],
        }
    )


def release_resource_concepts(*names: str) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "etl_source": ["mitxonline"] * len(names),
            "readable_id": ["course-v1:X+A"] * len(names),
            "concept_id": [f"id-{name}" for name in names],
            "concept_name": list(names),
            "relation": ["teaches"] * len(names),
            "requirement_source": [None] * len(names),
        },
        schema_overrides={
            "concept_id": pl.String,
            "concept_name": pl.String,
            "requirement_source": pl.String,
        },
    )


def built_by(frame: pl.DataFrame, build: str) -> pl.DataFrame:
    """Return a candidate projection as dbt invocation build would write it."""
    return frame.with_columns(
        pl.lit(build, dtype=pl.String).alias(release_module.BUILD_COLUMN)
    )


def candidate(
    tables: dict[str, pl.DataFrame],
    *names: str,
    stale: tuple[str, ...] = (),
    build: str = "build1",
    **overrides,
) -> None:
    """Stage what one dbt invocation would build for the next release candidate.

    :param stale: names whose description inputs changed since they were described
    :param build: the dbt invocation id
    """
    tables["int__courseware__concept_description_inputs"] = description_inputs_for(
        *names
    )
    projections = {
        "int__courseware__release_concepts": release_concepts(*names, stale=stale),
        "int__courseware__release_resource_knowledge": release_resources(*names),
        "int__courseware__release_resource_concepts": release_resource_concepts(*names),
    }
    for table, frame in projections.items():
        tables[table] = built_by(frame, build)
    tables["int__courseware__release_manifest"] = manifest(
        concept_count=len(names),
        **{"resource_count": 1, "resource_concept_count": len(names), **overrides},
    ).with_columns(
        **{
            column: pl.lit(build if count else None, dtype=pl.String)
            for column, count in (
                ("concepts_invocation_id", len(names)),
                ("resources_invocation_id", 1),
                ("resource_concepts_invocation_id", len(names)),
            )
        }
    )


def release(tables: dict[str, pl.DataFrame], **publication) -> tuple[Any, Any]:
    """Build a release candidate, then run publication, as automation would."""
    resources = {"io_manager": Warehouse(tables)}
    built = materialize(
        [courseware_knowledge_release], resources=resources, raise_on_error=False
    )
    published = materialize(
        [courseware_release_publication],
        resources=resources,
        run_config={
            "ops": {
                "intermediate__courseware_release_publication": {"config": publication}
            }
        },
        raise_on_error=False,
    )
    return built, published


def published_rows(tables: dict[str, pl.DataFrame], table: str) -> pl.DataFrame:
    """Return what a Learn view selects: the rows of the published release."""
    pointer = tables["courseware_release_publication"].sort("published_at")
    return tables[table].filter(pl.col("release_id") == pointer["release_id"][-1])


def test_release_publishes_a_valid_candidate_and_skips_an_unchanged_one(tables):
    candidate(tables, "AUC")

    built, published = release(tables)

    assert check_result(built, "release_candidate_valid").passed
    assert published.success
    first = tables["courseware_release"].row(0, named=True)
    assert first["is_valid"]
    assert first["resolution_run_id"] == "res1"
    assert first["graph_run_id"] == "g1"
    assert published_rows(tables, "courseware_release_concept")["name"].to_list() == [
        "AUC"
    ]

    built, published = release(tables)

    assert check_result(built, "release_candidate_valid").passed
    assert tables["courseware_release"].height == 1
    assert tables["courseware_release_publication"].height == 1


def test_a_failed_graph_build_leaves_the_last_release_published(tables):
    candidate(tables, "AUC")
    release(tables)

    # A resolution run merged AUC into "area under the curve", and the graph build
    # after it failed, so the newest graph run is still over res1.
    candidate(
        tables,
        "area under the curve",
        resolution_run_id="res2",
        is_graph_current=False,
    )
    built, published = release(tables)

    check = check_result(built, "release_candidate_valid")
    assert not check.passed
    assert (
        "computed over resolution run res1, not res2"
        in (check.metadata["validation_errors"].value[0])
    )
    assert published.success
    assert tables["courseware_release"]["is_valid"].to_list() == [True, False]
    assert tables["courseware_release_publication"].height == 1
    for table, column, value in (
        ("courseware_release_concept", "name", "AUC"),
        ("courseware_release_resource_knowledge", "concepts_taught", ["AUC"]),
        ("courseware_release_resource_concept", "concept_id", "id-AUC"),
    ):
        assert published_rows(tables, table)[column].to_list() == [value]

    # The graph build succeeds: the next candidate moves all three projections.
    candidate(
        tables,
        "area under the curve",
        resolution_run_id="res2",
        graph_resolution_run_id="res2",
        graph_run_id="g2",
    )
    release(tables)

    pointer = tables["courseware_release_publication"]
    assert pointer.height == 2
    assert pointer["previous_release_id"][-1] == pointer["release_id"][0]
    for table, column, value in (
        ("courseware_release_concept", "name", "area under the curve"),
        (
            "courseware_release_resource_knowledge",
            "concepts_taught",
            ["area under the curve"],
        ),
        (
            "courseware_release_resource_concept",
            "concept_id",
            "id-area under the curve",
        ),
    ):
        assert published_rows(tables, table)[column].to_list() == [value]


@pytest.mark.parametrize(
    ("overrides", "names", "error"),
    [
        ({"description_prompt_version": None}, ("AUC",), "approves no description"),
        ({"undescribed_concept_count": 2}, ("AUC",), "2 concepts have no description"),
        ({}, (), "would empty the published release"),
        ({"resolution_run_id": None}, ("AUC",), "no resolution run"),
        (
            {"sense_assignment_version": None},
            ("AUC",),
            "resolution run res1 has no sense assignment",
        ),
    ],
)
def test_validation_refuses_an_incomplete_candidate(overrides, names, error):
    errors = release_module.validate_candidate(
        manifest(**overrides).row(0, named=True),
        release_concepts(*names),
        current_input_checksums(description_inputs_for(*names)),
        "pol",
    )

    assert any(error in reason for reason in errors), errors


def test_an_empty_candidate_writes_nothing_until_a_release_is_published(tables):
    candidate(tables)

    built, published = release(tables)

    assert check_result(built, "release_candidate_valid").passed
    assert published.success
    assert "courseware_release" not in tables

    candidate(tables, "AUC")
    release(tables)
    candidate(tables)
    built, _ = release(tables)

    # once a release is published, an empty candidate would empty it
    assert not check_result(built, "release_candidate_valid").passed
    assert tables["courseware_release"]["is_valid"].to_list() == [True, False]
    assert published_rows(tables, "courseware_release_concept")["name"].to_list() == [
        "AUC"
    ]


def test_validation_refuses_a_concept_that_is_not_accepted():
    errors = release_module.validate_candidate(
        manifest().row(0, named=True),
        release_concepts("AUC", review_status="proposed"),
        current_input_checksums(description_inputs_for("AUC")),
        "pol",
    )

    assert errors == ["1 concepts are not accepted"]


def test_a_rollback_holds_until_a_new_release_is_built(tables):
    candidate(tables, "AUC")
    release(tables)
    candidate(tables, "AUC", "pandas", extraction_count=4)
    release(tables)
    first, second = tables["courseware_release"]["release_id"].to_list()
    assert tables["courseware_release_publication"]["release_id"].to_list() == [
        first,
        second,
    ]

    _, published = release(tables, release_id=first)

    assert published.success
    pointer = tables["courseware_release_publication"]
    assert pointer["release_id"][-1] == first
    assert pointer["publication_kind"][-1] == "requested"

    # automation does not undo the rollback by republishing the newer release
    release(tables)
    assert tables["courseware_release_publication"]["release_id"][-1] == first

    candidate(tables, "AUC", "pandas", "numpy", extraction_count=5)
    release(tables)
    third = tables["courseware_release"]["release_id"][-1]
    assert tables["courseware_release_publication"]["release_id"][-1] == third


def test_a_rollback_does_not_publish_a_release_it_skipped(tables):
    candidate(tables, "AUC")
    release(tables)
    first = tables["courseware_release"]["release_id"][0]
    # r2 and r3 are built before publication runs again, which takes r3
    candidate(tables, "AUC", "pandas", extraction_count=4)
    materialize(
        [courseware_knowledge_release], resources={"io_manager": Warehouse(tables)}
    )
    candidate(tables, "AUC", "pandas", "numpy", extraction_count=5)
    release(tables)
    _, skipped, third = tables["courseware_release"]["release_id"].to_list()
    assert tables["courseware_release_publication"]["release_id"].to_list() == [
        first,
        third,
    ]

    release(tables, release_id=first)
    release(tables)

    # r2 was never published, but it is older than r3, so it does not undo the
    # rollback
    published = tables["courseware_release_publication"]["release_id"].to_list()
    assert published == [first, third, first]
    assert skipped not in published


def test_publication_refuses_an_invalid_release(tables):
    candidate(tables, "AUC", is_graph_current=False)
    release(tables)
    invalid = tables["courseware_release"]["release_id"][0]
    assert "courseware_release_publication" not in tables

    _, published = release(tables, release_id=invalid)

    assert not published.success
    assert failure_class(published) == "PermanentFailure"
    assert "courseware_release_publication" not in tables


def test_a_description_from_changed_inputs_is_refused(tables):
    candidate(tables, "AUC")
    release(tables)

    # a run AUC's description read was made ineligible, so its inputs changed
    candidate(tables, "AUC", "pandas", stale=("AUC",), source_policy_version="pol2")
    built, _ = release(tables)

    check = check_result(built, "release_candidate_valid")
    assert not check.passed
    assert check.metadata["validation_errors"].value == [
        "1 concepts' descriptions predate their current inputs or have none"
    ]
    assert published_rows(tables, "courseware_release_concept")["name"].to_list() == [
        "AUC"
    ]


def test_a_policy_change_may_empty_the_release_and_blocks_older_rollbacks(tables):
    candidate(tables, "AUC")
    release(tables)
    first = tables["courseware_release"]["release_id"][0]

    # the only run naming AUC is made ineligible: the empty candidate retracts it
    candidate(tables, source_policy_version="pol2", resource_count=0)
    tables["int__courseware__release_resource_knowledge"] = built_by(
        release_resources().head(0), "build1"
    )
    tables["int__courseware__release_manifest"] = tables[
        "int__courseware__release_manifest"
    ].with_columns(resources_invocation_id=pl.lit(None, dtype=pl.String))
    built, _ = release(tables)

    assert check_result(built, "release_candidate_valid").passed
    assert published_rows(tables, "courseware_release_concept").height == 0

    # rolling back to the release built under the old policy is refused
    _, published = release(tables, release_id=first)

    assert not published.success
    assert failure_class(published) == "PermanentFailure"


@pytest.mark.parametrize(
    "rebuilt",
    [
        # the concepts were rebuilt after the manifest, the resource model failed
        ("AUC", "pandas"),
        # the same count: AUC was merged into another concept, and the resource
        # projections still name AUC
        ("area under the curve",),
    ],
)
def test_a_projection_from_another_dbt_build_is_not_released(tables, rebuilt):
    candidate(tables, "AUC")
    tables["int__courseware__release_concepts"] = built_by(
        release_concepts(*rebuilt), "build2"
    )

    built, _ = release(tables)

    assert not built.success
    assert "not the dbt build the manifest read" in str(
        next(e for e in built.all_events if e.is_step_failure).step_failure_data.error
    )
    assert "courseware_release" not in tables


def test_a_release_carries_no_dbt_invocation(tables):
    candidate(tables, "AUC")
    release(tables)
    candidate(tables, "AUC", build="build2")

    release(tables)

    # a rebuild with the same rows is the same candidate
    assert tables["courseware_release"].height == 1
    for table in release_module.PROJECTIONS:
        assert release_module.BUILD_COLUMN not in tables[table].columns


def test_validation_refuses_a_description_of_a_concept_without_inputs():
    errors = release_module.validate_candidate(
        manifest().row(0, named=True),
        release_concepts("AUC"),
        current_input_checksums(description_inputs_for()),
        "pol",
    )

    assert errors == [
        "1 concepts' descriptions predate their current inputs or have none"
    ]


def test_graph_writes_the_closure_of_approved_edges_and_fails_on_an_approved_cycle(
    tables,
):
    graph_input(
        tables,
        {("a", "b"): "accepted", ("b", "c"): "accepted", ("c", "a"): "proposed"},
    )
    resources = {"io_manager": Warehouse(tables)}

    materialize([courseware_concept_graph], resources=resources)

    closure = tables["courseware_concept_graph_prerequisite_closure"]
    assert sorted(
        closure.select("from_concept_id", "to_concept_id", "min_depth").rows()
    ) == [("a", "b", 1), ("a", "c", 2), ("b", "c", 1)]
    run_id = tables["courseware_concept_graph_run"]["graph_run_id"][0]
    assert closure["graph_run_id"].unique().to_list() == [run_id]
    # every approved edge joins active concepts, so the run excludes none
    assert tables["courseware_concept_graph_run"][
        "prerequisite_closure_excluded_json"
    ].to_list() == ["[]"]

    # approving c -> a closes a cycle: the run fails permanently and writes nothing,
    # so the previous closure stays the newest
    graph_input(
        tables,
        {("a", "b"): "accepted", ("b", "c"): "accepted", ("c", "a"): "accepted"},
    )
    instance = DagsterInstance.ephemeral()
    (hooked,) = with_failure_hooks([courseware_concept_graph])
    failed = materialize(
        [hooked], resources=resources, instance=instance, raise_on_error=False
    )

    assert failure_class(failed) == "ApprovedPrerequisiteCycle"
    run = instance.get_run_by_id(failed.run_id)
    assert run is not None
    assert run.tags[RETRY_ON_ASSET_OR_OP_FAILURE_TAG] == "false"
    assert tables["courseware_concept_graph_run"].height == 1
    assert tables["courseware_concept_graph_prerequisite_closure"].height == 3


def test_courseware_definitions_load():
    Definitions(
        assets=[
            courseware_extraction_text,
            courseware_extraction,
            courseware_resolution,
            courseware_concept_description,
            courseware_concept_graph,
            courseware_knowledge_release,
            courseware_release_publication,
        ],
        sensors=[courseware_extraction_text_sensor],
        resources={
            "io_manager": Warehouse({}),
            "courseware_io_manager": Warehouse({}),
            "llm": FakeLLM(),
        },
    ).resolve_all_job_defs()

"""Dagster asset check results and freshness states as OpenMetadata test cases.

OpenMetadata's Dagster connector ingests jobs, runs and lineage but not asset
checks or freshness policies, so the freshness and schema-change signals on the
dbt models were invisible there. ``asset_check_openmetadata_sensor`` pushes
each check evaluation as a result of a test case on the model's table, and
``freshness_openmetadata_sensor`` each freshness policy state change.

Every pushed test case uses one test definition, ``dagsterAssetCheck``, whose
test platform is ``Other``, which tells them apart from dbt's test cases
(platform ``dbt``) in the UI. OpenMetadata 2.0.5's alert filters cannot match
on either, and Dagster's asset_check_failure_sensor already notifies for these
checks, so an OpenMetadata alert on test case failures will include them unless
they are written by a bot of their own (``matchUpdatedBy``).

dbt tests are not pushed. dagster-dbt turns each one into an asset check too,
but they already reach OpenMetadata through run_results.json, and pushing them
again would duplicate every dbt test case.
"""

import logging
import time
from collections.abc import Mapping, Sequence
from http import HTTPStatus
from typing import Any, NamedTuple
from urllib.parse import quote

import httpx2 as httpx
from dagster import AssetCheckEvaluation, AssetKey
from dagster_dbt import DagsterDbtTranslator
from ol_orchestrate.resources.openmetadata import OpenMetadataApiClient
from ol_orchestrate.sensors.failure_notification import (
    DBT_PROVENANCE_KEYS,
    format_check_metadata,
)

# The service OpenMetadata files the Trino warehouse under. The StarRocks
# migration renames it, which changes every table FQN pushed here.
TRINO_SERVICE = "Starburst Galaxy"

TEST_DEFINITION_NAME = "dagsterAssetCheck"

# The check name build_last_update_freshness_checks gave the freshness checks,
# which freshness policy state changes now write their results under.
FRESHNESS_CHECK_NAME = "freshness_check"

# OpenMetadata testCaseStatus for each FreshnessState value that has one.
FRESHNESS_TEST_CASE_STATUS = {"PASS": "Success", "WARN": "Failed", "FAIL": "Failed"}

# Created when missing and never updated after, so disabling it in OpenMetadata
# sticks.
TEST_DEFINITION_REQUEST: dict[str, Any] = {
    "name": TEST_DEFINITION_NAME,
    "displayName": "Dagster asset check",
    "description": (
        "A Dagster asset check, evaluated in Dagster and pushed here by the "
        "lakehouse code location's asset_check_openmetadata_sensor. The check's "
        "name is the test case name after the `dagster_` prefix."
    ),
    "entityType": "TABLE",
    "testPlatforms": ["Other"],
    "parameterDefinition": [],
}

_TABLE_RESOURCE_TYPES = frozenset({"model", "seed", "snapshot"})


def table_fqns_by_asset_key(
    manifest: Mapping[str, Any],
    translator: DagsterDbtTranslator,
    asset_keys: set[AssetKey],
    service: str,
) -> dict[AssetKey, str]:
    """OpenMetadata table FQN for each dbt asset in *asset_keys*.

    :param manifest: the dbt manifest the assets were built from. Its relations
        must be the ones OpenMetadata catalogs (production's).
    :param translator: the translator the assets were built with, so the keys
        match.
    :param asset_keys: the assets to map, e.g. one ``@dbt_assets`` definition's
        keys. Nodes outside it (StarRocks models, which live under another
        OpenMetadata service) are left out.
    :param service: the OpenMetadata database service the relations live under.
    :returns: ``service.database.schema.table`` by asset key.
    :rtype: dict[AssetKey, str]
    """
    fqns = {}
    for node in manifest["nodes"].values():
        if node["resource_type"] not in _TABLE_RESOURCE_TYPES:
            continue
        key = translator.get_asset_key(node)
        if key in asset_keys:
            fqns[key] = ".".join(
                (service, node["database"], node["schema"], node["alias"])
            )
    return fqns


def test_case_request(check_name: str, table_fqn: str) -> dict[str, Any]:
    """``CreateTestCase`` body for POST /v1/dataQuality/testCases.

    The ``dagster_`` prefix keeps a check from sharing a name with a dbt test
    case on the same table.
    """
    return {
        "name": f"dagster_{check_name}",
        "description": f"Dagster asset check `{check_name}`.",
        "testDefinition": TEST_DEFINITION_NAME,
        "entityLink": f"<#E::table::{table_fqn}>",
        "parameterValues": [],
    }


def test_case_result_request(
    evaluation: AssetCheckEvaluation, timestamp_ms: int
) -> dict[str, Any]:
    """``CreateTestCaseResult`` body for POST .../testCases/testCaseResults/{fqn}.

    OpenMetadata results have no severity, so a failed WARN-severity check
    (e.g. a column schema change) is Failed like an ERROR one, with the severity
    named in the result text. The text also carries the check's description and
    the same bounded slice of its metadata the Slack failure message shows.
    """
    outcome = "passed" if evaluation.passed else f"failed ({evaluation.severity.value})"
    lines = [f"Dagster check {outcome}."]
    if evaluation.description:
        lines.append(evaluation.description)
    if metadata := format_check_metadata(evaluation.metadata):
        lines.append(metadata)
    return {
        "timestamp": timestamp_ms,
        "testCaseStatus": "Success" if evaluation.passed else "Failed",
        "result": "\n".join(lines),
        "testResultValue": [],
    }


def push_check_evaluations(
    records: Sequence[Any],
    table_fqns: Mapping[AssetKey, str],
    client: OpenMetadataApiClient,
    log: logging.Logger,
    deadline: float,
) -> tuple[int, int]:
    """Write each check evaluation in *records* to OpenMetadata as a test case result.

    Evaluations of dbt tests and of assets with no table in *table_fqns* are
    skipped. So is a table OpenMetadata has not ingested yet (a 404), with a
    warning; any other API error is raised, so the sensor tick fails and the
    batch is retried. A retry re-posts the results written before the error,
    which OpenMetadata refuses with a 409 and this counts as written.

    OpenMetadata writes are slow (a production tick on 2026-10-20 took 35
    seconds for 12 results), so a batch can hold more pushable evaluations than
    fit in a sensor tick. Once ``time.monotonic()``
    passes *deadline*, no further write is started and the rest of the batch is
    left for the next tick. The first record is always handled, so every tick
    makes progress.

    :param records: asset check evaluation event log records, oldest first.
    :param deadline: a ``time.monotonic()`` value.
    :returns: how many results were written, and the storage id of the last
        record handled, which is where the sensor's cursor goes.
    :rtype: tuple[int, int]
    """

    def read(record: Any) -> _TestCaseResult | None:
        evaluation = record.event_log_entry.dagster_event.event_specific_data
        if set(evaluation.metadata or {}) >= DBT_PROVENANCE_KEYS:
            return None
        return _TestCaseResult(
            evaluation.asset_key,
            evaluation.check_name,
            test_case_result_request(
                evaluation, int(record.event_log_entry.timestamp * 1000)
            ),
        )

    return _push_results(
        [(record.storage_id, read(record)) for record in records],
        table_fqns,
        client,
        log,
        deadline,
    )


def freshness_test_case_result_request(
    change: Any, timestamp_ms: int
) -> dict[str, Any] | None:
    """``CreateTestCaseResult`` body for a freshness state change, or None.

    PASS is Success and FAIL and WARN are Failed, as a failed WARN-severity
    check is. UNKNOWN (never materialized) and NOT_APPLICABLE (policy removed)
    say nothing about freshness, so they write no result.

    :param change: a ``FreshnessStateChange``.
    """
    return _freshness_result(
        change.new_state.value,
        timestamp_ms,
        f"(was {change.previous_state.value}).",
    )


def _freshness_result(
    state: str, timestamp_ms: int, detail: str
) -> dict[str, Any] | None:
    status = FRESHNESS_TEST_CASE_STATUS.get(state)
    if status is None:
        return None
    return {
        "timestamp": timestamp_ms,
        "testCaseStatus": status,
        "result": f"Dagster freshness policy {state} {detail}",
        "testResultValue": [],
    }


def reconcile_freshness_test_cases(
    states: Mapping[AssetKey, Any],
    table_fqns: Mapping[AssetKey, str],
    client: OpenMetadataApiClient,
    log: logging.Logger,
) -> int:
    """Give each table that lacks a freshness test case one, with its current state.

    push_freshness_changes skips a change on a table OpenMetadata has not
    ingested yet, and the daemon does not repeat a state, so a new model's
    first state (written right after its first build, usually before
    OpenMetadata catalogs the table) would otherwise never arrive. A table that
    has the test case already gets every change through that sensor, so it is
    left alone. A table OpenMetadata still lacks is skipped with a warning and
    tried again on the next run.

    :param states: each asset's current ``FreshnessStateRecord``.
    :returns: how many test cases were created.
    :rtype: int
    """
    created = 0
    defined = False
    for key, record in states.items():
        if key not in table_fqns:
            continue
        body = _freshness_result(
            record.freshness_state.value,
            int(record.updated_at.timestamp() * 1000),
            "(current state, written when the test case was created).",
        )
        if body is None:
            continue
        table_fqn = table_fqns[key]
        name = test_case_request(FRESHNESS_CHECK_NAME, table_fqn)["name"]
        if _exists(
            client,
            "v1/dataQuality/testCases/name/" + quote(f"{table_fqn}.{name}", safe=""),
        ):
            continue
        if not defined:
            _ensure_test_definition(client)
            defined = True
        test_case_fqn = _ensure_test_case(client, FRESHNESS_CHECK_NAME, table_fqn, log)
        if test_case_fqn is not None:
            _post_result(client, test_case_fqn, body)
            created += 1
    return created


def push_freshness_changes(
    records: Sequence[Any],
    table_fqns: Mapping[AssetKey, str],
    client: OpenMetadataApiClient,
    log: logging.Logger,
    deadline: float,
) -> tuple[int, int]:
    """Write each freshness state change in *records* as a freshness test case result.

    Results go to the ``dagster_freshness_check`` test case, the one the
    last-update freshness checks these policies replaced wrote to, so each
    table's freshness history continues where the checks left it. Otherwise as
    push_check_evaluations.

    :param records: ``FRESHNESS_STATE_CHANGE`` event log records, oldest first.
    """

    def read(record: Any) -> _TestCaseResult | None:
        change = record.event_log_entry.dagster_event.event_specific_data
        body = freshness_test_case_result_request(
            change, int(change.state_change_timestamp * 1000)
        )
        if body is None:
            return None
        return _TestCaseResult(change.key, FRESHNESS_CHECK_NAME, body)

    return _push_results(
        [(record.storage_id, read(record)) for record in records],
        table_fqns,
        client,
        log,
        deadline,
    )


class _TestCaseResult(NamedTuple):
    asset_key: AssetKey
    check_name: str
    body: dict[str, Any]


def _push_results(
    results: Sequence[tuple[int, _TestCaseResult | None]],
    table_fqns: Mapping[AssetKey, str],
    client: OpenMetadataApiClient,
    log: logging.Logger,
    deadline: float,
) -> tuple[int, int]:
    """Write each result to its test case; see push_check_evaluations.

    :param results: each record's storage id, oldest first, with its result,
        or None for a record that writes nothing.
    """
    test_case_fqns: dict[tuple[AssetKey, str], str | None] = {}
    pushed = 0
    first = results[0][0]
    handled = first - 1
    for storage_id, result in results:
        if result is None or result.asset_key not in table_fqns:
            handled = storage_id
            continue
        # Gated on any record handled, not on a result written: a batch of checks
        # on tables OpenMetadata lacks writes nothing but still costs two
        # requests per check.
        if handled >= first and time.monotonic() > deadline:
            break
        if not test_case_fqns:
            _ensure_test_definition(client)
        key = (result.asset_key, result.check_name)
        if key not in test_case_fqns:
            test_case_fqns[key] = _ensure_test_case(
                client, result.check_name, table_fqns[result.asset_key], log
            )
        if (test_case_fqn := test_case_fqns[key]) is not None:
            _post_result(client, test_case_fqn, result.body)
            pushed += 1
        handled = storage_id
    return pushed, handled


def _ensure_test_definition(client: OpenMetadataApiClient) -> None:
    if not _exists(
        client, f"v1/dataQuality/testDefinitions/name/{TEST_DEFINITION_NAME}"
    ):
        client.send("PUT", "v1/dataQuality/testDefinitions", TEST_DEFINITION_REQUEST)


def _post_result(
    client: OpenMetadataApiClient, test_case_fqn: str, result: dict[str, Any]
) -> None:
    try:
        client.send(
            "POST",
            f"v1/dataQuality/testCases/testCaseResults/{quote(test_case_fqn, safe='')}",
            result,
        )
    except httpx.HTTPStatusError as exc:
        # A result is unique on (test case, timestamp), so a 409 is this
        # evaluation already written by a tick that failed later in its batch.
        if exc.response.status_code != HTTPStatus.CONFLICT:
            raise


def _exists(client: OpenMetadataApiClient, endpoint: str) -> bool:
    try:
        client.send("GET", endpoint)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != HTTPStatus.NOT_FOUND:
            raise
        return False
    return True


def _ensure_test_case(
    client: OpenMetadataApiClient,
    check_name: str,
    table_fqn: str,
    log: logging.Logger,
) -> str | None:
    """Create the check's test case unless it exists, and return its FQN.

    Created only when missing, not upserted, so an edit made in OpenMetadata
    (a description, an owner) is not overwritten every tick. None when
    OpenMetadata has no such table.
    """
    request = test_case_request(check_name, table_fqn)
    test_case_fqn = f"{table_fqn}.{request['name']}"
    if _exists(
        client, f"v1/dataQuality/testCases/name/{quote(test_case_fqn, safe='')}"
    ):
        return test_case_fqn
    try:
        client.send("POST", "v1/dataQuality/testCases", request)
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != HTTPStatus.NOT_FOUND:
            raise
        log.warning(
            "OpenMetadata has no table %s, so check %s is not pushed",
            table_fqn,
            check_name,
        )
        return None
    return test_case_fqn

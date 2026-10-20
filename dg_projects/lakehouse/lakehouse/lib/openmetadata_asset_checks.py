"""Dagster asset check results as OpenMetadata test cases.

OpenMetadata's Dagster connector ingests jobs, runs and lineage but not asset
checks, so the freshness and schema-change checks on the dbt models were
invisible there. ``asset_check_openmetadata_sensor`` pushes each evaluation as
a result of a test case on the model's table.

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
from collections.abc import Mapping, Sequence
from http import HTTPStatus
from typing import Any
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


def test_case_request(
    evaluation: AssetCheckEvaluation, table_fqn: str
) -> dict[str, Any]:
    """``CreateTestCase`` body for POST /v1/dataQuality/testCases.

    The ``dagster_`` prefix keeps a check from sharing a name with a dbt test
    case on the same table.
    """
    return {
        "name": f"dagster_{evaluation.check_name}",
        "description": f"Dagster asset check `{evaluation.check_name}`.",
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
) -> int:
    """Write each check evaluation in *records* to OpenMetadata as a test case result.

    Evaluations of dbt tests and of assets with no table in *table_fqns* are
    skipped. So is a table OpenMetadata has not ingested yet (a 404), with a
    warning; any other API error is raised, so the sensor tick fails and the
    batch is retried. A retry re-posts the results written before the error,
    which OpenMetadata refuses with a 409 and this counts as written.

    :param records: asset check evaluation event log records, oldest first.
    :returns: how many results were written.
    :rtype: int
    """
    evaluations = [
        (
            record.event_log_entry,
            record.event_log_entry.dagster_event.event_specific_data,
        )
        for record in records
    ]
    pushable = [
        (entry, evaluation, table_fqns[evaluation.asset_key])
        for entry, evaluation in evaluations
        if evaluation.asset_key in table_fqns
        and not set(evaluation.metadata or {}) >= DBT_PROVENANCE_KEYS
    ]
    if not pushable:
        return 0
    if not _exists(
        client, f"v1/dataQuality/testDefinitions/name/{TEST_DEFINITION_NAME}"
    ):
        client.send("PUT", "v1/dataQuality/testDefinitions", TEST_DEFINITION_REQUEST)
    test_case_fqns: dict[tuple[AssetKey, str], str | None] = {}
    pushed = 0
    for entry, evaluation, table_fqn in pushable:
        key = (evaluation.asset_key, evaluation.check_name)
        if key not in test_case_fqns:
            test_case_fqns[key] = _ensure_test_case(client, evaluation, table_fqn, log)
        test_case_fqn = test_case_fqns[key]
        if test_case_fqn is None:
            continue
        try:
            client.send(
                "POST",
                "v1/dataQuality/testCases/testCaseResults/"
                f"{quote(test_case_fqn, safe='')}",
                test_case_result_request(evaluation, int(entry.timestamp * 1000)),
            )
        except httpx.HTTPStatusError as exc:
            # A result is unique on (test case, timestamp), so a 409 is this
            # evaluation already written by a tick that failed later in its batch.
            if exc.response.status_code != HTTPStatus.CONFLICT:
                raise
        pushed += 1
    return pushed


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
    evaluation: AssetCheckEvaluation,
    table_fqn: str,
    log: logging.Logger,
) -> str | None:
    """Create the check's test case unless it exists, and return its FQN.

    Created only when missing, not upserted, so an edit made in OpenMetadata
    (a description, an owner) is not overwritten every tick. None when
    OpenMetadata has no such table.
    """
    request = test_case_request(evaluation, table_fqn)
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
            evaluation.check_name,
        )
        return None
    return test_case_fqn

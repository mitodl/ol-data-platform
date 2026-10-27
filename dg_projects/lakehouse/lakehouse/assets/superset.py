from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

import httpx2 as httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    Failure,
    MaterializeResult,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes

from lakehouse.lib.dbt_environment import DBT_AUTOMATION_ENABLED
from lakehouse.resources.superset_api import SupersetApiClient, SupersetApiClientFactory

_DEFAULT_SCHEMA_BASE = "ol_warehouse_production"

# What one dataset's Superset calls can raise: transport and HTTP status errors,
# RuntimeError from a token fetch or create the client rejects, and ValueError
# (JSONDecodeError included) from a response body that is not JSON. A token can
# expire partway through a database's datasets, since one client serves them all.
_SUPERSET_CALL_ERRORS = (httpx.HTTPError, RuntimeError, ValueError)

# Fewer failures than this cannot tell a Superset outage from one broken table.
_MIN_FAILURES_TO_RAISE = 2


def _validate_superset_database_config(database_id: int, database_name: str) -> None:
    """Validate that database_id and database_name are consistent.

    Raises ValueError if a non-trino database_name is paired with the default
    Trino database_id (1), which would silently register the dataset under the
    wrong database.

    Cross-database ID collision (e.g. TRINO_SUPERSET_DATABASE_ID accidentally
    set to a StarRocks ID) is guarded at the call site in definitions.py where
    both IDs are available for comparison.
    """
    if database_name != "trino" and database_id == 1:
        msg = (
            f"database_name={database_name!r} requires an explicit database_id. "
            "Received database_id=1, which is the default reserved for Trino."
        )
        raise ValueError(msg)


def superset_dataset_keys(
    asset_keys: Iterable[AssetKey],
    manifest: Mapping[str, Any],
    translator: DagsterDbtTranslator,
    layers: Collection[str],
) -> list[AssetKey]:
    """Return the dbt model keys to build Superset datasets for.

    A model in one of layers gets a dataset unless its dbt meta sets
    ``superset_dataset: false``, e.g. a model holding verbatim course text that no
    source-visibility policy covers yet.

    :param asset_keys: the dbt project's asset keys
    :param manifest: the dbt manifest the keys come from
    :param translator: the translator that keyed the manifest's nodes
    :param layers: the first key segments (dbt layers) that get datasets
    :returns: the keys to build datasets for
    :rtype: list[AssetKey]
    """
    opted_out = {
        translator.get_asset_key(node)
        for node in manifest["nodes"].values()
        if node["resource_type"] == "model"
        and node["config"].get("meta", {}).get("superset_dataset") is False
    }
    return [key for key in asset_keys if key.path[0] in layers and key not in opted_out]


def _dataset_key(database_name: str, dbt_model_name: str) -> AssetKey:
    if database_name == "trino":
        return AssetKey(("superset", "dataset", dbt_model_name))
    return AssetKey(("superset", database_name, "dataset", dbt_model_name))


def _refresh_dataset(  # noqa: PLR0913
    context: AssetExecutionContext,
    client: SupersetApiClient,
    dbt_asset_group_name: str,
    dbt_model_name: str,
    database_id: int,
    database_name: str,
    schema_base: str,
) -> dict[str, Any]:
    """Find or create one dataset and refresh its columns.

    :returns: The materialization metadata, whose ``status`` is ``refreshed``,
        ``skipped`` or ``error``.
    :rtype: dict[str, Any]
    """
    model_metadata = {
        "dbt_asset_group_name": dbt_asset_group_name,
        "dbt_model_name": dbt_model_name,
    }
    # StarRocks (and most non-Trino databases) normalize identifiers to
    # lowercase. Trino preserves the original case from the dbt model name.
    table_name = dbt_model_name if database_name == "trino" else dbt_model_name.lower()
    # A dataset that Superset can't resolve or create is reported as an error
    # materialization rather than raised: every dataset for a database shares
    # one step, so a single unusable dataset would otherwise fail all of them.
    try:
        dataset_id = client.get_or_create_dataset(
            schema_suffix=dbt_asset_group_name,
            table_name=table_name,
            database_id=database_id,
            schema_base=schema_base,
        )
    except _SUPERSET_CALL_ERRORS as e:
        context.log.exception(
            "Failed to get or create dataset for %s.%s",
            dbt_asset_group_name,
            dbt_model_name,
        )
        return {"status": "error", "error": str(e), **model_metadata}

    if dataset_id is None:
        context.log.warning(
            "Dataset ID not found for %s.%s. Skipping refresh.",
            dbt_asset_group_name,
            dbt_model_name,
        )
        return {
            "status": "skipped",
            "reason": "Superset Dataset ID not found",
            **model_metadata,
        }

    try:
        client.refresh_dataset(int(dataset_id))
        timestamp = datetime.now(UTC).isoformat()
        # Touch dataset metadata (updates changed_on)
        client.update_dataset(
            dataset_id=int(dataset_id),
            payload={"description": f"Auto refreshed at {timestamp}"},
        )
    except _SUPERSET_CALL_ERRORS as e:
        context.log.exception(
            "Failed to refresh dataset for %s.%s",
            dbt_asset_group_name,
            dbt_model_name,
        )
        return {"status": "error", "error": str(e), **model_metadata}

    context.log.info(
        "Successfully refreshed dataset: %s.%s",
        dbt_asset_group_name,
        dbt_model_name,
    )
    return {
        "status": "refreshed",
        "superset_dataset_id": dataset_id,
        **model_metadata,
    }


def build_superset_dataset_assets(
    dbt_model_keys: Sequence[AssetKey],
    database_id: int = 1,
    database_name: str = "trino",
    schema_base: str = _DEFAULT_SCHEMA_BASE,
) -> AssetsDefinition:
    """Build one subsettable asset definition for a database's Superset datasets.

    Every dataset is an asset of its own (``superset/dataset/<model>`` for
    Trino, ``superset/<database>/dataset/<model>`` otherwise), but they share a
    single step. One step per dataset made a full dbt run fan out to 228
    steps the moment the dbt step finished, each a subprocess importing the
    code location. Lakehouse run workers died about 40 seconds into that
    fan-out on 2026-10-12, with memory climbing toward the pod limit.

    :param dbt_model_keys: The dbt model asset keys to publish, as
        ``(<dbt group>, <model name>)``.
    :param database_id: The Superset database to register the datasets in.
    :param database_name: The engine behind ``database_id``.
    :param schema_base: The schema prefix the dbt group is appended to.
    :returns: The multi-asset publishing one dataset per dbt model.
    :rtype: AssetsDefinition
    """
    _validate_superset_database_config(database_id, database_name)

    dbt_key_by_dataset_key = {
        _dataset_key(database_name, dbt_key.path[1]): dbt_key
        for dbt_key in dbt_model_keys
    }
    group_name = (
        "superset_dataset"
        if database_name == "trino"
        else f"superset_{database_name}_dataset"
    )

    @multi_asset(
        name=f"{group_name}s",
        specs=[
            AssetSpec(
                key=dataset_key,
                deps=[dbt_key],
                group_name=group_name,
                # Gated with the dbt assets these follow: dbt_automation_sensor
                # targets both, so leaving these conditions live would let a
                # sensor started by hand in an automation-off environment still
                # fire this half.
                automation_condition=(
                    upstream_or_code_changes() if DBT_AUTOMATION_ENABLED else None
                ),
            )
            for dataset_key, dbt_key in dbt_key_by_dataset_key.items()
        ],
        # A run selects only the datasets whose dbt models it built.
        can_subset=True,
    )
    def _superset_datasets(
        context: AssetExecutionContext,
        superset_api: SupersetApiClientFactory,
    ) -> Iterator[MaterializeResult[Any]]:
        errors: list[str] = []
        for dataset_key in sorted(context.selected_asset_keys):
            dbt_key = dbt_key_by_dataset_key[dataset_key]
            metadata = _refresh_dataset(
                context,
                superset_api.client,
                dbt_asset_group_name=dbt_key.path[0],
                dbt_model_name=dbt_key.path[1],
                database_id=database_id,
                database_name=database_name,
                schema_base=schema_base,
            )
            if metadata["status"] == "error":
                errors.append(metadata["error"])
            yield MaterializeResult(asset_key=dataset_key, metadata=metadata)

        # One bad dataset stays in its own metadata, so a single persistently
        # broken table cannot fail every run -- including an automation run
        # that selected only that table. When at least two datasets were tried
        # and every one failed, the cause is Superset itself (credentials, an
        # outage), and the step has to fail for the Sentry hook and run_retries
        # to see it.
        if len(errors) >= _MIN_FAILURES_TO_RAISE and len(errors) == len(
            context.selected_asset_keys
        ):
            raise Failure(
                description=(
                    f"Every selected {database_name} Superset dataset failed "
                    f"({len(errors)}). First error: {errors[0]}"
                ),
                metadata={"failed_datasets": len(errors)},
            )

    return _superset_datasets

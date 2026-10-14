from collections.abc import Iterator, Sequence
from datetime import UTC, datetime
from typing import Any

import httpx2 as httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    MaterializeResult,
    multi_asset,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes

from lakehouse.lib.dbt_environment import DBT_AUTOMATION_ENABLED
from lakehouse.resources.superset_api import SupersetApiClient, SupersetApiClientFactory

_DEFAULT_SCHEMA_BASE = "ol_warehouse_production"


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
    except (httpx.HTTPError, RuntimeError) as e:
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
    except httpx.HTTPError as e:
        context.log.exception(
            "HTTP error while refreshing dataset for %s.%s",
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
    single step. One step per dataset made a full dbt run fan out to ~200
    steps the moment the dbt step finished, each a subprocess importing the
    code location, which is the memory spike the lakehouse run workers were
    dying at.

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
    ) -> Iterator[MaterializeResult]:
        for dataset_key in sorted(context.selected_asset_keys):
            dbt_key = dbt_key_by_dataset_key[dataset_key]
            yield MaterializeResult(
                asset_key=dataset_key,
                metadata=_refresh_dataset(
                    context,
                    superset_api.client,
                    dbt_asset_group_name=dbt_key.path[0],
                    dbt_model_name=dbt_key.path[1],
                    database_id=database_id,
                    database_name=database_name,
                    schema_base=schema_base,
                ),
            )

    return _superset_datasets

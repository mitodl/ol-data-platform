"""Tell MIT Learn when a warehouse view it pulls has been rebuilt.

MIT Learn reads its warehouse-pull sources (OCW, xPRO and MITx Online courses and
programs) with Celery Sync tasks on a daily beat. A view the lakehouse rebuilds
after a publish would otherwise wait for the next beat tick. This asset POSTs the
names of the rebuilt views to Learn's ``warehouse_views_rebuilt`` webhook, which
queues the Sync tasks that read them. The daily beat stays as reconciliation, so
a failed POST costs latency, not data.

Data flow:
    integrations__learn__* views (dbt, lakehouse)
        → this asset (one signed POST per rebuild)
            → MIT Learn queue_warehouse_syncs
"""

from typing import cast

import httpx2 as httpx
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AutomationCondition,
    Config,
    DagsterInstance,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from ol_orchestrate.lib.failures import RETRY_ON_ASSET_OR_OP_FAILURE_TAG
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.learn_api import MITLearnApiClient
from pydantic import Field

from delivery.lib.learn_webhook import webhook_failure

# The views mit-learn's WAREHOUSE_VIEW_SYNCS (learning_resources/tasks.py) maps to
# a Sync task. A view Learn doesn't map is skipped there, so a view missing here
# only delays Learn until the beat run.
LEARN_WAREHOUSE_VIEWS = (
    "integrations__learn__ocw_courses",
    "integrations__learn__xpro_courses",
    "integrations__learn__xpro_runs",
    "integrations__learn__xpro_programs",
    "integrations__learn__mitxonline_courses",
    "integrations__learn__mitxonline_runs",
    "integrations__learn__mitxonline_programs",
    "integrations__learn__mitxonline_program_children",
)
VIEW_KEYS = {AssetKey(["integrations", view]): view for view in LEARN_WAREHOUSE_VIEWS}


SENT_THROUGH = "sent_through"


class WarehouseViewsConfig(Config):
    """Run config for a hand materialization; a sensor run passes none."""

    all_views: bool = Field(
        default=False,
        description=(
            "Name every view, rebuilt or not, so Learn reruns every warehouse sync."
        ),
    )


def on_view_rebuilt() -> AutomationCondition:
    """Request one run after any view is rebuilt, once nothing it reads is building.

    The latch holds a rebuild until a run is requested, so a rebuild that lands
    while a run is in progress is sent by the next one. Waiting out
    ``any_deps_in_progress`` batches views built by the same dbt run into one
    POST. There is no code-version trigger: a deploy rebuilds nothing Learn reads.

    The trigger is ``any_deps_match(newly_updated())`` and not
    ``any_deps_updated()``, and the reset is ``newly_requested()`` alone. When a
    rebuild and the reset land on the same tick, ``since`` breaks the tie with the
    operands' timing metadata, which an ``Or`` doesn't carry, so with
    ``any_deps_updated()`` (an ``Or`` inside) the reset won and a rebuild landing
    just after a request was dropped. ``newly_updated`` is empty on a first
    evaluation, so enabling the sensor sends nothing on its own.

    :returns: the condition for the asset's sensor
    :rtype: AutomationCondition
    """
    return (
        AutomationCondition.any_deps_match(AutomationCondition.newly_updated()).since(
            AutomationCondition.newly_requested()
        )
        & ~AutomationCondition.in_progress()
        & ~AutomationCondition.any_deps_in_progress()
    )


def _latest_storage_id(instance: DagsterInstance, key: AssetKey) -> int:
    records = instance.fetch_materializations(key, limit=1).records
    return records[0].storage_id if records else 0


@asset(
    key=AssetKey(["mit_learn_delivery", "warehouse_views_rebuilt"]),
    group_name="mit_learn_delivery",
    description=(
        "POST the names of the integrations__learn__* views rebuilt since the last "
        "run to MIT Learn, which queues the warehouse Sync tasks that read them."
    ),
    deps=list(VIEW_KEYS),
    # A 4xx is permanent (webhook_failure), and the daily beat reconciles
    # whatever a failed POST would have sent.
    retry_policy=RetryPolicy(max_retries=2, delay=30.0),
)
def warehouse_views_rebuilt(
    context: AssetExecutionContext,
    config: WarehouseViewsConfig,
    learn_api: ApiClientFactory,
) -> MaterializeResult:
    """Send the views rebuilt since the last successful run read them.

    Each run records, as ``sent_through``, the event log's highest storage id from
    just before it read the views, and the next run sends the views materialized
    after it. Taken before the reads, so a view rebuilt between the reads of two
    others is past the mark and goes in the next run (a view this run already
    sent can go again, which only reruns a sync). Storage ids and not timestamps,
    so clocks on different pods don't matter. A failed run records nothing, so
    its views go again in the next one. A run with nothing new sends nothing.

    A materialization with no ``sent_through`` (one reported by hand from the UI)
    counts as 0, so the next run resends every view that has been built rather
    than failing every run after it.
    """
    instance = context.instance
    # The step retries a 5xx twice, and the run_retries daemon would rerun the
    # whole run three more times on top, up to 12 POSTs. The sensor runs this
    # asset alone, so turning run retries off for its run touches nothing else.
    # The next rebuild or Learn's daily beat covers a notice that never lands.
    instance.add_run_tags(context.run_id, {RETRY_ON_ASSET_OR_OP_FAILURE_TAG: "false"})
    read_through = instance.event_log_storage.get_maximum_record_id() or 0
    previous = instance.fetch_materializations(context.asset_key, limit=1).records
    metadata = (
        previous[0].asset_materialization.metadata
        if previous and previous[0].asset_materialization
        else {}
    )
    sent_through = (
        cast(int, metadata[SENT_THROUGH].value) if SENT_THROUGH in metadata else 0
    )
    latest = {
        view: _latest_storage_id(instance, key) for key, view in VIEW_KEYS.items()
    }
    if config.all_views:
        views = list(LEARN_WAREHOUSE_VIEWS)
    else:
        views = [
            view for view, storage_id in latest.items() if storage_id > sent_through
        ]
    read_through = max(read_through, sent_through)

    if not views:
        context.log.info("No view rebuilt since storage id %s", sent_through)
        return MaterializeResult(
            metadata={"views": MetadataValue.json([]), SENT_THROUGH: read_through}
        )

    context.log.info("Telling MIT Learn these views were rebuilt: %s", views)
    try:
        response = cast(
            MITLearnApiClient, learn_api.client
        ).notify_warehouse_views_rebuilt(views)
    except httpx.HTTPStatusError as exc:
        failure = webhook_failure("Warehouse views rebuilt", exc)
        context.log.exception(failure.description)
        raise failure from exc

    return MaterializeResult(
        metadata={
            "views": MetadataValue.json(views),
            "learn_message": response["message"],
            SENT_THROUGH: read_through,
        }
    )

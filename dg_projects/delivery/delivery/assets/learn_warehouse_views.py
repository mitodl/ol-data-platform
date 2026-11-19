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
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)
from ol_orchestrate.resources.api_client_factory import ApiClientFactory
from ol_orchestrate.resources.learn_api import MITLearnApiClient

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


def on_view_rebuilt() -> AutomationCondition:
    """Request one run after any view is rebuilt, once nothing it reads is building.

    The latch holds a rebuild until the run is requested, so a rebuild that lands
    while a run is in progress is sent by the next one. Waiting out
    ``any_deps_in_progress`` batches views built by the same dbt run into one
    POST. ``initial_evaluation`` resets the latch on the first tick, so enabling
    the sensor doesn't send every view at once. There is no code-version trigger:
    a deploy rebuilds nothing Learn reads.

    :returns: the condition for the asset's sensor
    :rtype: AutomationCondition
    """
    return (
        AutomationCondition.any_deps_updated().since(
            AutomationCondition.newly_requested()
            | AutomationCondition.initial_evaluation()
        )
        & ~AutomationCondition.in_progress()
        & ~AutomationCondition.any_deps_in_progress()
    )


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
    context: AssetExecutionContext, learn_api: ApiClientFactory
) -> MaterializeResult:
    """Send the views rebuilt since this asset last materialized.

    A run with nothing rebuilt since then, which only a hand materialization
    makes, sends every view, so it works as "resync now".
    """
    instance = context.instance
    previous = instance.get_latest_materialization_event(context.asset_key)
    since = previous.timestamp if previous else 0.0
    rebuilt = []
    for key, view in VIEW_KEYS.items():
        event = instance.get_latest_materialization_event(key)
        if event is not None and event.timestamp > since:
            rebuilt.append(view)
    views = rebuilt or list(LEARN_WAREHOUSE_VIEWS)

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
        }
    )

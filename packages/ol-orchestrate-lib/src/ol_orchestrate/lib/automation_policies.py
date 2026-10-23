from dagster import AutomationCondition

# Midnight UTC. See dbt_upstream_or_code_changes for why the wait spans two ticks.
PENDING_PARENT_EXPIRY_CRON = "0 0 * * *"


def upstream_or_code_changes() -> AutomationCondition:
    """Materialize when an upstream's data version or this asset's code changed.

    Includes a retry for failed executions. Without it an update can be dropped
    outright: ``data_version_changed`` is edge-triggered, true only on the tick
    where the upstream version moved, so if the run that tick launches fails,
    the signal is gone and nothing ever asks again. The asset then sits
    indefinitely on stale data while every tick reports nothing to do.

    ``execution_failed`` is level-triggered -- true for as long as the latest
    execution of the target is a failure. Used bare that is unbounded: a
    partition that can never succeed is re-requested on every tick forever, and
    ``run_retries.max_retries`` multiplies each request. That is how a single
    broken asset produced ~368,000 failed runs in fourteen days.

    ``.newly_true()`` converts it to an edge. It fires on the tick where the
    latest execution first becomes a failure and not again, because the level
    never drops back to false while the retry is in flight or after it also
    fails. One failure therefore buys exactly one re-request; a success clears
    the level, so the next genuine failure fires again. That covers the run that
    fails without covering the run that can never succeed.

    The upstream signal is latched on top of it, because failure is not the only
    way the edge gets lost. ``~in_progress()`` gates the whole conjunction, so an
    upstream version that moves *while a run is already going* is suppressed on
    the one tick where ``data_version_changed`` is true. That run then succeeds
    -- it was launched for the older version, but it succeeded -- leaving no
    failure to retry and no edge to re-fire. The asset sits on the older
    upstream until the upstream happens to change again, silently.

    The reset is ``newly_requested``, deliberately NOT the ``newly_updated`` that
    ``.since_last_handled()`` would also include. A run launched for the older
    version emits its own materialization, and counting that as handling would
    clear the latch for a version it never read -- reintroducing the same loss it
    is here to prevent. Requesting is the honest reset: it means this condition
    got its run.

    ``newly_requested`` also avoids the failure mode that kept the signal
    unlatched until now. A latch reset only by ``newly_updated`` never clears for
    an asset declared ``output_required=False`` that legitimately produces
    nothing, so it would re-request on every tick forever; a request is recorded
    whether or not the run emits an output, so this formulation stays quiet
    there.

    Trade: a materialization from outside this condition -- a manual run, a
    backfill -- no longer clears a pending upstream change, so one automation run
    can follow it. That is the conservative direction to err in, and it settles
    after that single run.

    Deploy note: ``NewlyTrueCondition`` diffs against the previous tick's true
    subset held in its cursor, and there is no cursor on the first evaluation.
    Every partition that is *already* failed therefore reads as newly true once,
    and fires one request each on a single tick. Against a large standing set of
    failures that is a stampede -- so clear the failed set, or roll this out a
    code location at a time, before deploying it.
    """
    return _upstream_or_code_changes(AutomationCondition.code_version_changed())


def dbt_upstream_or_code_changes() -> AutomationCondition:
    """``upstream_or_code_changes`` for dbt models, which read their parents' columns.

    A dbt model whose SQL starts selecting a new column must not run until the
    parent that adds the column has been rebuilt with its new SQL. The shared
    condition has nothing that orders the two, and it lost that race in
    production twice:

    - #3050 changed ``int__mitxonline__catalog_pages`` and gave it a new, never
      materialized staging parent. On the deploy tick ``any_deps_missing``
      blocked catalog_pages, which spent its one-tick ``code_version_changed``.
      Its changed children had nothing blocking them and ran alone against the
      old catalog_pages table (COLUMN_NOT_FOUND).
    - #3032 changed ``stg__openedx__s3__course_document_text``, which only the
      daily staging job builds, and its child in the same release. The child ran
      on the next sensor tick, hours before the staging model was rebuilt.

    Two changes close that. The asset's own code change is latched until it is
    requested, like the upstream signal, so a tick spent blocked no longer drops
    it. And the asset waits while any parent has a code change it has not yet
    materialized, unless that parent is requested on the same tick (dbt then
    builds them in one run, in dependency order). ``will_be_requested`` is only
    ever true inside the sensor's target, so a staging parent holds its children
    until a staging job rebuilds it.

    A parent that never succeeds, or that nothing builds, would hold its direct
    children forever, so the wait expires at the second daily cron tick after
    the change: 24 to 48 hours, longer than the daily cadence of every staging
    job. Expiring at the first tick would release a change seen just before
    midnight a few hours later, before the 06:00 UTC staging build.

    Cursor note: the latch and the per-parent state are new nodes with empty
    cursors on the first tick after this deploys, so a model change shipped in
    the same release is not held. Existing nodes keep their cursors: node ids
    hash the path from the root, and ``since`` / ``newly_true`` hash only their
    own subtree and target key, so appending an operand moves nothing.
    """
    own_code_change = AutomationCondition.code_version_changed().since(
        AutomationCondition.newly_requested()
    )
    return _upstream_or_code_changes(own_code_change) & ~_any_deps_code_change_pending()


def _any_deps_code_change_pending() -> AutomationCondition:
    """Hold an asset while a parent has new code it has not materialized yet.

    Known gap: a parent run that started on the old code and finished between
    the deploy and the next tick puts ``newly_updated`` on the same tick as the
    code change. ``since`` resolves that tie to the reset (``code_version_changed``
    carries no timestamp to order them by), so that parent is not held. That is
    the behavior from before this condition existed, not a new failure.
    """
    code_changed = AutomationCondition.code_version_changed()
    daily = AutomationCondition.cron_tick_passed(PENDING_PARENT_EXPIRY_CRON)
    # True from the first daily tick after the change. A tick on the same
    # evaluation as the change loses the tie to it, so it never arms early.
    armed = daily.since(code_changed)
    expired = daily & armed & ~armed.newly_true()
    pending = code_changed.since(AutomationCondition.newly_updated() | expired)
    return AutomationCondition.any_deps_match(
        pending & ~AutomationCondition.will_be_requested()
    ).with_label("any_deps_code_change_pending")


def _upstream_or_code_changes(
    has_code_changes: AutomationCondition,
) -> AutomationCondition:
    not_in_progress = ~AutomationCondition.in_progress()
    no_upstream_dependencies_in_process = ~AutomationCondition.any_deps_in_progress()
    has_upstream_changes = (
        AutomationCondition.any_deps_updated()
        .replace("newly_updated", AutomationCondition.data_version_changed())
        .since(
            AutomationCondition.newly_requested()
            | AutomationCondition.initial_evaluation()
        )
    )
    newly_missing = AutomationCondition.newly_missing()
    execution_newly_failed = AutomationCondition.execution_failed().newly_true()
    all_upstream_dependencies_present = ~AutomationCondition.any_deps_missing()
    return (
        not_in_progress
        & no_upstream_dependencies_in_process
        & (
            has_upstream_changes
            | has_code_changes
            | newly_missing
            | execution_newly_failed
        )
        & all_upstream_dependencies_present
    )

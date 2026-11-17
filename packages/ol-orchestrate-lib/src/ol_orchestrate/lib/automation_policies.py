from dagster import AssetKey, AssetSelection, AutomationCondition

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
    return _upstream_or_code_changes()


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

    The hold covers direct parents only. With A (changed) -> B (unchanged) -> C
    (changed), C is not held by A, so if B passes A's new column through, C can
    still run before A and fail the same way.

    A parent that never succeeds, or that nothing builds, would hold its direct
    children forever, so the wait expires at the second daily cron tick after
    the change: 24 to 48 hours. The ``sync_and_stage_*`` schedules start at
    00:00 UTC and repeat every 6 to 24 hours, and ``non_airbyte_staging_daily``
    runs at 06:00, so every staging schedule fires at least once inside that
    window. Expiring at the first midnight would release a change seen late in
    the evening one sensor tick later, before the 06:00 build and possibly
    while the 00:00 sync is still running.

    Cursor note: the latch is appended as the last trigger, so the bare
    ``code_version_changed`` keeps its index and its cursor, and a model whose
    SQL changes in the same release as this condition is still requested on the
    deploy tick. Node ids hash the path from the root, and ``since`` /
    ``newly_true`` hash only their own subtree and target key, so appending
    moves nothing. The latch and the per-parent state start with empty cursors,
    though, so that model is neither latched nor held on that first tick.
    """
    return (
        _upstream_or_code_changes(_own_code_change()) & ~_any_deps_code_change_pending()
    )


def keyset_snapshot_upstream_or_code_changes(
    keyset_key: AssetKey,
) -> AutomationCondition:
    """``dbt_upstream_or_code_changes`` for a raw history snapshot with a keyset.

    The snapshot's delete filter (``raw_delete_filter``) reads the raw table
    and its keyset, which ``ol_dlt.keyset`` reloads after each load of the
    raw table. A snapshot started by the raw change would read the previous
    load's keyset, and a second one would follow the keyset run, so the
    snapshot ignores its raw parent and runs on the keyset's update instead:
    load, keyset, snapshot, once per load.

    A keyset that has stopped running must not stop the history. While the
    keyset's freshness policy reads FAIL, a raw change starts the snapshot as
    it would without a keyset, and the filter applies the stale keyset (it
    keeps every row loaded after the keyset's read, so changes are recorded and
    deletes wait). The keyset's own FAIL state is the alert.

    Both signals are latched until the snapshot is requested, as the shared
    upstream signal is. The raw signal latches only a raw change that lands
    while the keyset is stale, so a load that came while the keyset was fresh
    waits for the keyset run, or for the next load once the keyset goes stale.
    Staleness is two load intervals, so that costs at most one more load.

    Why the stale gate sits inside the raw latch rather than outside it: a
    ``since`` node's cursor id hashes its children's names but not an
    ``AnyDepsCondition``'s allow or ignore selection, so two ``any_deps``
    latches that differ only in selection share one cursor, and the raw latch's
    state overwrites the keyset latch's on every tick (a raw load alone then
    starts the snapshot on the next tick). The ``&`` makes the two structurally
    different. test_keyset_snapshot_latches_have_their_own_cursors pins it.

    Known double run: the freshness daemon re-evaluates every 30 seconds, but
    the snapshot's sensor and the keyset's sensor tick independently. On the
    first load after a stale keyset recovers, the snapshot can be requested
    for the raw change on the same tick the keyset is, and again once the
    keyset lands.

    :param keyset_key: the asset key of the snapshot's keyset table.
    :returns: the condition for the snapshot's asset.
    :rtype: AutomationCondition
    """
    keyset = AssetSelection.assets(keyset_key)
    handled = (
        AutomationCondition.newly_requested() | AutomationCondition.initial_evaluation()
    )
    deps_changed = AutomationCondition.any_deps_updated().replace(
        "newly_updated", AutomationCondition.data_version_changed()
    )
    keyset_stale = AutomationCondition.any_deps_match(
        AutomationCondition.freshness_failed()
    ).allow(keyset)
    keyset_changed = deps_changed.allow(keyset).since(handled)
    raw_changed_while_stale = (deps_changed.ignore(keyset) & keyset_stale).since(
        handled
    )
    return (
        _upstream_or_code_changes(
            _own_code_change(),
            has_upstream_changes=keyset_changed | raw_changed_while_stale,
        )
        & ~_any_deps_code_change_pending()
    ).with_label("keyset_snapshot_upstream_or_code_changes")


def _own_code_change() -> AutomationCondition:
    """Latch the asset's own code change until it is requested."""
    # The reset excludes a tick that also sees a change. newly_requested is true
    # on the tick after a request, and since() resolves a tie to the reset, so a
    # change landing while that earlier run is still going would be consumed by
    # a request made before it, and in_progress blocks the bare trigger too.
    return AutomationCondition.code_version_changed().since(
        AutomationCondition.newly_requested()
        & ~AutomationCondition.code_version_changed()
    )


def _any_deps_code_change_pending() -> AutomationCondition:
    """Hold an asset while a parent has new code it has not materialized yet.

    Known gaps: ``newly_updated`` counts any materialization, not one built
    from the new SQL, so a parent run that started on the old image before the
    deploy releases the hold when it finishes.

    - If it finishes before the first tick that sees the change, the
      materialization and the code change land on the same tick. ``since``
      resolves that tie to the reset (``code_version_changed`` carries no
      timestamp to order them by), so the parent is never held.
    - If it finishes after that tick (e.g. a staging run already going when
      the deploy lands), its materialization clears the hold on the next tick,
      though the table was built with the old SQL.

    Either way the child runs as it did before this condition existed.
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
    extra_trigger: AutomationCondition | None = None,
    *,
    has_upstream_changes: AutomationCondition | None = None,
) -> AutomationCondition:
    """Build the shared tree, with ``extra_trigger`` appended to the trigger ``|``.

    Appended last so every existing operand keeps its index, and with it the
    node id its cursor is stored under. ``has_upstream_changes`` replaces the
    upstream trigger, which reshapes the tree, so only a new condition passes it.
    """
    not_in_progress = ~AutomationCondition.in_progress()
    no_upstream_dependencies_in_process = ~AutomationCondition.any_deps_in_progress()
    if has_upstream_changes is None:
        has_upstream_changes = (
            AutomationCondition.any_deps_updated()
            .replace("newly_updated", AutomationCondition.data_version_changed())
            .since(
                AutomationCondition.newly_requested()
                | AutomationCondition.initial_evaluation()
            )
        )
    has_code_changes = AutomationCondition.code_version_changed()
    newly_missing = AutomationCondition.newly_missing()
    execution_newly_failed = AutomationCondition.execution_failed().newly_true()
    all_upstream_dependencies_present = ~AutomationCondition.any_deps_missing()
    triggers = (
        has_upstream_changes | has_code_changes | newly_missing | execution_newly_failed
    )
    if extra_trigger is not None:
        triggers = triggers | extra_trigger
    return (
        not_in_progress
        & no_upstream_dependencies_in_process
        & triggers
        & all_upstream_dependencies_present
    )

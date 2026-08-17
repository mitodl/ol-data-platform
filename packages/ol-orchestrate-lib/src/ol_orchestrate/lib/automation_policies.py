from dagster import AutomationCondition


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
    has_code_changes = AutomationCondition.code_version_changed()
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

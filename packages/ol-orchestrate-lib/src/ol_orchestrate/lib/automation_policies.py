from dagster import AutomationCondition


def upstream_or_code_changes() -> AutomationCondition:
    """Materialize when an upstream's data version or this asset's code changed.

    Includes a retry for failed executions. Without it an update can be dropped
    outright: ``data_version_changed`` is edge-triggered, true only on the tick
    where the upstream version moved, so if the run that tick launches fails,
    the signal is gone and nothing ever asks again. The asset then sits
    indefinitely on stale data while every tick reports nothing to do.

    ``execution_failed`` is level-triggered -- true for as long as the latest
    execution of the target is a failure -- so it keeps asking until a run
    succeeds and goes quiet the moment one does. That covers the run that fails.

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
    latest_execution_failed = AutomationCondition.execution_failed()
    all_upstream_dependencies_present = ~AutomationCondition.any_deps_missing()
    return (
        not_in_progress
        & no_upstream_dependencies_in_process
        & (
            has_upstream_changes
            | has_code_changes
            | newly_missing
            | latest_execution_failed
        )
        & all_upstream_dependencies_present
    )

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
    succeeds and goes quiet the moment one does. It is deliberately preferred
    over latching the upstream signal with ``.since(newly_updated())``: a latch
    only resets on an actual materialization, so an asset declared
    ``output_required=False`` that legitimately produces nothing would be
    re-requested on every tick forever. A run that succeeds without emitting an
    output is not a failed execution, so this formulation stays quiet there.
    """
    not_in_progress = ~AutomationCondition.in_progress()
    no_upstream_dependencies_in_process = ~AutomationCondition.any_deps_in_progress()
    has_upstream_changes = AutomationCondition.any_deps_updated().replace(
        "newly_updated", AutomationCondition.data_version_changed()
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

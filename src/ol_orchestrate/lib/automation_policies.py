from dagster import AutomationCondition


def upstream_or_code_changes() -> AutomationCondition:
    no_upstream_dependencies_in_process = ~AutomationCondition.any_deps_in_progress()
    has_upstream_changes = AutomationCondition.any_deps_updated()
    has_code_changes = AutomationCondition.code_version_changed()
    newly_missing = AutomationCondition.newly_missing()
    return no_upstream_dependencies_in_process & (
        has_upstream_changes | has_code_changes | newly_missing
    )

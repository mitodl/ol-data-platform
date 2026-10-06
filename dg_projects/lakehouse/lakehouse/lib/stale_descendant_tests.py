"""Name the dbt tests a subset build would run against a table it did not rebuild.

dbt's eager indirect selection runs a test when *any* of its parents is
selected. A ``relationships`` test on ``bridge_user_courserun_role.user_fk``
has two parents, the bridge and ``dim_user``, so a run that rebuilds only
``dim_user`` also runs it, comparing the new ``user_pk`` values against a
bridge that still holds the previous build's keys. The automation condition
guarantees that ordering: a descendant is only requested on the tick *after*
its upstream's data version moved. The test then fails on orphans that the
bridge's own rebuild removes (#2571).

The tests named here are the ones attached to an unselected descendant of a
selected model. They are skipped in that run and still run, in the same
invocation as the rebuild, when the descendant itself is selected.

``--indirect-selection buildable`` would also skip them, but it additionally
skips every test between two models where neither descends from the other
(e.g. ``dim_course_run.courserun_start_date_key`` against ``dim_date``) unless
both are selected together, and those are not reading stale data.

Pure functions only, for the reason ``surrogate_key_drift`` gives.
"""

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from typing import Any


def _descendants(
    child_map: Mapping[str, list[str]], unique_ids: AbstractSet[str]
) -> set[str]:
    found: set[str] = set()
    pending = list(unique_ids)
    while pending:
        for child in child_map.get(pending.pop(), []):
            if child not in found:
                found.add(child)
                pending.append(child)
    return found


def stale_descendant_test_names(
    manifest: Mapping[str, Any], selected_unique_ids: AbstractSet[str]
) -> list[str]:
    """Return the tests that would compare a rebuilt model with a stale descendant.

    Only tests with an ``attached_node`` are returned. Dagster models those as
    asset checks on the attached model, which is unselected here, so skipping
    them leaves no selected check without a result.

    :param manifest: The parsed dbt ``manifest.json``.
    :param selected_unique_ids: dbt unique ids of the models this run builds.
    :returns: Sorted test names, usable as a dbt ``--exclude`` value.
    :rtype: list[str]
    """
    selected = set(selected_unique_ids)
    stale = _descendants(manifest["child_map"], selected) - selected
    return sorted(
        node["name"]
        for node in manifest["nodes"].values()
        if node["resource_type"] == "test"
        and node.get("attached_node") in stale
        and selected.intersection(node["depends_on"]["nodes"])
    )

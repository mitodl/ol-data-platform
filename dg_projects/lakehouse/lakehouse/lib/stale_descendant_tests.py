"""Name the dbt tests a subset build would run against a table it did not rebuild.

dbt's eager indirect selection runs a test when *any* of its parents is
selected. A ``relationships`` test on ``bridge_user_courserun_role.user_fk``
has two parents, the bridge and ``dim_user``, so a run that rebuilds only
``dim_user`` also runs it, comparing the new ``user_pk`` values against a
bridge that still holds the previous build's keys. The automation condition
guarantees that ordering: a descendant is only requested on the tick *after*
its upstream's data version moved. The test then fails on orphans that the
bridge's own rebuild removes (#2571).

The tests named here have a selected parent and are attached to an unselected
descendant of it. They are skipped in that run and still run, in the same
invocation as the rebuild, when the descendant itself is selected. A singular
test has no attached model, so it is skipped when any of its other parents is
such a descendant.

``--indirect-selection buildable`` would also skip them, but it additionally
skips every test between two models where neither descends from the other
(e.g. ``dim_course_run.courserun_start_date_key`` against ``dim_date``) unless
both are selected together, and those are not reading stale data.

That reasoning assumes something else keeps the unselected model built, which
only the automation sensor does. Where it does not run (QA), an unselected
model's table can be months old or absent: the 2026-10-07 QA Learn build failed
with TABLE_NOT_FOUND on a ``relationships`` test attached to
``stg__mitxonline__app__postgres__courses_course_to_department``, pulled in
through the selected ``courses_course`` it references. There
``unselected_model_test_names`` skips every test held against a model the run
does not build, descendant or not.

Pure functions only, for the reason ``surrogate_key_drift`` gives.
"""

from collections.abc import Collection, Mapping
from collections.abc import Set as AbstractSet
from typing import Any


def _descendants(child_map: Mapping[str, list[str]], unique_id: str) -> set[str]:
    found: set[str] = set()
    pending = [unique_id]
    while pending:
        for child in child_map.get(pending.pop(), []):
            if child not in found:
                found.add(child)
                pending.append(child)
    return found


def stale_descendant_test_names(
    manifest: Mapping[str, Any],
    selected_unique_ids: AbstractSet[str],
    keep: Collection[str] = (),
) -> list[str]:
    """Return the tests that would compare a rebuilt model with a stale descendant.

    Dagster models a test with an ``attached_node`` as an asset check on that
    model. The attached tests returned here belong to unselected models, so
    skipping them leaves no check of a selected asset without a result. A
    check the run selected by name on an unselected asset is the exception,
    which is what ``keep`` is for.

    :param manifest: The parsed dbt ``manifest.json``.
    :param selected_unique_ids: dbt unique ids of the models this run builds.
    :param keep: Test names the run asked for explicitly, never returned.
    :returns: Sorted test names, usable as a dbt ``--exclude`` value.
    :rtype: list[str]
    """
    child_map = manifest["child_map"]
    descendants = {uid: _descendants(child_map, uid) for uid in selected_unique_ids}
    names = []
    for node in manifest["nodes"].values():
        if node["resource_type"] != "test" or node["name"] in keep:
            continue
        parents = set(node["depends_on"]["nodes"])
        attached = node.get("attached_node")
        candidates = ({attached} if attached else parents) - selected_unique_ids
        if any(
            candidates & descendants[parent] for parent in parents & selected_unique_ids
        ):
            names.append(node["name"])
    return sorted(names)


def unselected_model_test_names(
    manifest: Mapping[str, Any],
    selected_unique_ids: AbstractSet[str],
    keep: Collection[str] = (),
) -> list[str]:
    """Return the tests a run would hold against a model it does not build.

    A superset of ``stale_descendant_test_names``, for an environment where no
    automation builds the models outside the selection. Like it, an attached
    test returned here belongs to an unselected model, so no check of a
    selected asset loses its result.

    :param manifest: The parsed dbt ``manifest.json``.
    :param selected_unique_ids: dbt unique ids of the models this run builds.
    :param keep: Test names the run asked for explicitly, never returned.
    :returns: Sorted test names, usable as a dbt ``--exclude`` value.
    :rtype: list[str]
    """
    names = []
    for node in manifest["nodes"].values():
        if node["resource_type"] != "test" or node["name"] in keep:
            continue
        parents = {
            parent
            for parent in node["depends_on"]["nodes"]
            if parent.startswith("model.")
        }
        attached = node.get("attached_node")
        candidates = ({attached} if attached else parents) - selected_unique_ids
        if candidates and parents & selected_unique_ids:
            names.append(node["name"])
    return sorted(names)

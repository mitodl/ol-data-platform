"""Locate repository trees the lakehouse image copies in beside the package."""

from pathlib import Path


def find_upward(anchor: Path, relative: Path, fallback: Path) -> Path:
    """Return the first ``<parent>/<relative>`` directory above *anchor*.

    The search is anchored on a module file rather than the process's cwd,
    which a run launcher is free to change, and walks upward instead of
    indexing a fixed parent because the two layouts this runs in sit at
    different depths. In the source tree a module in ``lakehouse/lib/`` is four
    levels below the repo root; in the image it is two, because
    dg_projects/lakehouse/Dockerfile copies the project's *contents* onto /app.
    No fixed index is right for both, and the one that matched the source tree
    raised IndexError at import inside the image, taking the entire code
    location down before dagster could load its definitions.

    :param anchor: the calling module's ``__file__``.
    :param relative: the tree to find, relative to the repo root.
    :param fallback: returned when no parent has it, so a missing tree is a
        findable runtime failure rather than an unimportable module.
    :returns: the directory found, or *fallback*.
    :rtype: Path
    """
    for parent in anchor.resolve().parents:
        candidate = parent / relative
        if candidate.is_dir():
            return candidate
    return fallback

"""Every ol_dlt source must declare `_dlt_load_id` nullable.

dlt defines the column as non-nullable and adds it to the Arrow schema it evolves
Iceberg tables from, so a source that leaves the default in place asks pyiceberg
to add a REQUIRED column. pyiceberg refuses that on any format-v1/v2 table that
already holds rows:

    ValueError: Incompatible change: cannot add required column: _dlt_load_id

That is not caught at import or by a unit test of the source itself -- it
surfaces on the source's next load against an existing table, in whichever
environment deploys first. This module is the guard: a new source that forgets
the declaration fails here instead.
"""

from __future__ import annotations

import importlib
import inspect
import pkgutil

import pytest

import ol_dlt.sources
from ol_dlt.config import DLT_LOAD_ID_COLUMN

LOAD_ID = "_dlt_load_id"


def _source_modules() -> list[str]:
    return sorted(
        m.name for m in pkgutil.iter_modules(ol_dlt.sources.__path__) if m.ispkg
    )


def test_the_declaration_is_nullable() -> None:
    """The whole point of the override -- guard against it being flipped back."""
    assert DLT_LOAD_ID_COLUMN[LOAD_ID]["nullable"] is True


def test_every_source_package_is_covered() -> None:
    """Fail when a new source package appears without being considered here."""
    known = {
        "edxorg_s3",
        "keycloak",
        "mit_climate",
        "mit_edx_programs",
        "mitpe",
        "mitxonline_app",
        "oll",
        "podcast_rss",
        "youtube",
    }
    assert set(_source_modules()) == known, (
        "A source package was added or removed. Make sure it declares "
        f"{LOAD_ID} nullable (config.with_nullable_load_id, or columns= on its "
        "resource hints), then update this list."
    )


@pytest.mark.parametrize("module_name", _source_modules())
def test_source_declares_nullable_load_id(module_name: str) -> None:
    """Each source routes the declaration through one of the two seams.

    Checked statically rather than by building the source: several need Vault
    credentials or network access to instantiate, and this assertion is about
    the wiring, not about a live connection.
    """
    module = importlib.import_module(f"ol_dlt.sources.{module_name}")
    src = inspect.getsource(module)
    assert (
        "with_nullable_load_id" in src
        or "DLT_LOAD_ID_COLUMN" in src
        or "build_database_source" in src
    ), (
        f"{module_name} does not declare {LOAD_ID} as nullable. Wrap its "
        "build_source() in config.with_nullable_load_id, or pass "
        "columns=config.DLT_LOAD_ID_COLUMN to its resource hints."
    )


def test_database_backed_sources_get_it_from_the_shared_builder() -> None:
    """build_table_resource is the seam for every DatabaseSourceSpec source."""
    import ol_dlt.database

    src = inspect.getsource(ol_dlt.database)
    assert "columns=config.DLT_LOAD_ID_COLUMN" in src

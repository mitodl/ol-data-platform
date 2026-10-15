"""The factory package of the `xpro/app_postgres` unit.

`_generated.py` holds the base factories `ol-dbt fixtures scaffold` wrote from
the app's models. This module subclasses them for what the models cannot say
and wires the tables together in `generate` (see ol_dbt_cli.fixture_factories).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Mapping

SEED = 311373020


def generate(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> dict[str, list[dict[str, Any]]]:
    """Return no generated rows until the tables are wired together."""
    return {}

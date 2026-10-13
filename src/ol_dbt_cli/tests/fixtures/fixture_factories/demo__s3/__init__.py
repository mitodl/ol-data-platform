"""The factory package of the `demo/s3` unit the raw-fixture tests build."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SEED = 20261013
RAW_TABLE = "raw__demo__s3__things"
DEFAULT_COUNT = 3


def generate(hand_written: Mapping[str, list[dict[str, Any]]], scale: float) -> dict[str, list[dict[str, Any]]]:
    """Continue the hand-written ids, leaving `name` out of every other row."""
    first = max((row["id"] for row in hand_written[RAW_TABLE]), default=0) + 1
    rows: list[dict[str, Any]] = []
    for number in range(first, first + round(DEFAULT_COUNT * scale)):
        row: dict[str, Any] = {"id": number}
        if number % 2:
            row["name"] = f"thing {number}"
        rows.append(row)
    return {RAW_TABLE: rows}

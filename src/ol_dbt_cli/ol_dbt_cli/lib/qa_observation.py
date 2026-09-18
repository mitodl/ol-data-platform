"""What the QA raw layer actually holds, per inventory table — RFC 12711 step 5.

The QA branch contract has two halves (docs/specs/QA_DATA_TOPOLOGY_SPEC.md §2).
Declarations contradicting the inventory need nothing but text. A declared
branch that is empty, or a mirror past its ``mirror_max_age_days``, needs an
observation of the QA lake, and CI holds no AWS credentials. So the
observation is taken out of band by ``ol-dbt inventory observe`` and committed
as ``ingestion/inventory/qa_observation.json``; ``ol-dbt validate`` reads the
file.

Emptiness comes from the current snapshot in each table's Iceberg metadata, not
from Glue ``UpdateTime``: the 2026-09-08 JSONL->Iceberg conversion rewrote
``UpdateTime`` on 2,354 QA tables, and converted empty shells have no snapshot
at all (spec §7, "How it was measured").
"""

from __future__ import annotations

import gzip
import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from ol_dbt_cli.lib.qa_contract import QA_STRATEGIES

if TYPE_CHECKING:
    from pathlib import Path

    from ol_dbt_cli.lib.inventory import Unit

OBSERVATION_FILENAME = "qa_observation.json"
QA_GLUE_DATABASE = "ol_warehouse_qa_raw"

_METADATA_FETCH_WORKERS = 16


@dataclass(frozen=True)
class TableState:
    present: bool
    iceberg: bool = False
    rows: int | None = None
    snapshot_at: datetime | None = None

    def to_json(self) -> dict[str, Any]:
        data = asdict(self)
        data["snapshot_at"] = self.snapshot_at.isoformat() if self.snapshot_at else None
        return data

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> TableState:
        snapshot_at = data.get("snapshot_at")
        return cls(
            present=data["present"],
            iceberg=data.get("iceberg", False),
            rows=data.get("rows"),
            snapshot_at=datetime.fromisoformat(snapshot_at) if snapshot_at else None,
        )


@dataclass(frozen=True)
class Observation:
    glue_database: str
    observed_at: datetime
    tables: dict[str, TableState]


def observed_tables(units: list[Unit]) -> set[str]:
    """Raw tables of every unit a QA contract can legally declare."""
    return {
        str(table["raw_table"])
        for unit in units
        if (unit.data.get("strategies") or {}).get("qa") in QA_STRATEGIES
        for table in unit.tables
    }


def load_observation(path: Path) -> Observation | None:
    if not path.exists():
        return None
    raw = json.loads(path.read_text())
    return Observation(
        glue_database=raw["glue_database"],
        observed_at=datetime.fromisoformat(raw["observed_at"]),
        tables={name: TableState.from_json(state) for name, state in raw["tables"].items()},
    )


def render_observation(observation: Observation) -> str:
    """One table per line, so a refresh diffs as the tables whose state changed."""
    header = {
        "glue_database": observation.glue_database,
        "observed_at": observation.observed_at.isoformat(),
    }
    lines = [f"  {json.dumps(key)}: {json.dumps(value)}," for key, value in header.items()]
    entries = [
        f"    {json.dumps(name)}: {json.dumps(observation.tables[name].to_json(), sort_keys=True)}"
        for name in sorted(observation.tables)
    ]
    return "\n".join(["{", *lines, '  "tables": {', ",\n".join(entries), "  }", "}", ""])


def _read_metadata(s3: Any, location: str) -> dict[str, Any]:
    parsed = urlparse(location)
    body = s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"].read()
    if location.endswith(".gz") or ".gz." in location:
        body = gzip.decompress(body)
    return json.loads(body)


def _current_snapshot(metadata: dict[str, Any]) -> tuple[int | None, datetime | None]:
    current = metadata.get("current-snapshot-id")
    for snapshot in metadata.get("snapshots") or []:
        if snapshot["snapshot-id"] == current:
            rows = int(snapshot.get("summary", {}).get("total-records", 0))
            return rows, datetime.fromtimestamp(snapshot["timestamp-ms"] / 1000, tz=UTC)
    return None, None


def observe_glue(tables: set[str], database: str = QA_GLUE_DATABASE, region: str = "us-east-1") -> Observation:
    """Read presence, row count and snapshot time for *tables* from Glue and S3.

    Glue stores table names lowercased, while some inventory names (salesforce)
    are mixed case, so the lookup is case-insensitive and the result is keyed by
    the inventory's spelling.
    """
    import boto3  # noqa: PLC0415

    glue = boto3.client("glue", region_name=region)
    s3 = boto3.client("s3", region_name=region)
    wanted = {name.lower(): name for name in tables}
    found: dict[str, dict[str, Any]] = {}
    for page in glue.get_paginator("get_tables").paginate(DatabaseName=database):
        for table in page["TableList"]:
            if table["Name"] in wanted:
                found[wanted[table["Name"]]] = table

    def state(name: str) -> TableState:
        table = found.get(name)
        if table is None:
            return TableState(present=False)
        params = table.get("Parameters") or {}
        if params.get("table_type", "").upper() != "ICEBERG":
            return TableState(present=True)
        rows, snapshot_at = _current_snapshot(_read_metadata(s3, params["metadata_location"]))
        return TableState(present=True, iceberg=True, rows=rows, snapshot_at=snapshot_at)

    names = sorted(tables)
    with ThreadPoolExecutor(max_workers=_METADATA_FETCH_WORKERS) as pool:
        states = dict(zip(names, pool.map(state, names), strict=True))
    return Observation(glue_database=database, observed_at=datetime.now(tz=UTC), tables=states)

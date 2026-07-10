"""Profile-based configuration for ol_dlt pipelines.

This module is the single source of truth for how a pipeline's destination,
dataset, and table format are chosen. It replaces the copy-pasted
``if DAGSTER_ENVIRONMENT in ("qa", "production")`` blocks that used to live in
every source's ``loads.py``.

The active "profile" is a plain (pure-dlt) environment variable, ``DLT_PROFILE``
(default ``dev``). There is no ``dlt.yml`` / dlt+ project manifest — we run
open-source dlt, so the profile is just this env var. The ``data_loading``
Dagster code location maps ``DAGSTER_ENVIRONMENT`` -> ``DLT_PROFILE`` before
importing pipeline factories.

Profiles:
  - dev / ci: local filesystem, parquet, dataset ol_warehouse_<profile>_raw
  - test: ephemeral tmp filesystem, parquet, dataset ol_warehouse_test_raw
  - qa / production: s3://ol-data-lake-raw-<profile>/<source> with the Glue
    catalog, iceberg, dataset ol_warehouse_<profile>_raw
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Literal

import dlt
from dlt.destinations import filesystem

# Point dlt at this project's .dlt/ (config.toml, .pyiceberg.yaml) and pyiceberg
# at the Glue catalog config, unless the runtime (e.g. Docker) already set them.
_PROJECT_DIR = Path(__file__).resolve().parent.parent
_DLT_DIR = _PROJECT_DIR / ".dlt"
os.environ.setdefault("DLT_PROJECT_DIR", str(_PROJECT_DIR))
os.environ.setdefault("PYICEBERG_HOME", str(_DLT_DIR))

DEFAULT_PROFILE = "dev"

# Profiles that write Iceberg tables to S3 and register them in the Glue catalog.
ICEBERG_PROFILES = frozenset({"qa", "production"})

# dlt's table_format hint accepts only iceberg/delta/hive/native (NOT "parquet",
# and NOT None per the type stub). "native" means the destination's native format
# — plain parquet files for filesystem — which is what local/test/ci want.
TableFormat = Literal["iceberg", "delta", "hive", "native"]

# Object-store layout shared by every filesystem destination (matches the
# per-source [destination.*] blocks that used to live in .dlt/config.toml).
_LAYOUT = "{table_name}/{load_id}.{file_id}.{ext}"

# Schema contract for JSON API sources (mit_climate, mitpe, mit_edx_programs).
# New tables/columns evolve freely — upstream APIs add fields routinely and
# dropping them would be worse than a wider raw table. A TYPE FLIP on an
# existing column is frozen (fails the load loudly) rather than silently
# widening/retyping the column, since dbt casts specific columns downstream and
# a silent retype there corrupts the raw table instead of erroring visibly.
JSON_API_SCHEMA_CONTRACT: dict[str, str] = {
    "tables": "evolve",
    "columns": "evolve",
    "data_type": "freeze",
}


def active_profile() -> str:
    """Return the active dlt profile from ``DLT_PROFILE`` (default ``dev``)."""
    return os.getenv("DLT_PROFILE", DEFAULT_PROFILE)


def active_table_format() -> TableFormat:
    """Return the table format for the active profile.

    ``"iceberg"`` for qa/production, ``"native"`` (plain filesystem parquet) for
    dev/ci/test. Use this in ``@dlt.resource(..., table_format=...)`` so a source
    body never branches on the environment itself.
    """
    return "iceberg" if active_profile() in ICEBERG_PROFILES else "native"


def dataset_name(profile: str | None = None) -> str:
    """Return the dataset/Glue-database name for the (active) profile."""
    return f"ol_warehouse_{profile or active_profile()}_raw"


def bucket_root(profile: str | None = None) -> str:
    """Return the destination bucket root (no trailing slash) for the profile.

    qa/production resolve to the raw data lake bucket for that environment.
    Local profiles (dev/ci/test) resolve to a filesystem path; the
    ``OL_DLT_BUCKET_URL`` env var overrides it (the test harness sets this to a
    per-test tmp dir).
    """
    resolved = profile or active_profile()
    if resolved in ICEBERG_PROFILES:
        return f"s3://ol-data-lake-raw-{resolved}"
    override = os.getenv("OL_DLT_BUCKET_URL")
    if override:
        return override.rstrip("/")
    if resolved == "test":
        return Path(tempfile.gettempdir(), "ol_dlt_test").as_uri()
    return "file:///tmp/.dlt/data"


def destination_for(source_name: str, profile: str | None = None) -> Any:  # noqa: ANN401
    """Build the filesystem destination for ``source_name`` under the profile.

    Each source gets an isolated ``<bucket_root>/<source_name>`` prefix, matching
    the per-source destination isolation that used to be enumerated in
    ``.dlt/config.toml``.
    """
    root = bucket_root(profile)
    return filesystem(
        bucket_url=f"{root}/{source_name}",
        layout=_LAYOUT,
    )


def pipeline_for(
    source_name: str,
    *,
    pipeline_name: str | None = None,
    profile: str | None = None,
) -> dlt.Pipeline:
    """Return a ``dlt.pipeline`` bound to the active profile's destination.

    This is the factory that every source's module-level pipeline instance is
    built from, replacing the per-``loads.py`` destination/dataset branching.

    Args:
        source_name: Logical source name; also the per-source bucket prefix.
        pipeline_name: dlt pipeline name (defaults to ``source_name``).
        profile: Override the active profile (mainly for tests).
    """
    return dlt.pipeline(
        pipeline_name=pipeline_name or source_name,
        destination=destination_for(source_name, profile),
        dataset_name=dataset_name(profile),
        progress="log",
    )


def resolve_secret(explicit: str | None, env_var: str) -> str | None:
    """Resolve a credential lazily: explicit value, else ``env_var``.

    Looks ``env_var`` up via ``dlt.secrets`` (not ``os.getenv``) so the value is
    resolved through dlt's standard config providers (env, secrets.toml, vault)
    and marked as secret, which redacts it from dlt's run traces and logs.

    Call this inside a resource body (not at import) so a module imports cleanly
    when secrets are absent in local development.
    """
    return explicit if explicit is not None else dlt.secrets.get(env_var)


def require_secrets(**named_values: str | None) -> dict[str, str]:
    """Return ``named_values`` if all are truthy, else raise a clear error.

    Centralizes the "resolve credentials lazily, fail loudly if missing" pattern
    that each authenticated source used to inline.
    """
    missing = [name for name, value in named_values.items() if not value]
    if missing:
        msg = f"Missing required credentials: {', '.join(missing)}"
        raise ValueError(msg)
    return {name: value for name, value in named_values.items() if value is not None}


# Minimum fraction of the last successful load's row count a "replace" fetch
# must reach before it's trusted to overwrite the table.
REPLACE_ROW_COUNT_FLOOR = 0.8


def guard_against_replace_truncation(
    resource_name: str, row_count: int, floor_ratio: float = REPLACE_ROW_COUNT_FLOOR
) -> None:
    """Raise if ``row_count`` looks like a truncated fetch, else record it.

    For a fetch-all source on ``write_disposition="replace"``, a transient
    partial failure (e.g. a paginated API returning an empty page early) still
    looks like a successful fetch and would silently truncate the table to
    whatever was collected so far. Call this with the FULL row count *before*
    yielding any rows: it compares against the last successful load's count
    and raises rather than letting the load proceed if the count dropped by
    more than ``floor_ratio``. Recording the new baseline only happens by
    returning normally, and dlt only persists state alongside a load that
    completes successfully — so a raised guard leaves the previously loaded
    table untouched.

    Uses ``dlt.current.source_state()`` under a dedicated key rather than
    ``dlt.current.resource_state()``: dlt resets resource-scoped state for
    every ``write_disposition="replace"`` resource before each extraction (by
    design — replace is meant to be stateless), which would silently discard
    the row-count baseline on every run. Source state isn't subject to that
    reset, so the baseline survives across runs.
    """
    floor_state = dlt.current.source_state().setdefault("_replace_row_count_floor", {})
    last_count = floor_state.get(resource_name)
    if last_count and row_count < last_count * floor_ratio:
        msg = (
            f"{resource_name}: fetched {row_count} rows, below "
            f"{floor_ratio:.0%} of the last successful load's {last_count} "
            "rows -- refusing to replace the table with a likely-truncated "
            "result."
        )
        raise ValueError(msg)
    floor_state[resource_name] = row_count

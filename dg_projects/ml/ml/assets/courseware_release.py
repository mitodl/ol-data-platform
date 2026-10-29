"""Knowledge releases: one validated snapshot of every learner-facing projection.

Resolution, description and graph each write their own runs, and the working dbt
models read the newest of each. A learner-facing projection must not: a resolution
merge followed by a failed graph build would put new concepts beside an old graph,
and Learn and a tenant reading the stages separately could disagree.

courseware_knowledge_release copies the candidate projections dbt builds
(int__courseware__release_*) into release-keyed tables under the manifest they were
built from (int__courseware__release_manifest), and validates the candidate. Its
check blocks, and courseware_release_publication moves the published pointer only
to a valid release. The Learn views select through that pointer, so a promotion
switches them together, and a candidate that fails validation, or a stage that
fails before one is built, leaves the last published release in place. Releases
are never deleted, so publishing an earlier one is a rollback.
"""

import hashlib
import json
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

import polars as pl
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetCheckSpec,
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    Config,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)
from ml.lib.warehouse import read_table, require_table, warehouse_database
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.failures import permanent_failure
from pydantic import Field

MANIFEST_KEY = AssetKey(["intermediate", "int__courseware__release_manifest"])
RELEASE_KEY = AssetKey(["intermediate", "courseware_release"])
PUBLICATION_KEY = AssetKey(["intermediate", "courseware_release_publication"])
# Each release table and the candidate model it copies, sorted by these keys for
# the checksum. Written in this order; the release row goes last, so a release
# row means every projection row of that release was written.
PROJECTIONS = {
    "courseware_release_concept": (
        AssetKey(["intermediate", "int__courseware__release_concepts"]),
        ("concept_id",),
    ),
    "courseware_release_resource_knowledge": (
        AssetKey(["intermediate", "int__courseware__release_resource_knowledge"]),
        ("etl_source", "readable_id"),
    ),
}
RELEASE_TABLES = (*PROJECTIONS, RELEASE_KEY.path[-1])
# The manifest columns copied onto the release row.
MANIFEST_SCHEMA = {
    "resolution_run_id": pl.String,
    "resolver_id": pl.String,
    "resolution_prompt_version": pl.String,
    "sense_assignment_version": pl.String,
    "graph_run_id": pl.String,
    "graph_version": pl.String,
    "graph_resolution_run_id": pl.String,
    "is_graph_current": pl.Boolean,
    "description_prompt_version": pl.String,
    "undescribed_concept_count": pl.Int64,
    "extraction_selection_checksum": pl.String,
    "extraction_count": pl.Int64,
    "source_policy_version": pl.String,
}
RELEASE_SCHEMA = {
    "release_id": pl.String,
    "built_at": pl.Datetime(time_zone="UTC"),
    "candidate_checksum": pl.String,
    "output_schema_version": pl.String,
    **MANIFEST_SCHEMA,
    "concept_count": pl.Int64,
    "resource_count": pl.Int64,
    "is_valid": pl.Boolean,
    "validation_errors_json": pl.String,
}
PUBLICATION_SCHEMA = {
    "release_id": pl.String,
    "published_at": pl.Datetime(time_zone="UTC"),
    "previous_release_id": pl.String,
    "publication_kind": pl.String,
}
_OUTPUT_METADATA = {
    "schema": warehouse_database("intermediate"),
    "write_mode": "append",
    "schema_update_mode": "update",
}


def validate_candidate(manifest: dict[str, Any], concepts: pl.DataFrame) -> list[str]:
    """Say why a candidate cannot be published.

    :param manifest: the int__courseware__release_manifest row
    :param concepts: the candidate's int__courseware__release_concepts rows
    :returns: one reason per failed condition; empty when the candidate is valid
    :rtype: list[str]
    """
    errors = []
    if manifest["resolution_run_id"] is None:
        errors.append("no resolution run")
    if not manifest["is_graph_current"]:
        errors.append(
            f"graph run {manifest['graph_run_id']} was computed over resolution run "
            f"{manifest['graph_resolution_run_id']}, not "
            f"{manifest['resolution_run_id']}"
        )
    if manifest["description_prompt_version"] is None:
        errors.append("courseware_description_approval approves no description prompt")
    if manifest["undescribed_concept_count"]:
        errors.append(
            f"{manifest['undescribed_concept_count']} concepts have no description "
            f"under prompt {manifest['description_prompt_version']}"
        )
    if not concepts.height:
        # Most likely a policy or review mistake; publishing it would empty every
        # learner-facing projection.
        errors.append("no accepted concept from a source-eligible run")
    unaccepted = concepts.filter(pl.col("review_status") != "accepted").height
    if unaccepted:
        errors.append(f"{unaccepted} concepts are not accepted")
    return errors


def output_schema_version(projections: dict[str, pl.DataFrame]) -> str:
    """Hash every projection's column names and types."""
    schema = {
        table: [[name, str(dtype)] for name, dtype in frame.schema.items()]
        for table, frame in sorted(projections.items())
    }
    return hashlib.sha256(json.dumps(schema).encode()).hexdigest()[:12]


def candidate_checksum(
    manifest: dict[str, Any], projections: dict[str, pl.DataFrame]
) -> str:
    """Hash the manifest and every projection row, independent of row order."""
    digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode())
    for table, frame in sorted(projections.items()):
        keys = list(PROJECTIONS[table][1])
        digest.update(table.encode())
        digest.update(
            frame.sort(keys).select(sorted(frame.columns)).write_ndjson().encode()
        )
    return digest.hexdigest()


def newest(rows: pl.LazyFrame | None, column: str) -> dict[str, Any] | None:
    """Return the row with the greatest value of column, or None for no rows."""
    if rows is None:
        return None
    last = rows.sort(column).tail(1).collect()
    return last.row(0, named=True) if last.height else None


def release_check(release: dict[str, Any]) -> AssetCheckResult:
    """Pass when the release passed validation, so it may be published."""
    return AssetCheckResult(
        asset_key=RELEASE_KEY,
        check_name="release_candidate_valid",
        passed=release["is_valid"],
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "release_id": MetadataValue.text(release["release_id"]),
            "validation_errors": MetadataValue.json(
                json.loads(release["validation_errors_json"])
            ),
        },
    )


@multi_asset(
    group_name="courseware",
    deps=[MANIFEST_KEY, *(key for key, _ in PROJECTIONS.values())],
    outs={
        table: AssetOut(
            key=AssetKey(["intermediate", table]),
            io_manager_key="io_manager",
            metadata=_OUTPUT_METADATA,
            automation_condition=upstream_or_code_changes(),
            # An unchanged candidate is not written again.
            is_required=False,
        )
        for table in RELEASE_TABLES
    },
    check_specs=[
        AssetCheckSpec(
            "release_candidate_valid",
            asset=RELEASE_KEY,
            description=(
                "The newest release candidate is coherent and complete: its graph "
                "was computed over its resolution run, and every accepted concept "
                "is described under the approved prompt."
            ),
            blocking=True,
        )
    ],
)
def courseware_knowledge_release(context: AssetExecutionContext):
    """Write the candidate projections and manifest as a new release, and validate it.

    A candidate whose manifest and rows match the newest release is not written
    again. An invalid candidate is still written, for review, but its check fails
    and courseware_release_publication never publishes it.
    """
    manifest_rows = require_table("intermediate", MANIFEST_KEY.path[-1]).collect()
    if manifest_rows.height != 1:
        msg = f"{MANIFEST_KEY.path[-1]} has {manifest_rows.height} rows, not one"
        raise permanent_failure(msg)
    manifest = {column: manifest_rows[column][0] for column in MANIFEST_SCHEMA}
    projections = {
        table: require_table("intermediate", key.path[-1]).collect()
        for table, (key, _) in PROJECTIONS.items()
    }
    checksum = candidate_checksum(manifest, projections)
    last = newest(read_table("intermediate", RELEASE_KEY.path[-1]), "built_at")
    if last and last["candidate_checksum"] == checksum:
        context.log.info(
            "The candidate is release %s's; nothing written", last["release_id"]
        )
        yield release_check(last)
        return

    errors = validate_candidate(manifest, projections["courseware_release_concept"])
    release_id = str(uuid.uuid4())
    for table, frame in projections.items():
        yield Output(
            frame.select(pl.lit(release_id).alias("release_id"), pl.all()),
            output_name=table,
            metadata={"row_count": MetadataValue.int(frame.height)},
        )
    release = pl.DataFrame(
        [
            {
                "release_id": release_id,
                "built_at": datetime.now(tz=UTC),
                "candidate_checksum": checksum,
                "output_schema_version": output_schema_version(projections),
                **manifest,
                "concept_count": projections["courseware_release_concept"].height,
                "resource_count": projections[
                    "courseware_release_resource_knowledge"
                ].height,
                "is_valid": not errors,
                "validation_errors_json": json.dumps(errors),
            }
        ],
        schema=RELEASE_SCHEMA,
    )
    row = release.row(0, named=True)
    context.log.info("Release %s, validation errors: %s", release_id, errors)
    yield Output(
        release,
        output_name=RELEASE_KEY.path[-1],
        metadata={
            "release_id": MetadataValue.text(release_id),
            "is_valid": MetadataValue.bool(not errors),
            "resolution_run_id": MetadataValue.text(str(manifest["resolution_run_id"])),
            "graph_run_id": MetadataValue.text(str(manifest["graph_run_id"])),
        },
    )
    yield release_check(row)


class ReleasePublicationConfig(Config):
    release_id: str | None = Field(
        default=None,
        description=(
            "Publish this release, e.g. an earlier one to roll back. It must have "
            "passed validation. Unset publishes the newest valid release built "
            "since the pointer last moved, so a rollback holds until a new "
            "release is built."
        ),
    )


def release_to_publish(
    releases: pl.DataFrame,
    current: dict[str, Any] | None,
    requested: str | None,
) -> str | None:
    """Choose the release to publish, or None to leave the pointer where it is.

    :param releases: every courseware_release row
    :param current: the newest courseware_release_publication row, if any
    :param requested: a release the run config names
    :returns: the release id to publish
    :rtype: str | None
    :raises PermanentFailure: when the requested release is unknown or invalid
    """
    current_id = current["release_id"] if current else None
    if requested is not None:
        named = releases.filter(pl.col("release_id") == requested)
        if not named.height or not named["is_valid"][0]:
            msg = f"Release {requested} does not exist or did not pass validation"
            raise permanent_failure(msg)
        return None if requested == current_id else requested
    valid = releases.filter(pl.col("is_valid"))
    if current is not None:
        valid = valid.filter(pl.col("built_at") > current["published_at"])
    target = newest(valid.lazy(), "built_at")
    if target is None or target["release_id"] == current_id:
        return None
    return target["release_id"]


@asset(
    key=PUBLICATION_KEY,
    group_name="courseware",
    deps=[RELEASE_KEY],
    io_manager_key="io_manager",
    metadata=_OUTPUT_METADATA,
    automation_condition=upstream_or_code_changes(),
    # Nothing is written when the pointer does not move.
    output_required=False,
)
def courseware_release_publication(
    context: AssetExecutionContext, config: ReleasePublicationConfig
) -> Iterator[Output[pl.DataFrame]]:
    """Move the published pointer to a valid release.

    Appends one row naming the release; int__courseware__published_release reads
    the newest, and every learner-facing projection selects through it.
    """
    releases = require_table("intermediate", RELEASE_KEY.path[-1]).collect()
    current = newest(
        read_table("intermediate", PUBLICATION_KEY.path[-1]), "published_at"
    )
    target = release_to_publish(releases, current, config.release_id)
    if target is None:
        context.log.info(
            "No release to publish; %s stays published",
            current["release_id"] if current else "nothing",
        )
        return
    publication = pl.DataFrame(
        [
            {
                "release_id": target,
                "published_at": datetime.now(tz=UTC),
                "previous_release_id": current["release_id"] if current else None,
                "publication_kind": "requested" if config.release_id else "automatic",
            }
        ],
        schema=PUBLICATION_SCHEMA,
    )
    context.log.info("Publishing release %s", target)
    yield Output(
        publication,
        metadata={
            "release_id": MetadataValue.text(target),
            "previous_release_id": MetadataValue.text(
                str(current["release_id"]) if current else ""
            ),
        },
    )

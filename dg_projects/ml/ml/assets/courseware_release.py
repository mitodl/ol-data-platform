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
    Failure,
    MetadataValue,
    Output,
    asset,
    multi_asset,
)
from ml.lib.courseware.describe import current_input_checksums
from ml.lib.warehouse import read_table, require_table, warehouse_database
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.failures import permanent_failure
from pydantic import Field

MANIFEST_KEY = AssetKey(["intermediate", "int__courseware__release_manifest"])
DESCRIPTION_INPUTS_KEY = AssetKey(
    ["intermediate", "int__courseware__concept_description_inputs"]
)
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


def validate_candidate(
    manifest: dict[str, Any],
    concepts: pl.DataFrame,
    input_checksums: dict[str, str],
    published_policy_version: str | None,
) -> list[str]:
    """Say why a candidate cannot be published.

    :param manifest: the int__courseware__release_manifest row
    :param concepts: the candidate's int__courseware__release_concepts rows
    :param input_checksums: each concept's current description input checksum
        (ml.lib.courseware.describe.current_input_checksums)
    :param published_policy_version: the published release's source policy version,
        or None when nothing is published
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
    # A description written from inputs that have since changed (a run made
    # ineligible, a merge) still says what the old inputs said.
    stale = sum(
        1
        for concept_id, checksum in concepts.select(
            "concept_id", "description_input_checksum"
        ).iter_rows()
        if checksum is not None
        and concept_id in input_checksums
        and input_checksums[concept_id] != checksum
    )
    if stale:
        errors.append(f"{stale} concepts' descriptions predate their current inputs")
    # An empty candidate under the published release's policy is most likely a
    # review mistake, and would empty every learner-facing projection. Under a
    # changed policy it is the policy retracting what it no longer allows.
    if (
        not concepts.height
        and published_policy_version == (manifest["source_policy_version"])
    ):
        errors.append(
            "no accepted concept from a source-eligible run, which would empty the "
            "published release"
        )
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


def published_release() -> dict[str, Any] | None:
    """Return the courseware_release row the pointer names, or None."""
    current = newest(
        read_table("intermediate", PUBLICATION_KEY.path[-1]), "published_at"
    )
    if current is None:
        return None
    return (
        require_table("intermediate", RELEASE_KEY.path[-1])
        .filter(pl.col("release_id") == current["release_id"])
        .collect()
        .row(0, named=True)
    )


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
    # The manifest alone: dbt builds it only after both projections built in the
    # same run, so a run that rebuilt one projection and failed the other does not
    # request a release. The projections and description inputs are read too.
    deps=[MANIFEST_KEY],
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
    again, and nor is an empty one while nothing is published. An invalid
    candidate is still written, for review, but its check fails and
    courseware_release_publication never publishes it.
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
    counts = {
        "courseware_release_concept": manifest_rows["concept_count"][0],
        "courseware_release_resource_knowledge": manifest_rows["resource_count"][0],
    }
    if any(projections[table].height != count for table, count in counts.items()):
        msg = (
            "The candidate projections changed since the manifest was built "
            f"({ {table: frame.height for table, frame in projections.items()} } rows, "
            f"manifest {counts}); the next dbt build requests another release"
        )
        raise Failure(msg)
    published = published_release()
    if not projections["courseware_release_concept"].height and published is None:
        # Until a reviewer accepts a concept every candidate is empty, and failing
        # each automation request for it would only add noise.
        context.log.info("No accepted concept yet and nothing published; no release")
        yield AssetCheckResult(
            asset_key=RELEASE_KEY,
            check_name="release_candidate_valid",
            passed=True,
            metadata={"note": MetadataValue.text("nothing to release yet")},
        )
        return
    checksum = candidate_checksum(manifest, projections)
    last = newest(read_table("intermediate", RELEASE_KEY.path[-1]), "built_at")
    if last and last["candidate_checksum"] == checksum:
        context.log.info(
            "The candidate is release %s's; nothing written", last["release_id"]
        )
        yield release_check(last)
        return

    errors = validate_candidate(
        manifest,
        projections["courseware_release_concept"],
        current_input_checksums(
            require_table("intermediate", DESCRIPTION_INPUTS_KEY.path[-1]).collect()
        ),
        published["source_policy_version"] if published else None,
    )
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
            "passed validation under the current source policy. Unset publishes "
            "the newest valid, never-published release built after the published "
            "one, so a rollback holds until a new release is built."
        ),
    )


def release_to_publish(
    releases: pl.DataFrame,
    published_ids: list[str],
    requested: str | None,
    policy_version: str | None,
) -> str | None:
    """Choose the release to publish, or None to leave the pointer where it is.

    Automatic publication takes the newest valid release under the current source
    policy that has never been published and was built after the published one,
    so a rollback holds until a newer release is built.

    :param releases: every courseware_release row
    :param published_ids: the release of each publication row, oldest first
    :param requested: a release the run config names
    :param policy_version: the current source policy version (the manifest's)
    :returns: the release id to publish
    :rtype: str | None
    :raises PermanentFailure: when the requested release is unknown, invalid or
        built under another source policy
    """
    current_id = published_ids[-1] if published_ids else None
    if requested is not None:
        named = releases.filter(pl.col("release_id") == requested)
        if (
            not named.height
            or not named["is_valid"][0]
            or named["source_policy_version"][0] != policy_version
        ):
            msg = (
                f"Release {requested} does not exist, did not pass validation, or "
                f"was built under another source policy than {policy_version}"
            )
            raise permanent_failure(msg)
        return None if requested == current_id else requested
    candidates = releases.filter(
        pl.col("is_valid")
        & (pl.col("source_policy_version") == policy_version)
        & ~pl.col("release_id").is_in(published_ids)
    )
    if current_id is not None:
        current_built = releases.filter(pl.col("release_id") == current_id)["built_at"][
            0
        ]
        candidates = candidates.filter(pl.col("built_at") > current_built)
    target = newest(candidates.lazy(), "built_at")
    return target["release_id"] if target else None


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
    stored = read_table("intermediate", RELEASE_KEY.path[-1])
    if stored is None:
        context.log.info("No release has been built yet")
        return
    releases = stored.collect()
    publications = read_table("intermediate", PUBLICATION_KEY.path[-1])
    published_ids = (
        publications.sort("published_at").collect()["release_id"].to_list()
        if publications is not None
        else []
    )
    policy_version = require_table("intermediate", MANIFEST_KEY.path[-1]).collect()[
        "source_policy_version"
    ][0]
    target = release_to_publish(
        releases, published_ids, config.release_id, policy_version
    )
    current_id = published_ids[-1] if published_ids else None
    if target is None:
        context.log.info(
            "No release to publish; %s stays published", current_id or "nothing"
        )
        return
    publication = pl.DataFrame(
        [
            {
                "release_id": target,
                "published_at": datetime.now(tz=UTC),
                "previous_release_id": current_id,
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
            "previous_release_id": MetadataValue.text(current_id or ""),
        },
    )

"""Upload dbt artifacts to S3 for OpenMetadata ingestion.

OpenMetadata's dbt connector reads manifest.json, catalog.json, and
run_results.json from S3 to enrich table metadata with dbt descriptions,
lineage, tags, and test results.

This asset runs after `full_dbt_project` completes and uploads the generated
artifacts from the dbt target directory to a well-known S3 prefix.
"""

import json
from datetime import UTC, datetime

import boto3
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from ol_orchestrate.lib.constants import DAGSTER_ENV

from lakehouse.assets.lakehouse.dbt import DBT_REPO_DIR

ARTIFACT_BUCKET = f"ol-data-lake-landing-zone-{DAGSTER_ENV}"
ARTIFACT_PREFIX = "dbt-artifacts"
ARTIFACT_FILES = ("manifest.json", "catalog.json", "run_results.json")

# In dev/ci, use the sandbox bucket
if DAGSTER_ENV in ("dev", "ci"):
    ARTIFACT_BUCKET = "ol-devops-sandbox"
    ARTIFACT_PREFIX = (
        f"pipeline-storage{'-ci' if DAGSTER_ENV == 'ci' else ''}/dbt-artifacts"
    )


@asset(
    group_name="openmetadata",
    description=(
        "Upload dbt artifacts (manifest.json, catalog.json, run_results.json) "
        "to S3 for consumption by OpenMetadata's dbt connector. "
        "Run this after dbt build completes to keep OpenMetadata in sync."
    ),
)
def dbt_artifacts_upload(context: AssetExecutionContext) -> MaterializeResult:
    """Upload dbt build artifacts to S3 after dbt build completes."""
    target_dir = DBT_REPO_DIR / "target"
    s3 = boto3.client("s3")
    uploaded: list[str] = []
    skipped: list[str] = []
    timestamp = datetime.now(tz=UTC).isoformat()

    for filename in ARTIFACT_FILES:
        artifact_path = target_dir / filename
        if not artifact_path.exists():
            context.log.warning("dbt artifact not found: %s", artifact_path)
            skipped.append(filename)
            continue

        s3_key = f"{ARTIFACT_PREFIX}/{filename}"
        context.log.info(
            "Uploading %s to s3://%s/%s", filename, ARTIFACT_BUCKET, s3_key
        )
        s3.upload_file(
            Filename=str(artifact_path),
            Bucket=ARTIFACT_BUCKET,
            Key=s3_key,
            ExtraArgs={"ContentType": "application/json"},
        )
        uploaded.append(filename)

    # Also upload a versioned copy for audit trail
    if uploaded and (target_dir / "manifest.json").exists():
        manifest_data = json.loads((target_dir / "manifest.json").read_text())
        invocation_id = manifest_data.get("metadata", {}).get(
            "invocation_id", "unknown"
        )
        for filename in uploaded:
            versioned_key = f"{ARTIFACT_PREFIX}/history/{invocation_id}/{filename}"
            s3.upload_file(
                Filename=str(target_dir / filename),
                Bucket=ARTIFACT_BUCKET,
                Key=versioned_key,
                ExtraArgs={"ContentType": "application/json"},
            )

    context.log.info(
        "dbt artifact upload complete: %d uploaded, %d skipped",
        len(uploaded),
        len(skipped),
    )

    return MaterializeResult(
        metadata={
            "uploaded": MetadataValue.json(uploaded),
            "skipped": MetadataValue.json(skipped),
            "s3_bucket": MetadataValue.text(ARTIFACT_BUCKET),
            "s3_prefix": MetadataValue.text(ARTIFACT_PREFIX),
            "timestamp": MetadataValue.text(timestamp),
            "environment": MetadataValue.text(DAGSTER_ENV),
        },
    )

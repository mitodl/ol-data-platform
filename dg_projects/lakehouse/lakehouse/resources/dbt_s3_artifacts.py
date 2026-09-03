import hashlib
import json
from pathlib import Path
from typing import Any

import boto3
from botocore.exceptions import ClientError
from dagster import AssetExecutionContext, ConfigurableResource, OpExecutionContext
from pydantic import Field, field_validator

# What S3 returns for an object that is not there. GetObject answers NoSuchKey;
# a bucket-level miss answers NoSuchBucket; some paths (and several
# S3-compatible endpoints) answer a bare "404" instead. Anything else -- 403,
# throttling, a 5xx -- is a failed read, not an absent object.
_MISSING_OBJECT_CODES = frozenset({"NoSuchKey", "NoSuchBucket", "404"})


class DbtS3ArtifactsResource(ConfigurableResource):
    """Uploads dbt build artifacts to S3 for consumption by OpenMetadata OMJobs."""

    s3_bucket: str = Field(description="S3 bucket to upload dbt artifacts into.")
    s3_prefix: str = Field(
        default="openmetadata/dbt-artifacts",
        description=(
            "Key prefix under which artifacts are stored in the bucket. "
            "Trailing slashes are stripped automatically."
        ),
    )

    @field_validator("s3_prefix")
    @classmethod
    def strip_trailing_slash(cls, v: str) -> str:
        """Normalize prefix so keys are always constructed as prefix/filename."""
        return v.rstrip("/")

    def read_json_artifact(
        self,
        artifact: str,
        context: AssetExecutionContext | OpExecutionContext,
    ) -> Any | None:
        """Read and decode a JSON artifact at ``<prefix>/<artifact>``.

        Returns None only when the object genuinely is not there, which callers
        read as "no prior state". Every other failure raises.

        Both narrowings matter because callers use the returned state to decide
        whether something needs repairing, and then overwrite it. A denied,
        throttled, or 5xx read reported as "absent" would let the run record a
        fresh baseline over a comparison it never actually made, so whatever
        had drifted is now indistinguishable from steady state. Undecodable
        content is the same failure with a different cause: the object exists
        and says something, and treating it as silence blesses a baseline
        nothing was checked against. Delete the object deliberately to reset
        the baseline; do not let a bad read do it by accident.
        """
        key = f"{self.s3_prefix}/{artifact}"
        try:
            response = boto3.client("s3").get_object(Bucket=self.s3_bucket, Key=key)
            body = response["Body"].read()
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") not in _MISSING_OBJECT_CODES:
                raise
            context.log.info("No existing s3://%s/%s", self.s3_bucket, key)
            return None
        try:
            return json.loads(body)
        except ValueError as exc:
            msg = (
                f"s3://{self.s3_bucket}/{key} exists but is not valid JSON. It is "
                "written only by this code, so this means it was corrupted or "
                "hand-edited. Delete the object to reset the baseline -- the next "
                "run will then record a fresh one -- rather than letting a run "
                "treat it as absent and bless a baseline it never compared against."
            )
            raise ValueError(msg) from exc

    def write_json_artifact(
        self,
        artifact: str,
        payload: Any,
        context: AssetExecutionContext | OpExecutionContext,
    ) -> None:
        """Write *payload* as JSON to ``<prefix>/<artifact>``, overwriting it."""
        key = f"{self.s3_prefix}/{artifact}"
        context.log.info("Writing s3://%s/%s", self.s3_bucket, key)
        boto3.client("s3").put_object(
            Body=json.dumps(payload, sort_keys=True).encode(),
            Bucket=self.s3_bucket,
            Key=key,
        )

    def upload_artifacts(
        self,
        target_path: Path,
        artifacts: list[str],
        context: AssetExecutionContext | OpExecutionContext,
    ) -> None:
        """Upload artifact files from *target_path* to S3 with content-based versioning.

        - ``run_results.json`` is stored at a per-run versioned key
          (``<prefix>/runs/<run_id>/run_results.json``) so every incremental and
          full run is captured without overwriting prior results.

        - Other artifacts (``manifest.json``, ``catalog.json``) are uploaded only
          when their content has changed. S3's ETag equals the hex MD5 of the body
          for single-part objects (all small JSON files), so a ``HeadObject`` ETag
          comparison is sufficient to skip redundant writes.

        Raises ``FileNotFoundError`` if any requested artifact is absent.
        """
        s3 = boto3.client("s3")
        for artifact in artifacts:
            local_path = target_path / artifact
            if not local_path.exists():
                msg = f"dbt artifact not found at {local_path}"
                raise FileNotFoundError(msg)

            content = local_path.read_bytes()

            if artifact == "run_results.json":
                # Each run's results are stored at a unique key so that results
                # from incremental subset runs are all captured and never
                # overwrite each other.
                key = f"{self.s3_prefix}/runs/{context.run_id}/{artifact}"
                context.log.info(
                    "Uploading %s to s3://%s/%s", artifact, self.s3_bucket, key
                )
                s3.put_object(Body=content, Bucket=self.s3_bucket, Key=key)
            else:
                # Skip upload when the object content is unchanged.  For
                # single-part uploads (all files here are small JSON), S3's ETag
                # equals the hex-encoded MD5 of the body.
                key = f"{self.s3_prefix}/{artifact}"
                content_md5 = hashlib.md5(content).hexdigest()  # noqa: S324
                try:
                    head = s3.head_object(Bucket=self.s3_bucket, Key=key)
                    if head["ETag"].strip('"') == content_md5:
                        context.log.info(
                            "%s is unchanged (md5=%s), skipping upload",
                            artifact,
                            content_md5,
                        )
                        continue
                except ClientError:
                    pass  # Object does not yet exist; proceed with upload.
                context.log.info(
                    "Uploading %s (md5=%s) to s3://%s/%s",
                    artifact,
                    content_md5,
                    self.s3_bucket,
                    key,
                )
                s3.put_object(Body=content, Bucket=self.s3_bucket, Key=key)

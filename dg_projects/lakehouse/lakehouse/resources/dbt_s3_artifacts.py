import hashlib
from pathlib import Path

import boto3
from botocore.exceptions import ClientError
from dagster import AssetExecutionContext, ConfigurableResource
from pydantic import Field, field_validator


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

    def upload_artifacts(
        self,
        target_path: Path,
        artifacts: list[str],
        context: AssetExecutionContext,
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

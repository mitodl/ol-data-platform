from pathlib import Path

import boto3
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
        """Upload named artifact files from *target_path* to S3.

        Raises FileNotFoundError if any requested artifact is absent so that
        callers get a hard failure rather than silently stale metadata in S3.
        """
        s3 = boto3.client("s3")
        for artifact in artifacts:
            local_path = target_path / artifact
            if not local_path.exists():
                msg = f"dbt artifact not found at {local_path}"
                raise FileNotFoundError(msg)
            key = f"{self.s3_prefix}/{artifact}"
            context.log.info(f"Uploading {artifact} to s3://{self.s3_bucket}/{key}")  # noqa: G004
            s3.upload_file(str(local_path), self.s3_bucket, key)

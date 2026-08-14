from pathlib import Path
from typing import Any

from dagster import (
    ConfigurableIOManager,
    DagsterEventType,
    EventRecordsFilter,
    Failure,
    InputContext,
    MetadataValue,
    OutputContext,
)
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from pydantic import PrivateAttr
from s3fs import S3FileSystem
from upath import UPath

from ol_orchestrate.resources.secrets.vault import Vault

# How far back through an asset partition's materialization history to look for
# an event that records a `path`. Only guards against a handful of malformed
# events at the head of the history -- if the most recent dozen all lack a
# location there is a real problem to surface, not to paper over.
MATERIALIZATION_LOOKBACK = 10


class FileObjectIOManager(ConfigurableIOManager):
    path_prefix: str | None = None
    gcs_credentials: str | None = None
    gcs_project_id: str | None = None
    vault: Vault | None = None
    vault_gcs_token_path: str | None = None
    _gcs_fs: GCSFileSystem = PrivateAttr(default=None)
    _s3_fs: S3FileSystem = PrivateAttr(default=None)

    def load_input(self, context: InputContext) -> UPath:
        """Resolve an upstream asset partition to the object it was written to.

        The location is read back out of the upstream materialization event
        rather than recomputed, so this is only ever as trustworthy as the
        event log. Three things can go wrong, and all three used to surface as
        an opaque ``KeyError``/``IndexError`` inside the io manager or as a
        ``NoSuchKey`` from fsspec several frames later:

        * the partition has never been materialized,
        * the newest materialization carries no ``path`` metadata,
        * the recorded object is no longer in the bucket.

        None of those are fixable by running the step again, so each raises a
        ``Failure`` naming the asset, the partition and the path -- which is
        the actual win here, since the previous errors named none of them.

        ``allow_retries=False`` is set for correctness but does less than it
        sounds like: Dagster only consults it when the op or asset carries a
        ``RetryPolicy`` (see
        ``dagster._core.execution.plan.utils.op_execution_error_boundary``).
        The run-level auto-reexecution daemon decides from run status and the
        ``dagster/max_retries`` / ``dagster/retry_on_asset_or_op_failure``
        tags and never looks at it, so these failures are still re-run by
        ``run_retries``. Suppressing that would mean a run-tag or instance
        level change, which is deliberately out of scope here.
        """
        asset_label = context.asset_key.to_user_string()
        records = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                asset_key=context.asset_key,
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_partitions=[context.partition_key],
            ),
            limit=MATERIALIZATION_LOOKBACK,
        )
        if not records:
            raise Failure(
                description=(
                    f"No materialization of {asset_label} partition "
                    f"{context.partition_key!r} has been recorded, so there is "
                    "no location to load from. Materialize the upstream asset "
                    "for this partition first."
                ),
                allow_retries=False,
            )

        # Newest first. Deliberately does not fall back to an older event when
        # the newest one points at a missing object: these paths are content
        # hashed, so an earlier event is an earlier *version* of the data, and
        # silently loading it would trade a loud failure for stale results.
        # Only events with no location at all get skipped over.
        path_metadata = next(
            (
                metadata
                for record in records
                if (metadata := record.asset_materialization.metadata.get("path"))
                is not None
            ),
            None,
        )
        if path_metadata is None:
            raise Failure(
                description=(
                    f"None of the last {len(records)} materializations of "
                    f"{asset_label} partition {context.partition_key!r} recorded "
                    "a 'path'. The upstream asset emitted materialization events "
                    "without a location, so there is nothing to load."
                ),
                allow_retries=False,
            )

        asset_path = UPath(path_metadata.value)
        resolved_path = UPath(
            asset_path,
            **self.configure_path_fs(asset_path.protocol).storage_options,
        )
        if not resolved_path.exists():
            raise Failure(
                description=(
                    f"{asset_label} partition {context.partition_key!r} points at "
                    f"{asset_path}, which does not exist. The materialization "
                    "event outlived the object it describes -- re-materialize the "
                    "upstream asset to write it again."
                ),
                metadata={"path": MetadataValue.path(str(asset_path))},
                allow_retries=False,
            )
        return resolved_path

    def handle_output(self, context: OutputContext, obj: tuple[Path, str]) -> None:
        context.log.info("Writing contents of %s to %s", *obj)
        output_path = UPath(obj[1])
        output_path = UPath(
            obj[1], **self.configure_path_fs(output_path.protocol).storage_options
        )
        output_path.write_bytes(obj[0].read_bytes())
        context.add_output_metadata({"path": MetadataValue.path(str(output_path))})
        obj[0].unlink()

    def configure_path_fs(
        self, path_protocol
    ) -> S3FileSystem | GCSFileSystem | LocalFileSystem:
        proto_map = {
            "s3": self.configure_s3_fs,
            "gs": self.configure_gcs_fs,
            "gcs": self.configure_gcs_fs,
            "file": self.configure_local_fs,
        }
        return proto_map[path_protocol]()

    def configure_gcs_fs(self) -> GCSFileSystem:
        if not self._gcs_fs:
            token = self.gcs_credentials or self.vault_read_token()
            self._gcs_fs = GCSFileSystem(project=self.gcs_project_id, token=token)
        return self._gcs_fs

    def configure_s3_fs(self) -> S3FileSystem:
        if not self._s3_fs:
            self._s3_fs = S3FileSystem()
        return self._s3_fs

    def configure_local_fs(self) -> LocalFileSystem:
        return LocalFileSystem()

    def vault_read_token(self) -> str:
        kv_version = 1
        vault_mount, vault_path = self.vault_gcs_token_path.split("/", 1)  # type: ignore[union-attr]
        mount_config = self.vault.client.sys.read_mount_configuration(vault_mount)[  # type: ignore[union-attr]
            "data"
        ]
        if mount_version := mount_config.get("options", {}).get("version", None):
            kv_version = int(mount_version)
        self.vault.client.secrets.kv.default_kv_version = kv_version  # type: ignore[union-attr]
        if kv_version == 1:
            return self.vault.client.secrets.kv.v1.read_secret(  # type: ignore[union-attr]
                mount_point=vault_mount, path=vault_path
            )["data"]
        else:
            return self.vault.client.secrets.kv.v2.read_secret(  # type: ignore[union-attr]
                mount_point=vault_mount, path=vault_path
            )["data"]["data"]


class S3FileObjectIOManager(FileObjectIOManager):
    bucket: str | None = None

    def handle_output(self, context: OutputContext, obj: tuple[Path, str]) -> None:
        if self.bucket:
            dest_path = f"{self.bucket}/{self.path_prefix or ''}/{obj[1]}".replace(
                "//", "/"
            )
            obj = (obj[0], f"s3://{dest_path}")

        return super().handle_output(context, obj)


class DummyIOManager(ConfigurableIOManager):
    input_file_path: str | None

    def load_input(self, context: InputContext) -> Path:  # noqa: ARG002
        if self.input_file_path is None:
            msg = "input_file_path must be set"
            raise ValueError(msg)
        return Path(self.input_file_path)

    def handle_output(self, context: "OutputContext", obj: Any) -> None: ...


class LocalFileObjectIOManager(FileObjectIOManager):
    bucket: str | None = None

    def handle_output(self, context: OutputContext, obj: tuple[Path, str]) -> None:
        if self.bucket:
            dest_path = Path(
                f"{self.bucket}/{self.path_prefix or ''}/{obj[1]}".replace("//", "/")
            )
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            obj = (obj[0], f"file://{dest_path}")

        return super().handle_output(context, obj)

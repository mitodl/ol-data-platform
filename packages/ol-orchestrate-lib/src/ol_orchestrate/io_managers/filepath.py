from pathlib import Path
from typing import Any

from dagster import (
    ConfigurableIOManager,
    DagsterEventType,
    EventRecordsFilter,
    InputContext,
    MetadataValue,
    OutputContext,
)
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from pydantic import PrivateAttr
from s3fs import S3FileSystem
from upath import UPath

from ol_orchestrate.lib.failures import PermanentFailure
from ol_orchestrate.resources.secrets.vault import Vault


class UpstreamObjectUnavailable(PermanentFailure):
    """The upstream materialization does not point at a readable object.

    A distinct class rather than a bare ``PermanentFailure`` so the three ways
    this happens -- no materialization, no path in its metadata, a path whose
    object is gone -- group as one Sentry issue, instead of dissolving into
    whatever the reader raised downstream. Nothing a rerun does changes any of
    them, which is why they carry ``allow_retries=False`` and stop
    ``run_retries`` via the ``stop_run_retries`` hook.

    Which asset and partition broke goes in ``metadata``, never in the
    description. Sentry titles an event from the exception message, so an
    interpolated partition key or S3 path gives every occurrence a different
    title and the issue shows whichever one arrived last -- the asset key and
    partition are already on the event as ``dagster_step`` and
    ``dagster_partition`` tags.
    """


class FileObjectIOManager(ConfigurableIOManager):
    path_prefix: str | None = None
    gcs_credentials: str | None = None
    gcs_project_id: str | None = None
    vault: Vault | None = None
    vault_gcs_token_path: str | None = None
    _gcs_fs: GCSFileSystem = PrivateAttr(default=None)
    _s3_fs: S3FileSystem = PrivateAttr(default=None)

    def load_input(self, context: InputContext) -> UPath:
        """Resolve the upstream materialization to a path that actually exists.

        The materialization record is a claim about the object store, not the
        object store. Trusting it unconditionally is what turned one missing S3
        key into ~368,000 failed runs: the manager handed back a path to nothing,
        the reader raised NoSuchKey deep in whatever library opened it, and the
        automation condition asked for the same run again. Checking here fails
        once, permanently, and names the key.
        """
        asset_dep = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                asset_key=context.asset_key,
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_partitions=[context.partition_key],
            ),
            limit=1,
        )
        location = {
            "asset_key": context.asset_key.to_user_string(),
            "partition": context.partition_key,
        }
        if not asset_dep:
            raise UpstreamObjectUnavailable(
                description=(
                    "No materialization recorded for the upstream partition, so "
                    "there is no path to load. The upstream needs to run before "
                    "this asset can."
                ),
                metadata=location,
                allow_retries=False,
            )

        path_metadata = asset_dep[0].asset_materialization.metadata.get("path")
        if path_metadata is None:
            raise UpstreamObjectUnavailable(
                description=(
                    "The latest materialization of the upstream partition "
                    "recorded no 'path' metadata. Whatever wrote it did not go "
                    "through this IO manager's handle_output."
                ),
                metadata=location,
                allow_retries=False,
            )

        asset_path = UPath(path_metadata.value)
        resolved_path = UPath(
            asset_path,
            **self.configure_path_fs(asset_path.protocol).storage_options,
        )
        if not resolved_path.exists():
            raise UpstreamObjectUnavailable(
                description=(
                    "The upstream partition recorded a path to an object that is "
                    "not there. The materialization outlived the object -- "
                    "expired by a lifecycle rule, deleted, or written to a "
                    "different bucket than the one recorded."
                ),
                metadata={
                    **location,
                    "missing_path": MetadataValue.text(str(resolved_path)),
                },
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

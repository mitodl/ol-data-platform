from pathlib import Path
from typing import Any, Optional

from dagster import (
    ConfigurableIOManager,
    DagsterEventType,
    EventRecordsFilter,
    InputContext,
    MetadataValue,
    Nothing,
    OutputContext,
)
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from pydantic import PrivateAttr
from s3fs import S3FileSystem
from upath import UPath

from ol_orchestrate.lib.dagster_types.files import DagsterPath
from ol_orchestrate.resources.secrets.vault import Vault


class FileObjectIOManager(ConfigurableIOManager):
    path_prefix: Optional[str] = None
    gcs_credentials: Optional[str] = None
    gcs_project_id: Optional[str] = None
    vault: Optional[Vault] = None
    vault_gcs_token_path: Optional[str] = None
    _gcs_fs: GCSFileSystem = PrivateAttr(default=None)
    _s3_fs: S3FileSystem = PrivateAttr(default=None)

    def load_input(self, context: InputContext) -> UPath:
        asset_dep = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                asset_key=context.asset_key,
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_partitions=[context.partition_key],
            ),
            limit=1,
        )[0]

        asset_path = UPath(asset_dep.asset_materialization.metadata["path"].value)
        return UPath(
            asset_path,
            **self.configure_path_fs(asset_path.protocol).storage_options,
        )

    def handle_output(self, context: OutputContext, obj: tuple[Path, str]) -> Nothing:
        context.log.info("Writing contents of %s to %s", *obj)
        output_path = UPath(obj[1])
        output_path = UPath(
            obj[1], **self.configure_path_fs(output_path.protocol).storage_options
        )
        output_path.write_bytes(obj[0].read_bytes())
        context.add_output_metadata({"path": MetadataValue.path(output_path)})
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
    bucket: Optional[str] = None

    def handle_output(self, context: OutputContext, obj: tuple[Path, str]) -> Nothing:
        if self.bucket:
            dest_path = f"{self.bucket}/{self.path_prefix or ''}/{obj[1]}".replace(
                "//", "/"
            )
            obj = (obj[0], f"s3://{dest_path}")

        return super().handle_output(context, obj)


class DummyIOManager(ConfigurableIOManager):
    input_file_path: Optional[str]

    def load_input(self, context: InputContext) -> DagsterPath:  # noqa: ARG002
        return DagsterPath(self.input_file_path)

    def handle_output(self, context: "OutputContext", obj: Any) -> None: ...


class LocalFileObjectIOManager(FileObjectIOManager):
    bucket: Optional[str] = None

    def handle_output(self, context: OutputContext, obj: tuple[Path, str]) -> Nothing:
        if self.bucket:
            dest_path = Path(
                f"{self.bucket}/{self.path_prefix or ''}/{obj[1]}".replace("//", "/")
            )
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            obj = (obj[0], f"file://{dest_path}")

        return super().handle_output(context, obj)

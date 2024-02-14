from typing import Optional

from dagster import (
    AssetKey,
    ConfigurableIOManager,
    DagsterEventType,
    EventRecordsFilter,
    InputContext,
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
    gcs_credentials: Optional[str] = None
    gcs_project_id: Optional[str] = None
    vault: Optional[Vault] = None
    vault_gcs_token_path: Optional[str] = None
    _gcs_fs: GCSFileSystem = PrivateAttr(default=None)
    _s3_fs: S3FileSystem = PrivateAttr(default=None)

    def _load_input(self, context: InputContext) -> UPath:
        context.log.info(f"Context info: {context.__dict__}")
        context.log.info(f"Input metadata: {context.metadata}")
        asset_dep = context.instance.get_event_records(
            event_records_filter=EventRecordsFilter(
                asset_key=AssetKey(("edxorg", "raw_data_archive")),
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
                asset_partitions=[context.partition_key],
            ),
            limit=1,
        )[0]

        asset_path = UPath(asset_dep.asset_materialization.metadata["path"].value)
        asset_path = UPath(
            asset_dep.asset_materialization.metadata["path"].value,
            storage_options=self.configure_path_fs(asset_path.protocol).storage_options,
        )
        return asset_path

    def load_input(self, context: InputContext) -> UPath:
        return self._load_input(context)

    def handle_output(self, context: OutputContext, obj: DagsterPath) -> Nothing:
        output_metadata = context.get_logged_metadata()
        output_path = UPath(output_metadata["path"].value)
        output_path.storage_options = self.configure_path_fs(
            output_path.protocol
        ).storage_options
        output_path.write_bytes(obj.read_bytes())

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
        vault_mount, vault_path = self.vault_gcs_token_path.split("/", 1)
        mount_config = self.vault.client.sys.read_mount_configuration(vault_mount)[
            "data"
        ]
        if mount_version := mount_config.get("options", {}).get("version", None):
            kv_version = int(mount_version)
        self.vault.client.secrets.kv.default_kv_version = kv_version
        if kv_version == 1:
            return self.vault.client.secrets.kv.v1.read_secret(
                mount_point=vault_mount, path=vault_path
            )["data"]
        else:
            return self.vault.client.secrets.kv.v2.read_secret(
                mount_point=vault_mount, path=vault_path
            )["data"]["data"]

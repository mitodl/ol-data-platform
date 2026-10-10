"""Unit tests for ol_dlt.config profile resolution."""

import os

import pytest

from ol_dlt import config


@pytest.mark.parametrize(
    ("profile", "expected_format"),
    [
        ("dev", "iceberg"),
        ("ci", "native"),
        ("test", "native"),
        ("qa", "iceberg"),
        ("production", "iceberg"),
    ],
)
def test_active_table_format(
    monkeypatch: pytest.MonkeyPatch, profile: str, expected_format: str
) -> None:
    monkeypatch.setenv("DLT_PROFILE", profile)
    assert config.active_profile() == profile
    assert config.active_table_format() == expected_format


def test_default_profile_is_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DLT_PROFILE", raising=False)
    assert config.active_profile() == "dev"
    assert config.active_table_format() == "iceberg"


@pytest.mark.parametrize(
    ("profile", "expected_dataset"),
    [
        ("dev", "ol_warehouse_local_raw"),
        ("ci", "ol_warehouse_ci_raw"),
        ("qa", "ol_warehouse_qa_raw"),
        ("production", "ol_warehouse_production_raw"),
    ],
)
def test_dataset_name(profile: str, expected_dataset: str) -> None:
    assert config.dataset_name(profile) == expected_dataset


@pytest.mark.parametrize(
    ("profile", "expected_root"),
    [
        ("qa", "s3://ol-data-lake-raw-qa"),
        ("production", "s3://ol-data-lake-raw-production"),
        ("dev", "s3://ol-data-lake-local/raw"),
        ("ci", "file:///tmp/.dlt/data"),
    ],
)
def test_bucket_root(
    monkeypatch: pytest.MonkeyPatch, profile: str, expected_root: str
) -> None:
    monkeypatch.delenv("OL_DLT_BUCKET_URL", raising=False)
    assert config.bucket_root(profile) == expected_root


def test_bucket_root_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OL_DLT_BUCKET_URL", "file:///custom/dir/")
    assert config.bucket_root("dev") == "file:///custom/dir"


def test_destination_per_source_isolation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLT_PROFILE", "production")
    dest = config.destination_for("oll")
    assert dest.config_params["bucket_url"] == "s3://ol-data-lake-raw-production/oll"


def test_dev_destination_is_the_local_lake(monkeypatch: pytest.MonkeyPatch) -> None:
    """``dev`` writes to the local object store through the local catalog.

    The endpoint and keys come from the catalog entry, so dlt's own files and
    the Iceberg tables cannot land in different stores.
    """
    monkeypatch.delenv("OL_DLT_BUCKET_URL", raising=False)
    monkeypatch.delenv("ICEBERG_CATALOG__ICEBERG_CATALOG_NAME", raising=False)
    monkeypatch.setenv(
        "PYICEBERG_CATALOG__OL_DATA_LAKE_LOCAL__S3__ENDPOINT", "http://rustfs.test:9000"
    )
    dest = config.destination_for("oll", "dev")
    assert dest.config_params["bucket_url"] == "s3://ol-data-lake-local/raw/oll"
    credentials = dest.config_params["credentials"]
    assert credentials.endpoint_url == "http://rustfs.test:9000"
    assert credentials.aws_access_key_id == "localdevaccess"
    assert os.environ["ICEBERG_CATALOG__ICEBERG_CATALOG_NAME"] == "ol-data-lake-local"


def test_dev_destination_keeps_a_catalog_already_chosen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("ICEBERG_CATALOG__ICEBERG_CATALOG_NAME", "another")
    config.destination_for("oll", "dev")
    assert os.environ["ICEBERG_CATALOG__ICEBERG_CATALOG_NAME"] == "another"


@pytest.mark.parametrize("profile", ["ci", "test", "qa", "production"])
def test_other_profiles_leave_the_catalog_alone(
    monkeypatch: pytest.MonkeyPatch, profile: str
) -> None:
    monkeypatch.delenv("ICEBERG_CATALOG__ICEBERG_CATALOG_NAME", raising=False)
    dest = config.destination_for("oll", profile)
    assert "credentials" not in dest.config_params
    assert "ICEBERG_CATALOG__ICEBERG_CATALOG_NAME" not in os.environ


def test_pipeline_for_binds_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DLT_PROFILE", "qa")
    pipeline = config.pipeline_for("oll")
    assert pipeline.pipeline_name == "oll"
    assert pipeline.dataset_name == "ol_warehouse_qa_raw"


def test_resolve_secret_prefers_explicit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EDX_API_CLIENT_ID", "from_env")
    assert config.resolve_secret("explicit", "EDX_API_CLIENT_ID") == "explicit"
    assert config.resolve_secret(None, "EDX_API_CLIENT_ID") == "from_env"
    assert config.resolve_secret(None, "DOES_NOT_EXIST") is None


def test_require_secrets_raises_on_missing() -> None:
    with pytest.raises(ValueError, match="client_secret"):
        config.require_secrets(client_id="x", client_secret=None)


def test_require_secrets_returns_when_complete() -> None:
    resolved = config.require_secrets(client_id="x", client_secret="y")
    assert resolved == {"client_id": "x", "client_secret": "y"}

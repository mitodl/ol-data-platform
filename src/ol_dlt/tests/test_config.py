"""Unit tests for ol_dlt.config profile resolution."""

import pytest

from ol_dlt import config


@pytest.mark.parametrize(
    ("profile", "expected_format"),
    [
        ("dev", "native"),
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
    assert config.active_table_format() == "native"


@pytest.mark.parametrize(
    ("profile", "expected_dataset"),
    [
        ("dev", "ol_warehouse_dev_raw"),
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
        ("dev", "file:///tmp/.dlt/data"),
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

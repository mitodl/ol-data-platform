import hashlib
import logging
import os
import zipfile
from pathlib import Path
from typing import Any, Literal

import httpx2 as httpx

from ol_orchestrate.resources.secrets.vault import Vault

DEFAULT_DRF_PAGE_TIMEOUT_SECONDS = 30.0


def authenticate_vault(dagster_env: str, vault_address: str) -> Vault:
    """
    Authenticate with Vault based on the dagster environment and authentication method.

    Parameters:
        dagster_env (str): The environment in which the Dagster service is running.
        vault_address (str): The address of the Vault server.

    Returns:
        Vault: An authenticated Vault client.
    """
    if dagster_env == "dev":
        if os.environ.get("GITHUB_TOKEN"):
            auth_method = "github"
            vault = Vault(vault_addr=vault_address, vault_auth_type=auth_method)
        else:
            auth_method = "oidc"
            vault = Vault(
                vault_addr=vault_address,
                vault_auth_type=auth_method,
                vault_role=os.environ.get("DAGSTER_VAULT_ROLE", "developer"),
            )
    else:
        vault_role = os.getenv("DAGSTER_VAULT_ROLE", "dagster")
        vault_mount = os.getenv("DAGSTER_VAULT_MOUNT", "k8s-data")
        vault = Vault(
            vault_addr=vault_address,
            vault_auth_type="kubernetes",
            vault_role=vault_role,
            auth_mount=vault_mount,
        )
    vault.authenticate()
    return vault


def s3_uploads_bucket(
    dagster_env: Literal["dev", "ci", "qa", "production"],
) -> dict[str, Any]:
    """
    Return the S3 bucket configuration based on the environment.

    Parameters:
        dagster_env (Literal): Environment name, one of "dev", "ci", "qa", or
            "production".

    Returns:
        dict: A dictionary with the S3 bucket and prefix for the specified environment.
    """
    bucket_map = {
        "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
        "ci": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage-ci"},
        "qa": {"bucket": "ol-data-lake-landing-zone-qa", "prefix": ""},
        "production": {
            "bucket": "ol-data-lake-landing-zone-production",
            "prefix": "",
        },
    }
    return bucket_map[dagster_env]


def compute_zip_content_hash(zip_path: Path, skip_filename: str) -> str:
    """Compute a SHA-256 hash of the contents of a ZIP file, skipping specified file
    that has volatile data (e.g., timestamps).

    Args:
        zip_path (Path): Path to the ZIP file.
        skip_filename (str): file name to exclude from hashing (e.g., imsmanifest.xml).

    Returns:
        str: Hex digest of the SHA-256 hash.
    """
    hasher = hashlib.new("sha256")

    with zipfile.ZipFile(zip_path, "r") as zip_file:
        for item in zip_file.infolist():
            hasher.update(item.filename.encode("utf-8"))
            if not item.is_dir() and item.filename != skip_filename:
                with zip_file.open(item.filename) as file_content:
                    while True:
                        chunk = file_content.read(4096)
                        if not chunk:
                            break
                        hasher.update(chunk)

    return hasher.hexdigest()


def fetch_all_drf_pages(
    start_url: str,
    *,
    timeout: float = DEFAULT_DRF_PAGE_TIMEOUT_SECONDS,
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    """Fetch all results from an unauthenticated DRF-style paginated endpoint.

    Follows the `next` link until exhausted. Page payloads are expected in the
    standard Django REST Framework shape: `{"results": [...], "next": <url>|null}`.

    Args:
        start_url: The first page URL to fetch.
        timeout: Per-request timeout in seconds.
        logger: Optional logger (e.g., a Dagster `context.log`) for per-page info.

    Returns:
        The concatenated `results` list from every page.
    """
    results: list[dict[str, Any]] = []
    url: str | None = start_url
    with httpx.Client(follow_redirects=True, timeout=timeout) as client:
        while url:
            if logger is not None:
                logger.info("Fetching page: %s", url)
            response = client.get(url)
            response.raise_for_status()
            payload = response.json()
            results.extend(payload.get("results", []))
            url = payload.get("next")
    return results

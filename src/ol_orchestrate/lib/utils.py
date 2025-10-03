import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any, Literal

import pygsheets
from dagster import OpExecutionContext
from google.oauth2 import service_account

from ol_orchestrate.resources.secrets.vault import Vault


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
        vault = Vault(vault_addr=vault_address, vault_auth_type="github")
        vault._auth_github()  # noqa: SLF001
    else:
        vault = Vault(
            vault_addr=vault_address, vault_role="dagster-server", aws_auth_mount="aws"
        )
        vault._auth_aws_iam()  # noqa: SLF001

    return vault


def s3_uploads_bucket(
    dagster_env: Literal["dev", "qa", "production"],
) -> dict[str, Any]:
    """
    Return the S3 bucket configuration based on the environment.

    Parameters:
        dagster_env (Literal): Environment name, one of "dev", "qa", or "production".

    Returns:
        dict: A dictionary with the S3 bucket and prefix for the specified environment.
    """
    bucket_map = {
        "dev": {"bucket": "ol-devops-sandbox", "prefix": "pipeline-storage"},
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


def fetch_canvas_course_ids_from_google_sheet(context: OpExecutionContext):
    """
    Fetch all canvas course IDs from a Google Sheet
    """
    sheet_config = context.resources.google_sheet_config

    if sheet_config.service_account_json is None:
        context.log.error("No google service account credentials found in vault")
        return []

    creds_dict = (
        sheet_config.service_account_json
        if isinstance(sheet_config.service_account_json, dict)
        else json.loads(sheet_config.service_account_json)
    )

    creds_dict["private_key"] = creds_dict["private_key"].replace("\\n", "\n")

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = service_account.Credentials.from_service_account_info(
        creds_dict, scopes=scopes
    )

    google_sheet_client = pygsheets.authorize(custom_credentials=credentials)
    spreadsheet = google_sheet_client.open_by_key(sheet_config.sheet_id)

    # Find worksheet by gid
    worksheet = next(
        (
            worksheet
            for worksheet in spreadsheet.worksheets()
            if worksheet.id == sheet_config.worksheet_id
        ),
        None,
    )
    if worksheet is None:
        context.log.error("No worksheet found with gid %s", sheet_config.worksheet_id)
        return []

    # Get all values from the first column and filter to only numeric values
    column_values = worksheet.get_col(1, include_tailing_empty=False)

    return {value.strip() for value in column_values if value.strip().isdigit()}

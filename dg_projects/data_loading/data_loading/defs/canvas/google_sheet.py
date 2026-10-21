"""Read the Canvas course IDs to export from a Google Sheet."""

import json
from typing import Any

import pygsheets
from dagster import ConfigurableResource, OpExecutionContext, ResourceDependency
from google.oauth2 import service_account
from ol_orchestrate.resources.secrets.vault import Vault


class GoogleSheetConfig(ConfigurableResource):
    vault: ResourceDependency[Vault]
    # The service account key is read from Vault when the sheet is read rather
    # than passed in as a field: Dagster shows a resource's field values on the
    # code location's Resources page, private key included.
    mount_point: str = "secret-data"
    config_path: str = "pipelines/google-service-account"
    # Google Sheet ID for canvas course IDs
    sheet_id: str = "13AoothEhEvWs2cJEEfZETm7E6h3-ZY4tD11KX_ARe1A"
    worksheet_id: int = 1472315099  # Worksheet ID (gid) within the Google Sheet

    def service_account_json(self) -> dict[str, Any]:
        """Read the service account key from Vault.

        A failed read raises rather than returning nothing, so the sensor tick
        fails visibly instead of skipping, and never gets as far as diffing an
        unread sheet against the partitions.
        """
        return self.vault.client.secrets.kv.v1.read_secret(
            mount_point=self.mount_point, path=self.config_path
        )["data"]


def fetch_canvas_course_ids_from_google_sheet(
    context: OpExecutionContext,
) -> set[str] | None:
    """Fetch all canvas course IDs from a Google Sheet.

    Returns ``None`` when the sheet could not be read, and a set -- possibly
    empty -- when it could. The caller diffs this against the existing dynamic
    partitions and deletes the difference, so "we could not read the sheet" and
    "the sheet lists no courses" cannot both be ``set()``: the first would delete
    every Canvas partition, taking any in-flight run for one down with it
    (DAGSTER-2B). A failed Vault read raises, which keeps it out of that diff
    the same way and fails the tick where it can be seen.
    """
    sheet_config = context.resources.google_sheet_config

    service_account_json = sheet_config.service_account_json()
    creds_dict = (
        service_account_json
        if isinstance(service_account_json, dict)
        else json.loads(service_account_json)
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
        return None

    # Get all values from the first column and filter to only numeric values
    column_values = worksheet.get_col(1, include_tailing_empty=False)

    return {value.strip() for value in column_values if value.strip().isdigit()}

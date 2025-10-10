import json

import pygsheets
from dagster import OpExecutionContext
from google.oauth2 import service_account


def fetch_canvas_course_ids_from_google_sheet(context: OpExecutionContext):
    """
    Fetch all canvas course IDs from a Google Sheet
    """
    sheet_config = context.resources.google_sheet_config

    if sheet_config.service_account_json is None:
        context.log.error("No google service account credentials found in vault")
        return set()

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
        return set()

    # Get all values from the first column and filter to only numeric values
    column_values = worksheet.get_col(1, include_tailing_empty=False)

    return {value.strip() for value in column_values if value.strip().isdigit()}

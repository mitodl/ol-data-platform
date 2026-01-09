"""Helper functions for Google Sheets data processing."""

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

import ffmpeg
import pygsheets
from dagster import OpExecutionContext
from dateutil import parser  # type: ignore[import-untyped]
from google.oauth2 import service_account

log = logging.getLogger(__name__)


def fetch_video_shorts_from_google_sheet(
    context: OpExecutionContext,
) -> list[dict[str, Any]]:
    """
    Fetch Video Shorts metadata from a Google Sheet.

    Args:
        context: Dagster execution context with video_shorts_sheet_config resource

    Returns:
        List of video metadata dictionaries, sorted by pub_date descending
    """
    sheet_config = context.resources.video_shorts_sheet_config

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

    # Use first worksheet
    worksheet = spreadsheet.sheet1
    rows = worksheet.get_all_values(
        include_tailing_empty=False,
        include_tailing_empty_rows=False,
        # Use UNFORMATTED_VALUE to get raw cell values (e.g., serial numbers for dates)
        value_render=pygsheets.ValueRenderOption.UNFORMATTED_VALUE,
    )

    context.log.info("Fetched %d rows from Google Sheets", len(rows))

    # Debug: Log first few rows to see what we got
    if rows:
        context.log.info(
            "First row (header): %s", rows[0] if len(rows) > 0 else "No header"
        )
        if len(rows) > 1:
            context.log.info("Second row (first data): %s", rows[1])
            context.log.info("Sample of row lengths: %s", [len(r) for r in rows[:5]])
    else:
        context.log.warning("No rows returned from Google Sheets!")

    # Parse rows into video metadata
    return parse_sheet_rows(rows)


def parse_pub_date(date_value: str | float) -> datetime | None:  # noqa: PLR0911
    """
    Parse publication date from Google Sheets.

    Handles multiple formats:
    - String dates: "09/11/25" (MM/DD/YY) or "09/11/2025" (MM/DD/YYYY)
    - Serial numbers: Excel/Google Sheets date serial (days since 1899-12-30)

    Args:
        date_value: Date value from the sheet (string or number)

    Returns:
        datetime object or None if parsing fails
    """
    if date_value is None or date_value == "":
        return None

    # Handle numeric serial dates (Excel/Google Sheets format)
    if isinstance(date_value, (int, float)):
        try:
            # Google Sheets uses same epoch as Excel: December 30, 1899
            # Serial number represents days since epoch
            epoch = datetime(1899, 12, 30)  # noqa: DTZ001
            return epoch + timedelta(days=float(date_value))
        except (ValueError, OverflowError):
            return None

    # Handle string dates
    if isinstance(date_value, str):
        date_str = date_value.strip()
        if not date_str:
            return None

        try:
            # Parse flexibly with dateutil (handles MM/DD/YY, MM/DD/YYYY, etc.)
            return parser.parse(date_str, dayfirst=False)  # US format (month first)
        except (ValueError, parser.ParserError):
            return None

    return None


def extract_filename_from_dropbox_url(dropbox_url: str) -> str:
    """
    Extract the filename from a Dropbox URL.

    Args:
        dropbox_url: Dropbox URL containing the filename in the path

    Returns:
        URL-decoded filename

    Example:
        >>> extract_filename_from_dropbox_url(
        ...     "https://www.dropbox.com/scl/fi/abc123/My%20File.mp4?rlkey=xyz&dl=0"
        ... )
        "My File.mp4"
    """
    parsed_url = urlparse(dropbox_url)
    path = parsed_url.path

    # The filename is the last component of the path
    filename = path.split("/")[-1]

    # URL-decode the filename (handles %20 -> space, etc.)
    return unquote(filename)


def generate_partition_key(dropbox_url: str) -> str:
    """
    Generate a partition key from a Dropbox URL using SHA256 hash.

    Args:
        dropbox_url: Full Dropbox URL

    Returns:
        First 16 characters of SHA256 hash (64-bit hex)
    """
    url_hash = hashlib.sha256(dropbox_url.encode("utf-8")).hexdigest()
    return url_hash[:16]


def convert_dropbox_link_to_direct(dropbox_url: str) -> str:
    """
    Convert a Dropbox share link to a direct download link.

    Changes:
    - www.dropbox.com -> dl.dropboxusercontent.com
    - Removes query parameters except those needed for download
    - Changes dl=0 to dl=1

    Args:
        dropbox_url: Dropbox share URL

    Returns:
        Direct download URL
    """
    # Replace domain for direct download
    direct_url = dropbox_url.replace("www.dropbox.com", "dl.dropboxusercontent.com")

    # Remove /scl/fo/ path component if present
    direct_url = re.sub(r"/scl/fo/[^/]+/", "/", direct_url)

    # Remove preview parameter if present
    direct_url = re.sub(r"[&?]preview=[^&]*", "", direct_url)

    # Remove e parameter if present
    direct_url = re.sub(r"[&?]e=[^&]*", "", direct_url)

    # Ensure dl=1 for direct download
    if "dl=0" in direct_url:
        direct_url = direct_url.replace("dl=0", "dl=1")
    elif "dl=" not in direct_url:
        separator = "&" if "?" in direct_url else "?"
        direct_url = f"{direct_url}{separator}dl=1"
    log.info("Converted Dropbox URL to direct link: %s", direct_url)
    return direct_url


def parse_sheet_rows(
    rows: list[list[str]],
) -> list[dict[str, Any]]:
    """
    Parse Google Sheets rows into structured video metadata.

    Expected columns:
    - Pub date: Publication date (MM/DD/YY format)
    - Video name: Title of the video
    - Dropbox link: URL to video on Dropbox (filename extracted from URL)

    Only videos with pub_date <= today are included (future videos are skipped).

    Args:
        rows: Raw rows from Google Sheets (first row is header)

    Returns:
        List of video metadata dictionaries with pub_date <= today,
        sorted by pub_date descending
    """
    if not rows or len(rows) < 2:  # Need at least header + 1 data row  # noqa: PLR2004
        log.warning("Sheet has fewer than 2 rows (header + data): %d rows", len(rows))
        return []

    # Get today's date (without time) for comparison
    today = datetime.now().date()  # noqa: DTZ005

    # Parse header row
    header = [col.strip().lower() for col in rows[0]]
    log.info("Sheet headers found: %s", header)

    # Find column indices
    try:
        pub_date_idx = header.index("pub date")
        video_name_idx = header.index("video name")
        dropbox_link_idx = header.index("dropbox link")
    except ValueError as e:
        msg = f"Missing required column in sheet header: {e}. Found headers: {header}"
        raise ValueError(msg) from e

    # Parse data rows
    videos = []
    skipped_rows = 0
    for row_idx, row in enumerate(rows[1:], start=2):  # Skip header, start at row 2
        if len(row) <= max(pub_date_idx, video_name_idx, dropbox_link_idx):
            log.debug(
                "Row %d: Skipped (incomplete row, only %d cells)", row_idx, len(row)
            )
            skipped_rows += 1
            continue  # Skip incomplete rows

        # Extract values - handle both strings and other types (numbers, etc.)
        pub_date_value = row[pub_date_idx] if pub_date_idx < len(row) else ""
        video_name_raw = row[video_name_idx] if video_name_idx < len(row) else ""
        dropbox_link_raw = row[dropbox_link_idx] if dropbox_link_idx < len(row) else ""

        # Convert to strings and strip whitespace
        video_name = str(video_name_raw).strip() if video_name_raw else ""
        dropbox_link = str(dropbox_link_raw).strip() if dropbox_link_raw else ""

        # Extract filename from Dropbox URL
        file_name = (
            extract_filename_from_dropbox_url(dropbox_link) if dropbox_link else ""
        )

        # Skip rows without parseable pub_date or dropbox_link
        pub_date = parse_pub_date(pub_date_value)
        if not pub_date or not dropbox_link:
            log.warning(
                "Row %d: Skipped - pub_date_value='%s' (type=%s, parsed=%s), "
                "dropbox_link='%s' (has_link=%s)",
                row_idx,
                pub_date_value,
                type(pub_date_value).__name__,
                "OK" if pub_date else "FAILED",
                dropbox_link[:50] if dropbox_link else "",
                "YES" if dropbox_link else "NO",
            )
            skipped_rows += 1
            continue

        # Skip rows with future publication dates
        if pub_date.date() > today:
            log.info(
                "Row %d: Skipped - pub_date=%s is in the future (today=%s)",
                row_idx,
                pub_date.date(),
                today,
            )
            skipped_rows += 1
            continue

        partition_key = generate_partition_key(dropbox_link)

        # Format date for display (convert datetime back to string)
        pub_date_str = pub_date.isoformat()

        log.info(
            "Row %d: Accepted - video='%s', file='%s', date='%s' (from %s)",
            row_idx,
            video_name[:30] if video_name else "(no name)",
            file_name,
            pub_date_str,
            pub_date_value,
        )

        videos.append(
            {
                "partition_key": partition_key,
                "pub_date": pub_date,
                "pub_date_str": pub_date_str,
                "video_name": video_name,
                "file_name": file_name,
                "dropbox_link": dropbox_link,
            }
        )

    # Sort by pub_date descending (newest first)
    videos.sort(key=lambda v: v["pub_date"], reverse=True)  # type: ignore[arg-type, return-value]

    log.info(
        "Parsed %d videos with pub_date <= %s (%d rows, %d skipped)",
        len(videos),
        today,
        len(rows) - 1,
        skipped_rows,
    )

    return videos


def generate_video_thumbnail(
    video_path: Path,
    thumbnail_path: Path,
    width: int,
    height: int,
) -> None:
    """
    Generate a thumbnail image from a video file using ffmpeg.

    Extracts the first frame and resizes it to the specified dimensions.

    Args:
        video_path: Path to the input video file
        thumbnail_path: Path where thumbnail will be saved
        width: Thumbnail width in pixels
        height: Thumbnail height in pixels

    Raises:
        RuntimeError: If video cannot be read or thumbnail cannot be generated
    """
    try:
        # Extract first frame from video and resize to specified dimensions
        (
            ffmpeg.input(str(video_path), ss=0)
            .filter("scale", width, height)
            .output(
                str(thumbnail_path),
                vframes=1,
                format="image2",
                vcodec="mjpeg",
            )
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
    except ffmpeg.Error as e:
        stderr = e.stderr.decode("utf-8") if e.stderr else "No error details"
        msg = f"Failed to generate thumbnail from {video_path}: {stderr}"
        raise RuntimeError(msg) from e

    # Verify thumbnail was created
    if not thumbnail_path.exists() or thumbnail_path.stat().st_size == 0:
        msg = f"Thumbnail file was not created or is empty: {thumbnail_path}"
        raise RuntimeError(msg)

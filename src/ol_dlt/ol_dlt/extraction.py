"""Reading a course's files through Tika, for the sources that load them whole.

The OCW and Canvas content sources read a course only when it changed, and
send every file of it to the one shared Tika. They tell a file Tika cannot read
from a Tika that is down, and a course's state records a failure that may pass
so the course is read again on a later day.
"""

import logging
from collections.abc import Iterable
from datetime import UTC, datetime, timedelta

import requests

from ol_dlt import tika

logger = logging.getLogger(__name__)

STATUS_EXTRACTED = "extracted"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"
STATUS_MISSING = "missing"
# A file that never reached Tika because it cannot be read at all.
STATUS_UNREADABLE = "unreadable"

# This many files of a course failing in a row, or all of them, may mean Tika
# is down. A probe document settles it.
FAILURES_BEFORE_PROBE = 8
PROBE_DOCUMENT = b"OCW content extraction probe."
# Two tries, so a Tika busy with this course's own files gets a second chance
# to answer before the load is failed.
PROBE_ATTEMPTS = 2
PROBE_TIMEOUT_SECONDS = 60
AUTH_FAILURES = frozenset({401, 403})

# The least time between two reads of a course that is waiting on something:
# a retry of a failed file, or a second look at an empty prefix. Under a day,
# so the daily run counts, and long enough that one run crossing midnight
# does not.
WAIT_BETWEEN_READS = timedelta(hours=20)

# How many daily reads a course gets while Tika keeps failing on one of its
# files for a reason that may pass (a timeout, a 5xx). After that the failed
# files stay failed until the course changes.
MAX_ATTEMPTS = 3

# These say nothing about Tika, so a run of them is no reason to probe it.
NOT_SENT_TO_TIKA = frozenset({STATUS_MISSING, STATUS_UNREADABLE})

# A file's text, how the extraction went, and whether another read may do better.
type Extraction = tuple[str | None, str, bool]


class TikaUnavailableError(RuntimeError):
    """Tika cannot be used, so no other file or course would get past it."""


def may_pass(error: requests.RequestException) -> bool:
    """Tell a failure worth another read from Tika refusing the document."""
    response = error.response
    if response is None:
        return True
    return response.status_code >= 500 or response.status_code == 429  # noqa: PLR2004


def raise_if_token_rejected(error: requests.RequestException, reading: str) -> None:
    """Fail the load on a rejected token, which no file or probe can get past."""
    status = error.response.status_code if error.response is not None else None
    if status in AUTH_FAILURES:
        msg = f"Tika rejected the access token ({status}) reading {reading}."
        raise TikaUnavailableError(msg) from error


def extract(
    client: tika.TikaClient,
    body: bytes,
    reading: str,
    content_type: str | None = None,
) -> Extraction:
    """Send a file to Tika and report how it went.

    :param client: The Tika client.
    :param body: The file.
    :param reading: What to call the file in logs and errors.
    :param content_type: The file's MIME type; Tika detects it when None.
    :returns: The text, its status, and whether to read the file again.
    :raises TikaUnavailableError: Tika rejected the access token.
    """
    if not body:
        return None, STATUS_EMPTY, False
    try:
        text = client.extract_text(body, content_type=content_type)
    except requests.RequestException as error:
        # Not about this file: every other one would fail the same way and be
        # recorded as unreadable.
        raise_if_token_rejected(error, reading)
        logger.exception("Tika could not read %s", reading)
        return None, STATUS_FAILED, may_pass(error)
    return (text, STATUS_EXTRACTED, False) if text else (None, STATUS_EMPTY, False)


def raise_if_tika_is_down(client: tika.TikaClient, course: str, failed: int) -> None:
    """Fail the load when Tika cannot read a document known to be readable.

    Without the probe, a course whose files Tika really cannot read would fail
    every load at the same place, and no course sorted after it would load.
    """
    for attempt in range(1, PROBE_ATTEMPTS + 1):
        try:
            client.extract_text(PROBE_DOCUMENT, timeout=PROBE_TIMEOUT_SECONDS)
        except requests.RequestException as error:
            raise_if_token_rejected(error, "the probe document")
            if attempt < PROBE_ATTEMPTS:
                continue
            msg = (
                f"Tika failed {failed} files of {course} and then a probe "
                "document. Treating it as a Tika outage, so the course is not "
                "recorded as read."
            )
            raise TikaUnavailableError(msg) from error
        return


def collect(
    client: tika.TikaClient, course: str, results: Iterable[Extraction]
) -> list[Extraction]:
    """Gather a course's extractions, failing the load if Tika is down.

    :param client: The Tika client.
    :param course: What to call the course in errors.
    :param results: The extractions, in the order they finish being read.
    :returns: The extractions.
    :raises TikaUnavailableError: Tika is down.
    """
    collected = []
    failed_in_a_row = 0
    probed = False
    for result in results:
        collected.append(result)
        # A file that was never sent to Tika neither counts as a failure nor
        # breaks a run of them.
        if result[1] in NOT_SENT_TO_TIKA:
            continue
        failed_in_a_row = failed_in_a_row + 1 if result[1] == STATUS_FAILED else 0
        if failed_in_a_row >= FAILURES_BEFORE_PROBE and not probed:
            raise_if_tika_is_down(client, course, failed_in_a_row)
            probed = True
    read = [
        status for _text, status, _retry in collected if status not in NOT_SENT_TO_TIKA
    ]
    if read and not probed and all(status == STATUS_FAILED for status in read):
        raise_if_tika_is_down(client, course, len(read))
    return collected


def as_utc(value: str) -> datetime:
    """Parse a recorded time, taking one with no time zone as UTC."""
    then = datetime.fromisoformat(value)
    return then.replace(tzinfo=UTC) if then.tzinfo is None else then


def waited(since: str, now: datetime) -> bool:
    """Tell whether ``WAIT_BETWEEN_READS`` has passed since a recorded time."""
    return now - as_utc(since) >= WAIT_BETWEEN_READS


def needs_read(recorded: str | None, version: str, now: datetime) -> bool:
    """Tell whether a course's recorded state calls for reading it again.

    The state is the version, or ``version|attempts|time`` while a file of
    that version is failing in a way that may pass.
    """
    if recorded is None:
        return True
    recorded_version, _, retry = recorded.partition("|")
    if recorded_version != version:
        return True
    if not retry:
        return False
    attempts, _, last_read = retry.partition("|")
    return int(attempts) < MAX_ATTEMPTS and waited(last_read, now)


def record_read(
    recorded: str | None, version: str, now: datetime, *, retry: bool
) -> str:
    """Return the state to hold for a course that was just read."""
    if not retry:
        return version
    attempts = 0
    if recorded is not None and recorded.startswith(f"{version}|"):
        attempts = int(recorded.split("|")[1])
    return f"{version}|{attempts + 1}|{now.isoformat(timespec='seconds')}"

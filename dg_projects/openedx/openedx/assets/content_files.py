"""Text extraction from Open edX course content files.

Cohort 4 of the MIT Learn ETL migration moves ContentFile text extraction onto
the data platform. MIT Learn previously downloaded each course archive and ran
its own extraction; here the platform extracts once, publishes the text, and
MIT Learn consumes it.

Data flow:
    openedx/processed_data/course_static_assets   (tar.gz, per course run)
        -> extract_course_document_text           (this asset, Tika)
            -> openedx/processed_data/course_document_text  (JSONL in S3)
                -> integrations__learn__content_files       (dbt)

Only documents Tika can parse are handled here. Video transcripts are a
separate asset: they need no external service, and pairing them with Tika would
mean a Tika outage also stopped transcript extraction.
"""

import json
import logging
import mimetypes
import tarfile
from collections.abc import Callable, Iterable, Iterator
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import httpx2 as httpx
import jsonlines
from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    DataVersion,
    Output,
    asset,
)
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from upath import UPath

log = logging.getLogger(__name__)

# A course whose documents ALL fail extraction is indistinguishable, downstream,
# from a course with no documents -- both publish zero rows. That is the failure
# mode that looks like success, so it is made loud here instead. Below this
# success rate the asset raises rather than publishing a silently empty set.
#
# Individual documents legitimately fail (corrupt PDFs, password-protected
# files), so this is deliberately not 100%.
MIN_EXTRACTION_SUCCESS_RATE = 0.5

# Below this many candidate documents the rate is not meaningful -- one failure
# out of one is 0% and would trip the guard on a course with a single bad PDF.
MIN_DOCUMENTS_FOR_RATE_CHECK = 5

# Subtitle suffixes. These are NOT documents, even though a real archive makes
# them look like one: `mimetypes` maps `.srt` to `text/plain`, which Tika
# happily accepts and returns verbatim -- timestamps, sequence numbers and all.
# A 14.310x export carries 302 `.srt` and 720 `.srt.sjson` files, so without
# this exclusion every one of the `.srt` would be extracted twice: once as
# unusable timestamp-laden "text" here, and once properly by the transcript
# asset. The transcript parser owns them.
SRT_SUFFIX = ".srt"
SJSON_SUFFIX = ".sjson"
TRANSCRIPT_SUFFIXES = frozenset({SRT_SUFFIX, SJSON_SUFFIX})


def is_transcript(relative_path: str) -> bool:
    """Report whether a bundle member is a subtitle file rather than a document.

    Dispatch is on the FINAL suffix: Open edX writes subtitles as
    `subs_<id>.srt.sjson`, which is SJSON despite what the middle of the name
    suggests.
    """
    return Path(relative_path).suffix.lower() in TRANSCRIPT_SUFFIXES


def _content_type(relative_path: str) -> str:
    """Guess a document's MIME type from its path.

    The bundle manifest carries the same value, but it is a sibling S3 object;
    deriving it here keeps this asset dependent on the archive alone.
    """
    mime_type, _ = mimetypes.guess_type(relative_path)
    return mime_type or "application/octet-stream"


# Statuses that mean the caller is not allowed to use Tika at all, rather than
# that this particular document is unparseable. An empty access token -- what a
# Vault read failure leaves behind -- produces exactly this.
_TIKA_AUTH_STATUSES = frozenset({401, 403})
_SERVER_ERROR_FLOOR = 500


class TikaUnavailableError(RuntimeError):
    """Tika is unusable, so a per-document failure count would be misleading."""


def raise_if_service_failure(error: Exception, relative_path: str) -> None:
    """Re-raise as a service failure when the error is not about this document.

    Counting a dead or misconfigured Tika as N document-level failures reads
    downstream as "this course has unparseable documents" when it actually
    means "no document in any course could have been extracted". Worse, a
    course with fewer candidates than MIN_DOCUMENTS_FOR_RATE_CHECK skips the
    health guard entirely and publishes an empty JSONL as a success.

    Only unambiguous service-level signals are promoted: an auth rejection, a
    5xx, or a transport error that means the request never got an answer. A
    timeout stays a document-level failure because one oversized PDF times out
    on a perfectly healthy service; a genuinely dead service produces enough of
    them to trip the guard, which now also fires when every candidate failed.
    """
    status: int | None = None
    if isinstance(error, httpx.HTTPStatusError):
        status = error.response.status_code
    elif not isinstance(error, httpx.TransportError):
        return
    if isinstance(error, httpx.TimeoutException):
        return

    if status is None or status in _TIKA_AUTH_STATUSES or status >= _SERVER_ERROR_FLOOR:
        msg = (
            f"Tika is unavailable (failed on {relative_path}): {error}. "
            "Refusing to record this as a document-level extraction failure, "
            "which would publish as 'this course has no readable documents'."
        )
        raise TikaUnavailableError(msg) from error


def build_document_rows(
    members: Iterable[tuple[str, Callable[[], bytes]]],
    *,
    course_id: str,
    source_system: str,
    extract: Any,
    is_supported: Any,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Extract text from each supported document, returning rows and counters.

    Split out from the asset body so the dispatch and failure accounting can be
    tested without a Dagster context or a live Tika service.

    ``members`` yields ``(relative_path, read_bytes)``. The bytes are behind a
    callable so a member that fails the path-based filters below never gets
    read at all -- in a real export the skipped images and archives are the
    bulk of the archive.

    A file Tika cannot parse is skipped silently -- images and archives are
    normal course content, not errors. A file Tika *should* parse but does not
    is counted as a failure and feeds the success-rate guard.

    Subtitles are skipped even though Tika would accept them, because the
    transcript asset owns those and Tika's output for one is unusable.
    """
    rows: list[dict[str, Any]] = []
    counters = {
        "candidates": 0,
        "extracted": 0,
        "empty": 0,
        "failed": 0,
        "skipped": 0,
        "transcripts": 0,
    }

    for relative_path, read_bytes in members:
        if is_transcript(relative_path):
            counters["transcripts"] += 1
            continue

        content_type = _content_type(relative_path)

        if not is_supported(content_type):
            counters["skipped"] += 1
            continue

        file_bytes = read_bytes()

        # A zero-byte file has no text, which is a fact rather than a failure.
        # Tika answers 422 for an empty body, and counting that as a failed
        # extraction would both waste a round trip and pollute the denominator
        # the health guard divides by -- an Open edX export ships a stack of
        # empty about/ placeholders, so this is common enough to matter.
        if not file_bytes:
            counters["candidates"] += 1
            counters["empty"] += 1
            rows.append(
                {
                    "course_id": course_id,
                    "source_system": source_system,
                    "file_path": relative_path,
                    "content_type": content_type,
                    "size_bytes": 0,
                    "content": None,
                    "extraction_status": "empty",
                }
            )
            continue

        counters["candidates"] += 1
        try:
            text = extract(file_bytes, content_type)
        except Exception as error:
            raise_if_service_failure(error, relative_path)
            counters["failed"] += 1
            log.exception("Tika extraction failed for %s", relative_path)
            continue

        if text:
            counters["extracted"] += 1
        else:
            # Tika answered but produced nothing. Common for scanned PDFs with
            # no OCR; recorded as a row so the absence is visible downstream
            # rather than looking like the file was never seen.
            counters["empty"] += 1

        rows.append(
            {
                "course_id": course_id,
                "source_system": source_system,
                "file_path": relative_path,
                "content_type": content_type,
                "size_bytes": len(file_bytes),
                "content": text or None,
                "extraction_status": "extracted" if text else "empty",
            }
        )

    return rows, counters


def assert_extraction_healthy(counters: dict[str, int], course_id: str) -> None:
    """Refuse to publish a document set that mostly failed to extract.

    Zero candidates is fine -- plenty of courses carry no documents. A course
    with documents that overwhelmingly fail is not fine, because the published
    result is indistinguishable from "this course has no documents".
    """
    candidates = counters["candidates"]
    succeeded = counters["extracted"] + counters["empty"]

    # Total failure is loud at any size. The rate floor below exists so one bad
    # PDF in a two-document course does not trip the guard, but that same floor
    # let a course whose every document failed publish an empty JSONL as a
    # success -- which is precisely the case worth refusing.
    if candidates and not succeeded:
        msg = (
            f"Refusing to publish document text for {course_id}: all "
            f"{candidates} candidate documents failed extraction. Publishing "
            "would be indistinguishable from 'this course has no documents'."
        )
        raise RuntimeError(msg)

    if candidates < MIN_DOCUMENTS_FOR_RATE_CHECK:
        return

    success_rate = succeeded / candidates
    if success_rate < MIN_EXTRACTION_SUCCESS_RATE:
        msg = (
            f"Refusing to publish document text for {course_id}: only "
            f"{succeeded}/{candidates} documents extracted "
            f"({success_rate:.0%}, floor is {MIN_EXTRACTION_SUCCESS_RATE:.0%}). "
            "A near-total extraction failure publishes as 'no documents', so it "
            "is raised rather than written."
        )
        raise RuntimeError(msg)


@asset(
    key=AssetKey(("openedx", "processed_data", "course_document_text")),
    group_name="openedx",
    ins={
        "course_static_assets": AssetIn(
            key=AssetKey(("openedx", "processed_data", "course_static_assets"))
        )
    },
    io_manager_key="s3file_io_manager",
    automation_condition=upstream_or_code_changes(),
    required_resource_keys={"openedx", "tika"},
    # Every changed course in every deployment can trigger a run here, and each
    # run makes hundreds of long-running requests to a single shared Tika. A
    # backfill or a bulk republication therefore points the whole fan-out at one
    # service at once.
    #
    # As with `openedx_course_export`, naming the pool only makes the limit
    # *settable*: until a slot limit is configured for
    # `openedx_document_extraction` on the instance (Deployment -> Concurrency),
    # these runs are still unbounded.
    pool="openedx_document_extraction",
    description=(
        "Plain text extracted via Apache Tika from the documents in a course "
        "archive (PDFs, HTML, Office files). One JSONL row per document."
    ),
)
def extract_course_document_text(
    context: AssetExecutionContext, course_static_assets: UPath
):
    """Extract text from every Tika-parseable document in the course bundle."""
    tika = context.resources.tika
    source_system = context.resources.openedx.deployment
    course_id = context.partition_key

    temp_files: list[Path] = []
    try:
        bundle_path = Path(
            NamedTemporaryFile(delete=False, suffix="_static_assets.tar.gz").name
        )
        temp_files.append(bundle_path)
        context.log.info(
            "Downloading static asset bundle from %s", course_static_assets
        )
        course_static_assets.fs.get_file(str(course_static_assets), str(bundle_path))

        # Members are walked lazily and their bytes pulled only for the ones
        # that survive filtering. A real 14.310x export is 152 MiB and is mostly
        # images and archives that get skipped; reading every member up front
        # held all of that resident alongside the tarball and the extracted
        # text, which is what could exhaust a worker on a media-heavy course.
        with tarfile.open(bundle_path, "r:gz") as bundle:

            def _members(
                bundle: tarfile.TarFile = bundle,
            ) -> Iterator[tuple[str, Callable[[], bytes]]]:
                for member in bundle:
                    if not member.isfile():
                        continue

                    def _read(member: tarfile.TarInfo = member) -> bytes:
                        extracted = bundle.extractfile(member)
                        return extracted.read() if extracted else b""

                    yield member.name, _read

            rows, counters = build_document_rows(
                _members(),
                course_id=course_id,
                source_system=source_system,
                extract=tika.extract_text,
                is_supported=tika.is_supported,
            )
        assert_extraction_healthy(counters, course_id)

        output_file = Path(
            NamedTemporaryFile(delete=False, suffix="_document_text.jsonl").name
        )
        temp_files.append(output_file)
        with jsonlines.open(output_file, "w") as writer:
            writer.write_all(rows)

        # Version on the bundle this was extracted from: the text is a pure
        # function of the archive plus the extractor, so re-extracting an
        # unchanged bundle would produce an identical result.
        data_version = str(course_static_assets).rsplit("/", 1)[-1].split(".")[0]
        object_key = (
            f"{'/'.join(context.asset_key.path)}/{source_system}/"
            f"{course_id}/{data_version}.jsonl"
        )

        context.log.info(
            "Extracted %d/%d documents for %s (%d empty, %d failed, %d skipped)",
            counters["extracted"],
            counters["candidates"],
            course_id,
            counters["empty"],
            counters["failed"],
            counters["skipped"],
        )

        yield Output(
            (output_file, object_key),
            data_version=DataVersion(data_version),
            metadata={
                "course_id": course_id,
                "object_key": object_key,
                "document_count": len(rows),
                "extracted": counters["extracted"],
                "empty": counters["empty"],
                "failed": counters["failed"],
                "skipped_unsupported": counters["skipped"],
                "skipped_transcripts": counters["transcripts"],
                "counters": json.dumps(counters),
            },
        )
    finally:
        for temp_file in temp_files:
            temp_file.unlink(missing_ok=True)

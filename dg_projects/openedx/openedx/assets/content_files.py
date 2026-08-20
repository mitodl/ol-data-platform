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
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

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


def build_document_rows(
    members: list[tuple[str, bytes]],
    *,
    course_id: str,
    source_system: str,
    extract: Any,
    is_supported: Any,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Extract text from each supported document, returning rows and counters.

    Split out from the asset body so the dispatch and failure accounting can be
    tested without a Dagster context or a live Tika service.

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

    for relative_path, file_bytes in members:
        if is_transcript(relative_path):
            counters["transcripts"] += 1
            continue

        content_type = _content_type(relative_path)

        if not is_supported(content_type):
            counters["skipped"] += 1
            continue

        counters["candidates"] += 1
        try:
            text = extract(file_bytes, content_type)
        except Exception:
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
    if candidates < MIN_DOCUMENTS_FOR_RATE_CHECK:
        return

    succeeded = counters["extracted"] + counters["empty"]
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

        with tarfile.open(bundle_path, "r:gz") as bundle:
            members = [
                (member.name, extracted.read())
                for member in bundle.getmembers()
                if member.isfile() and (extracted := bundle.extractfile(member))
            ]

        context.log.info("Bundle for %s holds %d files", course_id, len(members))

        rows, counters = build_document_rows(
            members,
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

"""OCW course content ingestion via dlt.

OCW publishes every course as a static site under ``courses/<slug>/`` in the
public ``ocw-content-live-production`` bucket. MIT Learn builds its OCW
ContentFiles from that bucket: one per ``data.json`` under ``pages/`` and
``resources/``, with the text of a resource's file extracted by Tika
(``learning_resources/etl/ocw.py`` in mit-learn). This source reads the same
objects, so the warehouse holds what Learn would have extracted:

    courses/<slug>/data.json                -> content_kind = course
    courses/<slug>/pages/**/data.json       -> content_kind = page
    courses/<slug>/resources/**/data.json   -> content_kind = resource
        + the text of the file the resource points at
    -> raw__ocw__s3__course_content

A course is read whole, and only when it changed. Its version is a digest of
the ETags of the objects above and of the files under its own prefix, held in
dlt state with the ETags of any files it read from another course's prefix. A
publish that rewrites identical bytes keeps every ETag, so it changes nothing
here. A changed course appends a full new set of rows stamped with one
``course_retrieved_at``, and staging keeps each course's newest set. A course
that leaves the bucket appends one ``unpublished`` row, which becomes its
newest set and so empties it downstream. So does a course whose prefix has no
``data.json`` on two reads ``WAIT_BETWEEN_READS`` apart.

A file Tika fails on is loaded with ``extraction_status`` ``failed``. When the
failure may pass (a timeout, a 5xx), the course is read again on up to
``MAX_ATTEMPTS`` days, and staging keeps the text of an earlier read of the
same file. When ``FAILURES_BEFORE_PROBE`` files of a course fail in a row, or
all of them do, a probe document tells a Tika outage, which fails the load,
from a course Tika cannot read, which is recorded so the courses after it
still load. Tika rejecting the access token always fails the load.

One load covers at most ``budget_bytes`` of source files. The caller re-runs
the source until a load reads nothing (``build_batched_assets`` in the
data_loading code location), and the sweep resumes after the last course a
load finished rather than listing the bucket from the start again.

Tika credentials: see ``ol_dlt.tika``.

Run standalone:
    DLT_PROFILE=dev TIKA_ACCESS_TOKEN=... python -m ol_dlt.sources.ocw_content
"""

import hashlib
import json
import logging
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote

import dlt
import pyarrow as pa
import requests
import s3fs

from ol_dlt import config, tika

logger = logging.getLogger(__name__)

BUCKET = "ocw-content-live-production"
RAW_TABLE = "raw__ocw__s3__course_content"

# The extensions MIT Learn extracts text for: VALID_FILE_TYPES in mit-learn's
# learning_resources/constants.py. A resource whose file has any other
# extension still gets a row, with no text.
VALID_FILE_TYPES = frozenset(
    {
        ".csv",
        ".doc",
        ".docx",
        ".htm",
        ".html",
        ".json",
        ".m",
        ".mat",
        ".md",
        ".pdf",
        ".ppt",
        ".pptx",
        ".ps",
        ".py",
        ".r",
        ".rtf",
        ".sjson",
        ".srt",
        ".txt",
        ".vtt",
        ".xls",
        ".xlsx",
        ".xml",
    }
)
# Learn drops these resources, so their text is never read.
SKIPPED_TITLES = frozenset({"3play caption file", "3play pdf file"})

# Site files Hugo generates beside a course's uploads. No resource points at
# them, and a theme release can rewrite them without the course changing, so
# they stay out of the course version.
SITE_FILES = frozenset(
    {"content_map.json", "index.html", "index.xml", "robots.txt", "sitemap.xml"}
)

# Source file bytes one load may read. Text is a few percent of a PDF's size,
# so a load's rows stay far below the ~1.7 GB dlt's Iceberg writer peaked at
# for a 512 MiB batch of text (see ol_dlt.sources.course_xml_blocks).
BUDGET_BYTES = 2 * 1024**3

# S3 reads and Tika calls in flight at once.
WORKERS = 8
# Courses listed ahead of the one being read.
LISTING_WINDOW = 32

# A run that would unpublish more of the known courses than this fails
# instead: an unpublished course loses its ContentFiles in MIT Learn.
MAX_UNPUBLISH_FRACTION = 0.1
MIN_COURSES_FOR_UNPUBLISH_GUARD = 20

# This many files of a course failing in a row, or all of them, may mean Tika
# is down. A probe document settles it.
FAILURES_BEFORE_PROBE = 8
PROBE_DOCUMENT = b"OCW content extraction probe."
# Two tries, so a Tika busy with this course's own files gets a second chance
# to answer before the load is failed.
PROBE_ATTEMPTS = 2
PROBE_TIMEOUT_SECONDS = 60
AUTH_FAILURES = frozenset({401, 403})

# A larger file is recorded as failed without being read: eight are held in
# memory at once. The largest of 800 MB sampled across 60 courses was under
# 100 MB.
MAX_FILE_BYTES = 256 * 1024**2

# The least time between two reads of a course that is waiting on something:
# a retry of a failed file, or a second look at an empty prefix. Under a day,
# so the daily run counts, and long enough that one run crossing midnight
# does not.
WAIT_BETWEEN_READS = timedelta(hours=20)

# How many daily reads a course gets while Tika keeps failing on one of its
# files for a reason that may pass (a timeout, a 5xx). After that the failed
# files stay failed until the course changes.
MAX_ATTEMPTS = 3

KIND_COURSE = "course"
KIND_PAGE = "page"
KIND_RESOURCE = "resource"
KIND_UNPUBLISHED = "unpublished"

STATUS_EXTRACTED = "extracted"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"
STATUS_MISSING = "missing"

# State of a known course whose prefix was last seen with no data.json, with
# the day it was seen.
EMPTY_MARK = "!empty"

SCHEMA = pa.schema(
    [
        pa.field("course_slug", pa.string()),
        pa.field("content_kind", pa.string()),
        pa.field("s3_key", pa.string()),
        pa.field("s3_etag", pa.string()),
        pa.field("data_json", pa.string()),
        pa.field("file_key", pa.string()),
        pa.field("file_etag", pa.string()),
        pa.field("file_size_bytes", pa.int64()),
        pa.field("content", pa.string()),
        pa.field("extraction_status", pa.string()),
        pa.field("course_version", pa.string()),
        pa.field("course_retrieved_at", pa.timestamp("us", tz="UTC")),
    ]
)

type CourseObjects = dict[str, dict[str, Any]]


def text_file_key(resource: dict[str, Any]) -> str | None:
    """Return the S3 key of the file Learn extracts text from for a resource.

    Follows ``transform_contentfile`` and ``transform_contentfile_legacy`` in
    mit-learn: a video's text is its transcript, anything else's is its file.

    :param resource: A parsed ``resources/**/data.json``.
    :returns: The key, or None when Learn would read no text.
    """
    if resource.get("resourcetype"):
        if resource["resourcetype"] == "Video":
            path = (resource.get("video_files") or {}).get("video_transcript_file")
        else:
            path = resource.get("file")
    elif resource.get("resource_type") == "Video":
        path = resource.get("transcript_file")
    else:
        path = resource.get("file")

    if not isinstance(path, str) or resource.get("title") in SKIPPED_TITLES:
        return None
    if "courses" not in path:
        return None
    if PurePosixPath(path).suffix.lower() not in VALID_FILE_TYPES:
        return None
    # Learn's own split: a path naming "courses" twice yields a key that is not
    # in the bucket, and the resource is dropped there as it is here.
    return unquote("courses" + path.split("courses")[1])


def content_kind(slug: str, key: str) -> str | None:
    """Classify a key as one of the ``data.json`` files Learn reads, or None."""
    prefix = f"courses/{slug}/"
    if key == f"{prefix}data.json":
        return KIND_COURSE
    if not key.endswith("data.json"):
        return None
    if key.startswith(f"{prefix}pages/"):
        return KIND_PAGE
    if key.startswith(f"{prefix}resources/"):
        return KIND_RESOURCE
    return None


def course_version(slug: str, objects: CourseObjects) -> str:
    """Digest the ETags of everything a course's rows are built from."""
    prefix = f"courses/{slug}/"
    digest = hashlib.md5(usedforsecurity=False)
    for key in sorted(objects):
        path = PurePosixPath(key)
        is_file = (
            not key.startswith((f"{prefix}pages/", f"{prefix}resources/"))
            and path.name not in SITE_FILES
            and path.suffix.lower() in VALID_FILE_TYPES
        )
        if content_kind(slug, key) or is_file:
            digest.update(f"{key}\t{objects[key]['ETag']}\n".encode())
    return digest.hexdigest()


def list_courses(fs: s3fs.S3FileSystem, bucket: str) -> list[str]:
    """Return every published course slug, sorted."""
    return sorted(
        PurePosixPath(path).name for path in fs.ls(f"{bucket}/courses/", detail=False)
    )


def list_course(fs: s3fs.S3FileSystem, bucket: str, slug: str) -> CourseObjects:
    """Return every object of one course, keyed by S3 key."""
    listing = fs.find(f"{bucket}/courses/{slug}/", detail=True)
    return {name.removeprefix(f"{bucket}/"): info for name, info in listing.items()}


def _etag(info: dict[str, Any]) -> str:
    return info["ETag"].strip('"')


def _may_pass(error: requests.RequestException) -> bool:
    """Tell a failure worth another read from Tika refusing the document."""
    response = error.response
    if response is None:
        return True
    return response.status_code >= 500 or response.status_code == 429  # noqa: PLR2004


def _extract_text(
    fs: s3fs.S3FileSystem,
    client: tika.TikaClient,
    bucket: str,
    key: str,
    size: int,
) -> tuple[str | None, str, bool]:
    """Return a file's text, how the extraction went, and whether to retry it."""
    if size > MAX_FILE_BYTES:
        logger.warning("%s is %s bytes, over the limit; not extracted", key, size)
        return None, STATUS_FAILED, False
    try:
        body = fs.cat_file(f"{bucket}/{key}")
    except FileNotFoundError:
        # Listed a moment ago and gone now, e.g. a publish in progress.
        return None, STATUS_MISSING, False
    if not body:
        return None, STATUS_EMPTY, False
    try:
        text = client.extract_text(body)
    except requests.RequestException as error:
        status = error.response.status_code if error.response is not None else None
        if status in AUTH_FAILURES:
            # Not about this file: every other one would fail the same way
            # and be recorded as unreadable.
            msg = f"Tika rejected the access token ({status}) reading {key}."
            raise RuntimeError(msg) from error
        logger.exception("Tika could not read %s", key)
        return None, STATUS_FAILED, _may_pass(error)
    return (text, STATUS_EXTRACTED, False) if text else (None, STATUS_EMPTY, False)


def _raise_if_tika_is_down(client: tika.TikaClient, slug: str, failed: int) -> None:
    """Fail the load when Tika cannot read a document known to be readable.

    Without the probe, a course whose files Tika really cannot read would fail
    every load at the same place, and no course sorted after it would load.
    """
    for attempt in range(1, PROBE_ATTEMPTS + 1):
        try:
            client.extract_text(PROBE_DOCUMENT, timeout=PROBE_TIMEOUT_SECONDS)
        except requests.RequestException as error:
            if attempt < PROBE_ATTEMPTS:
                continue
            msg = (
                f"Tika failed {failed} files of {slug} and then a probe "
                "document. Treating it as a Tika outage, so the course is not "
                "recorded as read."
            )
            raise RuntimeError(msg) from error
        return


def _object_info(fs: s3fs.S3FileSystem, bucket: str, key: str) -> dict[str, Any] | None:
    """Return an object's listing entry, or None when it is not in the bucket."""
    try:
        info = fs.info(f"{bucket}/{key}")
    except FileNotFoundError:
        return None
    # A key that is only a prefix comes back as a directory, with nothing to
    # read.
    return info if info["type"] == "file" else None


def read_course(  # noqa: PLR0913
    *,
    fs: s3fs.S3FileSystem,
    pool: ThreadPoolExecutor,
    client: tika.TikaClient,
    bucket: str,
    slug: str,
    objects: CourseObjects,
    version: str,
) -> tuple[list[dict[str, Any]], int, bool, dict[str, str]]:
    """Read one course's ``data.json`` files and the text of its resource files.

    :returns: The course's rows, how many source bytes they were read from,
        whether a file failed in a way another read might not, and the ETag of
        each file read from outside the course's prefix.
    :raises RuntimeError: Tika is down.
    """
    retrieved_at = datetime.now(tz=UTC)
    json_keys = [key for key in sorted(objects) if content_kind(slug, key)]
    bodies = pool.map(lambda key: fs.cat_file(f"{bucket}/{key}"), json_keys)

    rows: list[dict[str, Any]] = []
    for key, body in zip(json_keys, bodies, strict=True):
        data_json = body.decode("utf-8", errors="replace")
        kind = content_kind(slug, key)
        file_key = None
        if kind == KIND_RESOURCE:
            try:
                file_key = text_file_key(json.loads(data_json))
            except json.JSONDecodeError:
                logger.warning("%s is not valid JSON", key)
        rows.append(
            {
                **dict.fromkeys(SCHEMA.names),
                "course_slug": slug,
                "content_kind": kind,
                "s3_key": key,
                "s3_etag": _etag(objects[key]),
                "data_json": data_json,
                "file_key": file_key,
                "course_version": version,
                "course_retrieved_at": retrieved_at,
            }
        )
    # A resource can point at a file under another course, which this course's
    # listing does not cover. Learn reads any key in the bucket.
    files: CourseObjects = {}
    for key in sorted({row["file_key"] for row in rows if row["file_key"]}):
        info = objects.get(key) or _object_info(fs, bucket, key)
        if info is not None:
            files[key] = info

    texts: dict[str, tuple[str | None, str, bool]] = {}
    failed_in_a_row = 0
    probed = False
    results = pool.map(
        lambda key: _extract_text(fs, client, bucket, key, files[key]["size"]),
        files,
    )
    for key, result in zip(files, results, strict=True):
        texts[key] = result
        failed_in_a_row = failed_in_a_row + 1 if result[1] == STATUS_FAILED else 0
        if failed_in_a_row >= FAILURES_BEFORE_PROBE and not probed:
            _raise_if_tika_is_down(client, slug, failed_in_a_row)
            probed = True
    if (
        texts
        and not probed
        and all(status == STATUS_FAILED for _text, status, _retry in texts.values())
    ):
        _raise_if_tika_is_down(client, slug, len(texts))

    for row in rows:
        file_key = row["file_key"]
        if file_key is None:
            continue
        if file_key not in files:
            row["extraction_status"] = STATUS_MISSING
            continue
        row["content"], row["extraction_status"], _retry = texts[file_key]
        row["file_etag"] = _etag(files[file_key])
        row["file_size_bytes"] = files[file_key]["size"]

    retry = any(may_pass for _text, _status, may_pass in texts.values())
    source_bytes = sum(objects[key]["size"] for key in json_keys) + sum(
        info["size"] for info in files.values()
    )
    external = {key: _etag(info) for key, info in files.items() if key not in objects}
    return rows, source_bytes, retry, external


def external_files_changed(
    fs: s3fs.S3FileSystem, bucket: str, recorded: dict[str, str] | None
) -> bool:
    """Tell whether a file a course read from another prefix has changed.

    Those files are not in the course's own listing, so not in its version.
    """
    for key, etag in (recorded or {}).items():
        info = _object_info(fs, bucket, key)
        if info is None or _etag(info) != etag:
            return True
    return False


def _waited(since: str, now: datetime) -> bool:
    return now - datetime.fromisoformat(since) >= WAIT_BETWEEN_READS


def _unpublished_row(slug: str, retrieved_at: datetime) -> dict[str, Any]:
    return {
        **dict.fromkeys(SCHEMA.names),
        "course_slug": slug,
        "content_kind": KIND_UNPUBLISHED,
        "course_retrieved_at": retrieved_at,
    }


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
    return int(attempts) < MAX_ATTEMPTS and _waited(last_read, now)


def settle_empty_course(
    recorded: str | None, version: str, now: datetime
) -> tuple[bool, str]:
    """Decide what a prefix with no ``data.json`` means for its course.

    A course that had rows is unpublished only once its prefix has been seen
    empty twice, ``WAIT_BETWEEN_READS`` apart: a publish in progress can leave
    it empty for a moment, and an unpublished row empties the course
    downstream.

    :returns: Whether to load the unpublished row, and the state to hold.
    """
    if recorded is None:
        return False, version
    recorded_version, _, first_seen = recorded.partition("|")
    if recorded_version == EMPTY_MARK:
        return (True, version) if _waited(first_seen, now) else (False, recorded)
    if recorded_version == version:
        return False, recorded
    return False, f"{EMPTY_MARK}|{now.isoformat(timespec='seconds')}"


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


def _refuse_mass_unpublish(unpublishing: int, known: int, reason: str) -> None:
    if (
        known >= MIN_COURSES_FOR_UNPUBLISH_GUARD
        and unpublishing > known * MAX_UNPUBLISH_FRACTION
    ):
        msg = (
            f"{unpublishing} of the {known} known OCW courses {reason}. "
            "Refusing to unpublish that many in one run."
        )
        raise RuntimeError(msg)


def _unpublished_rows(
    versions: dict[str, str], live: Sequence[str], retrieved_at: datetime
) -> list[dict[str, Any]]:
    """Build the row that empties each known course no longer in the bucket."""
    gone = sorted(set(versions) - set(live))
    _refuse_mass_unpublish(
        len(gone), len(versions), "are missing from the bucket listing"
    )
    return [_unpublished_row(slug, retrieved_at) for slug in gone]


@dlt.source(name="ocw_content_ingest")
def ocw_content_source(
    bucket: str = BUCKET,
    courses: Sequence[str] | None = None,
    budget_bytes: int = BUDGET_BYTES,
    table_format: config.TableFormat | None = None,
) -> Any:  # noqa: ANN401
    """Load the content of the OCW courses that changed since the last load.

    :param bucket: The OCW live bucket.
    :param courses: Slugs to read again whether or not they changed, e.g. to
        retry files Tika failed on. Reads every changed course when None.
    :param budget_bytes: How many source bytes one load may read. A course is
        never split, so a load can exceed it by one course. Ignored when
        ``courses`` is given.
    :param table_format: ``native`` or ``iceberg``; defaults to the active
        profile's.
    """

    @dlt.resource(
        name=RAW_TABLE,
        write_disposition="append",
        table_format=table_format or config.active_table_format(),
        columns=config.DLT_LOAD_ID_COLUMN,
    )
    def course_content() -> Iterator[pa.Table]:
        state = dlt.current.resource_state()
        versions: dict[str, str] = state.setdefault("course_versions", {})
        external_files: dict[str, dict[str, str]] = state.setdefault(
            "external_files", {}
        )
        now = datetime.now(tz=UTC)
        known = len(versions)
        fs = s3fs.S3FileSystem(anon=True, use_listings_cache=False)
        live = list_courses(fs, bucket)

        unpublishing = 0
        if courses is None:
            unpublished = _unpublished_rows(versions, live, now)
            if unpublished:
                yield pa.Table.from_pylist(unpublished, schema=SCHEMA)
            for row in unpublished:
                del versions[row["course_slug"]]
                external_files.pop(row["course_slug"], None)
            unpublishing = len(unpublished)
            # A sweep that a load's budget cut short resumes after the last
            # course it read, then wraps round to the ones before it.
            resume_after = state.get("resume_after", "")
            pending = [slug for slug in live if slug > resume_after] + [
                slug for slug in live if slug <= resume_after
            ]
        else:
            pending = sorted(set(courses) & set(live))

        # Built on this thread: the workers must not touch dlt's config, whose
        # injectable context is not thread-safe (see .dlt/config.toml).
        client: tika.TikaClient | None = None
        spent = 0
        pool = ThreadPoolExecutor(max_workers=WORKERS)
        try:
            for start in range(0, len(pending), LISTING_WINDOW):
                window = pending[start : start + LISTING_WINDOW]
                listings = pool.map(lambda slug: list_course(fs, bucket, slug), window)
                for slug, objects in zip(window, listings, strict=True):
                    version = course_version(slug, objects)
                    recorded = versions.get(slug)
                    if not any(content_kind(slug, key) for key in objects):
                        unpublish, versions[slug] = settle_empty_course(
                            recorded, version, now
                        )
                        if unpublish:
                            unpublishing += 1
                            _refuse_mass_unpublish(
                                unpublishing, known, "are gone or have no data.json"
                            )
                            external_files.pop(slug, None)
                            row = _unpublished_row(slug, datetime.now(tz=UTC))
                            yield pa.Table.from_pylist([row], schema=SCHEMA)
                        continue
                    if (
                        courses is None
                        and not needs_read(recorded, version, now)
                        and not external_files_changed(
                            fs, bucket, external_files.get(slug)
                        )
                    ):
                        continue
                    client = client or tika.client_for_profile()
                    rows, source_bytes, retry, external = read_course(
                        fs=fs,
                        pool=pool,
                        client=client,
                        bucket=bucket,
                        slug=slug,
                        objects=objects,
                        version=version,
                    )
                    yield pa.Table.from_pylist(rows, schema=SCHEMA)
                    versions[slug] = record_read(recorded, version, now, retry=retry)
                    if external:
                        external_files[slug] = external
                    else:
                        external_files.pop(slug, None)
                    spent += source_bytes
                    if courses is None and spent >= budget_bytes:
                        state["resume_after"] = slug
                        return
        finally:
            # Not waited for: when Tika is down, the calls in flight would each
            # hold the failure back by a timeout. Nothing is in flight when
            # the sweep ends normally.
            pool.shutdown(wait=False, cancel_futures=True)
        if courses is None:
            state["resume_after"] = ""

    return course_content


ocw_content_pipeline = config.pipeline_for("ocw", pipeline_name="ocw_content")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source for the active profile."""
    return config.with_nullable_load_id(ocw_content_source())


if __name__ == "__main__":
    print(ocw_content_pipeline.run(build_source()))  # noqa: T201

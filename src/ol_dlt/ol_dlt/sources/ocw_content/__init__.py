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
the ETags of the objects above and of the files under its own prefix, and of
``TEXT_FILE_RULE``. A publish that rewrites identical bytes keeps every ETag, so
it changes nothing here. A changed course appends a full new set of rows stamped
with one ``course_retrieved_at`` and its version, and staging keeps each course's
newest set. A course that leaves the bucket appends one ``unpublished`` row,
which becomes its newest set and so empties it downstream. So does a course
whose prefix has no ``data.json`` on two reads ``WAIT_BETWEEN_READS`` apart.

What a load knows about a course comes from those rows (``recorded_courses``),
which commit with the load that wrote them. So loads of different courses share
no state to lose, whichever pipeline they run on. dlt state holds only what the
sweep alone writes: its resume point, the unpublish count and the prefixes it
saw empty.

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

A load can also be scoped to named courses, which is how a course is loaded
soon after it is published rather than at the next sweep (the
``ocw_content_publish_sensor`` in the data_loading code location, which finds
them by ``published_at``). A scoped load runs on its own pipeline, so it cannot
touch the sweep's state. The sweep sees its rows and does not read the course
again. It leaves the unpublish rules alone: it neither unpublishes a course nor
counts toward the limit, because it has not seen the rest of the bucket.

Tika credentials: see ``ol_dlt.tika``.

Run standalone, as a sweep, or for the courses whose slugs follow it:
    DLT_PROFILE=dev TIKA_ACCESS_TOKEN=... python -m ol_dlt.sources.ocw_content
"""

import hashlib
import json
import logging
import sys
from collections.abc import Iterator, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import unquote

import dlt
import pyarrow as pa
import s3fs
from dlt.common.destination.exceptions import DestinationUndefinedEntity
from dlt.common.libs import pyiceberg as dlt_pyiceberg
from dlt.common.libs.pyarrow import py_arrow_to_table_schema_columns
from dlt.common.schema.utils import new_table
from pyiceberg.exceptions import NoSuchTableError

from ol_dlt import config, tika
from ol_dlt.extraction import (
    STATUS_FAILED,
    STATUS_MISSING,
    Extraction,
    collect,
    extract,
    needs_read,
    waited,
)

logger = logging.getLogger(__name__)

BUCKET = "ocw-content-live-production"
RAW_TABLE = "raw__ocw__s3__course_content"
SOURCE_NAME = "ocw_content_ingest"
# The bucket prefix the table lives under (config.destination_for).
PIPELINE_SOURCE = "ocw"

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

# Part of every course version, so that changing it reads every course again.
# Raise it when text_file_key picks a different file for data.json that has
# not changed. 2: a video's transcript comes from video_transcript_resources
# when it has one (mit-learn #4063), and a path is split on "courses" only when
# it does not start with it.
TEXT_FILE_RULE = 2

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
# HEAD requests in flight at once when reading every course's publish time.
PUBLISH_CHECK_WORKERS = 32

# A run that would unpublish more of the known courses than this fails
# instead: an unpublished course loses its ContentFiles in MIT Learn.
MAX_UNPUBLISH_FRACTION = 0.1
MIN_COURSES_FOR_UNPUBLISH_GUARD = 20
# A sweep may find this share of the courses the last one knew, and no less. It
# unpublishes at most MAX_UNPUBLISH_FRACTION of them, so less means a bad read.
MIN_RECORDED_FRACTION = 0.5

# A larger file is recorded as failed without being read: eight are held in
# memory at once. The largest of 800 MB sampled across 60 courses was under
# 100 MB.
MAX_FILE_BYTES = 256 * 1024**2

KIND_COURSE = "course"
KIND_PAGE = "page"
KIND_RESOURCE = "resource"
KIND_UNPUBLISHED = "unpublished"

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

# What a load reads back to tell what it knows about each course.
RECORD_COLUMNS = (
    "course_slug",
    "content_kind",
    "file_key",
    "file_etag",
    "extraction_status",
    "course_version",
    "course_retrieved_at",
)

type CourseObjects = dict[str, dict[str, Any]]


@dataclass(frozen=True)
class CourseRecord:
    """What the raw table holds about a course's newest read.

    :param state: The course's version, or ``version|attempts|time`` while a
        file of that version is failing in a way that may pass.
    :param read_at: When that read happened.
    :param external: The ETag of each file read that the version does not cover.
    """

    state: str
    read_at: datetime
    external: dict[str, str] = field(default_factory=dict)


def video_transcript_path(video_files: dict[str, Any]) -> str | None:
    """Return the path of a video's transcript, as Learn passes it on.

    Follows ``get_video_transcript_path`` in mit-learn: a republished course
    lists one transcript per language under ``video_transcript_resources``, and
    the first English one that names a file wins, else the first that names a
    file, with its leading slashes stripped. Older data has the single
    ``video_transcript_file``, passed on as it is.

    :param video_files: The ``video_files`` object of a video resource.
    :returns: The path, or None when the video has no transcript or Learn raises
        reading the list, which drops the resource.
    """
    resources = video_files.get("video_transcript_resources") or []
    # Learn calls .get on every entry and .lower on the language of every entry
    # that names a file, so a value that is not a list, an entry that is not an
    # object, or a language that is not a string raises.
    if not isinstance(resources, list) or not all(
        isinstance(entry, dict) for entry in resources
    ):
        return None
    transcripts = [entry for entry in resources if entry.get("file")]
    if any(
        entry.get("language") and not isinstance(entry["language"], str)
        for entry in transcripts
    ):
        return None
    if transcripts:
        english = [
            entry
            for entry in transcripts
            if (entry.get("language") or "").lower().startswith("en")
        ]
        path = (english or transcripts)[0]["file"]
        # Learn's .lstrip raises on a file that is not a string.
        return path.lstrip("/") if isinstance(path, str) else None
    return video_files.get("video_transcript_file")


def text_file_key(resource: dict[str, Any]) -> str | None:
    """Return the S3 key of the file Learn extracts text from for a resource.

    Follows ``transform_contentfile`` and ``transform_contentfile_legacy`` in
    mit-learn: a video's text is its transcript, anything else's is its file.

    :param resource: A parsed ``resources/**/data.json``.
    :returns: The key, or None when Learn would read no text.
    """
    if resource.get("resourcetype"):
        if resource["resourcetype"] == "Video":
            path = video_transcript_path(resource.get("video_files") or {})
        else:
            path = resource.get("file")
    elif resource.get("resource_type") == "Video":
        path = resource.get("transcript_file")
    else:
        path = resource.get("file")

    if not isinstance(path, str) or resource.get("title") in SKIPPED_TITLES:
        return None
    # Learn's own split, for a path not already starting with "courses": a path
    # with no "courses" raises there, and one naming "courses" twice keeps only
    # what lies between the first two. The extension is the split path's.
    if not path.startswith("courses"):
        if "courses" not in path:
            return None
        path = "courses" + path.split("courses")[1]
    if PurePosixPath(path).suffix.lower() not in VALID_FILE_TYPES:
        return None
    return unquote(path)


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


def in_course_version(slug: str, key: str) -> bool:
    """Tell whether a key of a course's own listing counts toward its version."""
    if content_kind(slug, key):
        return True
    prefix = f"courses/{slug}/"
    path = PurePosixPath(key)
    return (
        not key.startswith((f"{prefix}pages/", f"{prefix}resources/"))
        and path.name not in SITE_FILES
        and path.suffix.lower() in VALID_FILE_TYPES
    )


def course_version(slug: str, objects: CourseObjects) -> str:
    """Digest the ETags of the listed objects a course's rows are built from.

    A file a resource points at under ``pages/`` or ``resources/`` is left out,
    as is one under another course. ``read_course`` returns the ETags of those.
    """
    digest = hashlib.md5(usedforsecurity=False)
    # Not for a prefix with no data.json: a new rule would start its wait to be
    # unpublished again, and count it against the unpublish limit.
    if any(content_kind(slug, key) for key in objects):
        digest.update(f"text_file_rule\t{TEXT_FILE_RULE}\n".encode())
    for key in sorted(objects):
        if in_course_version(slug, key):
            digest.update(f"{key}\t{objects[key]['ETag']}\n".encode())
    return digest.hexdigest()


def list_courses(fs: s3fs.S3FileSystem, bucket: str) -> list[str]:
    """Return every published course slug, sorted."""
    return sorted(
        PurePosixPath(path).name for path in fs.ls(f"{bucket}/courses/", detail=False)
    )


def published_at(fs: s3fs.S3FileSystem, bucket: str) -> dict[str, datetime]:
    """Return when each course's ``data.json`` was last written, by slug.

    An online publish writes ``data.json`` with the rest of the course's
    build: for the 39 courses published 2026-10-16 to 2026-10-19, the build's
    last object landed at most 3 s after it. So this is the time of the
    course's last publish. A mass rebuild writes it too, often without
    changing it. One HEAD per course, where a listing of every object in the
    bucket takes ~456 requests.

    :returns: The time, for each course that has a ``data.json``.
    """
    slugs = list_courses(fs, bucket)
    with ThreadPoolExecutor(max_workers=PUBLISH_CHECK_WORKERS) as pool:
        infos = pool.map(
            lambda slug: _object_info(fs, bucket, f"courses/{slug}/data.json"), slugs
        )
        return {
            slug: info["LastModified"]
            for slug, info in zip(slugs, infos, strict=True)
            if info is not None
        }


def list_course(fs: s3fs.S3FileSystem, bucket: str, slug: str) -> CourseObjects:
    """Return every object of one course, keyed by S3 key."""
    listing = fs.find(f"{bucket}/courses/{slug}/", detail=True)
    return {name.removeprefix(f"{bucket}/"): info for name, info in listing.items()}


def _etag(info: dict[str, Any]) -> str:
    return info["ETag"].strip('"')


def _extract_text(
    fs: s3fs.S3FileSystem,
    client: tika.TikaClient,
    bucket: str,
    key: str,
    size: int,
) -> Extraction:
    """Return a file's text, how the extraction went, and whether to retry it."""
    if size > MAX_FILE_BYTES:
        logger.warning("%s is %s bytes, over the limit; not extracted", key, size)
        return None, STATUS_FAILED, False
    try:
        body = fs.cat_file(f"{bucket}/{key}")
    except FileNotFoundError:
        # Listed a moment ago and gone now, e.g. a publish in progress. If the
        # same bytes come back the ETags will not show it, so the course is
        # read again.
        return None, STATUS_MISSING, True
    return extract(client, body, key)


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
) -> tuple[list[dict[str, Any]], int]:
    """Read one course's ``data.json`` files and the text of its resource files.

    :returns: The course's rows and how many source bytes they were read from.
    :raises TikaUnavailableError: Tika is down.
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

    results = pool.map(
        lambda key: _extract_text(fs, client, bucket, key, files[key]["size"]),
        files,
    )
    texts = dict(zip(files, collect(client, slug, results), strict=True))

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

    source_bytes = sum(objects[key]["size"] for key in json_keys) + sum(
        info["size"] for info in files.values()
    )
    return rows, source_bytes


def external_files_changed(
    fs: s3fs.S3FileSystem, bucket: str, recorded: dict[str, str] | None
) -> bool:
    """Tell whether a file a course read outside its version has changed.

    That is a file under another course, or under this course's ``pages/`` or
    ``resources/``.
    """
    for key, etag in (recorded or {}).items():
        info = _object_info(fs, bucket, key)
        if info is None or _etag(info) != etag:
            return True
    return False


def course_records(rows: Sequence[dict[str, Any]]) -> dict[str, CourseRecord]:
    """Work out what is known about each course from its rows.

    A course whose newest row marks it unpublished is not known: it has no
    content, and if it comes back it is read like a new one. A read with a failed
    or missing file is due another, up to ``MAX_ATTEMPTS``. That includes a file
    Tika refused, one over the size limit and one a resource names but the
    bucket lacks, which another read will not fix, so a course with one is read
    twice more than it needs.

    :param rows: ``RECORD_COLUMNS`` of the raw table, in any order.
    :returns: A record for each known course, by slug.
    """
    by_course: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_course.setdefault(row["course_slug"], []).append(row)

    records = {}
    for slug, course_rows in by_course.items():
        newest = max(row["course_retrieved_at"] for row in course_rows)
        latest = [row for row in course_rows if row["course_retrieved_at"] == newest]
        if any(row["content_kind"] == KIND_UNPUBLISHED for row in latest):
            continue
        version = latest[0]["course_version"]
        state = version
        if any(
            row["extraction_status"] in {STATUS_FAILED, STATUS_MISSING}
            for row in latest
        ):
            attempts = len(
                {
                    row["course_retrieved_at"]
                    for row in course_rows
                    if row["course_version"] == version
                }
            )
            state = f"{version}|{attempts}|{newest.isoformat(timespec='seconds')}"
        records[slug] = CourseRecord(
            state,
            newest,
            {
                row["file_key"]: row["file_etag"]
                for row in latest
                if row["file_key"]
                and row["file_etag"]
                and not (
                    row["file_key"].startswith(f"courses/{slug}/")
                    and in_course_version(slug, row["file_key"])
                )
            },
        )
    return records


def _iceberg_records() -> dict[str, CourseRecord]:
    """Read the records through the catalog.

    Not through ``dlt.dataset``, which finds the newest metadata file by listing
    the table's directory. That can be one a failed commit left behind, and a
    directory that is not where dlt expects reads as no table, which would read
    every course again.
    """
    destination = config.destination_for(PIPELINE_SOURCE)
    catalog = dlt_pyiceberg.get_catalog(
        credentials=destination.config_params.get("credentials")
    )
    try:
        table = catalog.load_table((config.dataset_name(), RAW_TABLE))
    except NoSuchTableError:
        return {}
    return course_records(
        table.scan(selected_fields=RECORD_COLUMNS).to_arrow().to_pylist()
    )


def recorded_courses() -> dict[str, CourseRecord]:
    """Read what the raw table holds about every course.

    :returns: A record for each known course, by slug; empty when the table does
        not exist yet.
    """
    if config.active_table_format() == "iceberg":
        return _iceberg_records()
    # Declared here, not the stored schema dlt would pick. That is the newest
    # one, and a pipeline's first load, if empty, stores one without this table.
    schema = dlt.Schema(SOURCE_NAME)
    table = new_table(
        RAW_TABLE,
        columns=list(py_arrow_to_table_schema_columns(SCHEMA).values()),
        table_format=config.active_table_format(),
    )
    # dlt only reads a table it has seen data for.
    table["x-normalizer"] = {"seen-data": True}
    schema.update_table(table)
    dataset = dlt.dataset(
        config.destination_for(PIPELINE_SOURCE), config.dataset_name(), schema=schema
    )
    relation = dataset[RAW_TABLE].select(*RECORD_COLUMNS)
    try:
        return course_records(relation.arrow().to_pylist())
    except DestinationUndefinedEntity:
        return {}


def _unpublished_row(slug: str, retrieved_at: datetime) -> dict[str, Any]:
    return {
        **dict.fromkeys(SCHEMA.names),
        "course_slug": slug,
        "content_kind": KIND_UNPUBLISHED,
        "course_retrieved_at": retrieved_at,
    }


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
        return (True, version) if waited(first_seen, now) else (False, recorded)
    if recorded_version == version:
        return False, recorded
    return False, f"{EMPTY_MARK}|{now.isoformat(timespec='seconds')}"


def migrate_course_versions(state: dict[str, Any], empty: dict[str, str]) -> None:
    """Drop the per-course state a load kept before it read the raw table.

    The rows hold everything it held except a prefix's empty mark. How many
    courses it knew stays, for ``refuse_lost_records`` to check the rows against.
    """
    versions = state.pop("course_versions", {})
    for slug, recorded in versions.items():
        if recorded.startswith(EMPTY_MARK):
            empty[slug] = recorded
    state.pop("external_files", None)
    if versions:
        state["known_courses"] = len(versions)


def refuse_lost_records(state: dict[str, Any], recorded: int) -> None:
    """Fail a sweep that finds far fewer courses in the table than the last one.

    Every course it does not find is read again through Tika. Without this, a
    read that came back short would cost ~70,000 Tika calls and a second copy of
    the table, and nothing would say why.
    """
    known = state.get("known_courses", 0)
    if recorded < known * MIN_RECORDED_FRACTION:
        msg = (
            f"The raw table holds {recorded} courses and the last sweep knew "
            f"{known}. Refusing to read them all again."
        )
        raise RuntimeError(msg)
    state["known_courses"] = recorded


def read_since(mark: str, record: CourseRecord | None) -> bool:
    """Tell whether a course was read after its prefix was seen empty.

    If so the prefix was a publish in progress, and the mark says nothing now.
    """
    _, _, first_seen = mark.partition("|")
    if record is None:
        return False
    # The mark holds whole seconds, and a read in the same second came before it.
    return record.read_at.replace(microsecond=0) > datetime.fromisoformat(first_seen)


def count_unpublished(
    state: dict[str, Any], now: datetime, added: int, known: int
) -> None:
    """Add to the courses unpublished lately, refusing to pass the limit.

    Counted in dlt state over ``WAIT_BETWEEN_READS`` and not per load, because
    one sweep is split across loads wherever the byte budget falls.
    """
    window = state.get("unpublish_window")
    if window is None or waited(window["since"], now):
        window = {"since": now.isoformat(timespec="seconds"), "count": 0}
    count = window["count"] + added
    if (
        known >= MIN_COURSES_FOR_UNPUBLISH_GUARD
        and count > known * MAX_UNPUBLISH_FRACTION
    ):
        msg = (
            f"{count} of the {known} known OCW courses are gone from the "
            "bucket listing or have no data.json. Refusing to unpublish that "
            "many in one run."
        )
        raise RuntimeError(msg)
    state["unpublish_window"] = {**window, "count": count}


def _unpublished_rows(
    known: dict[str, CourseRecord], live: Sequence[str], retrieved_at: datetime
) -> list[dict[str, Any]]:
    """Build the row that empties each known course no longer in the bucket."""
    gone = sorted(set(known) - set(live))
    return [_unpublished_row(slug, retrieved_at) for slug in gone]


@dlt.source(name=SOURCE_NAME)
def ocw_content_source(  # noqa: PLR0913
    bucket: str = BUCKET,
    courses: Sequence[str] | None = None,
    budget_bytes: int = BUDGET_BYTES,
    table_format: config.TableFormat | None = None,
    *,
    force: bool = False,
) -> Any:  # noqa: ANN401
    """Load the content of the OCW courses that changed since the last load.

    :param bucket: The OCW live bucket.
    :param courses: Slugs to load on their own, e.g. the courses a publish
        just wrote. A slug not in the bucket, or whose prefix has no
        ``data.json``, is skipped and left to the sweep. Sweeps every course
        when None.
    :param budget_bytes: How many source bytes one load may read. A course is
        never split, so a load can exceed it by one course. A scoped load stops
        there too, and the next one skips the courses it read. Ignored with
        ``force``, which would read them again.
    :param table_format: ``native`` or ``iceberg``; defaults to the active
        profile's.
    :param force: Read the named ``courses`` whether or not they changed, e.g.
        to retry files Tika failed on.
    :raises ValueError: ``force`` without ``courses``.
    """
    if force and courses is None:
        msg = "force reads the named courses again; name them in courses."
        raise ValueError(msg)

    @dlt.resource(
        name=RAW_TABLE,
        write_disposition="append",
        table_format=table_format or config.active_table_format(),
        columns=config.DLT_LOAD_ID_COLUMN,
    )
    def course_content() -> Iterator[pa.Table]:
        state = dlt.current.resource_state()
        # Only the sweep writes this: the prefixes it has seen empty.
        empty_prefixes: dict[str, str] = state.setdefault("empty_prefixes", {})
        migrate_course_versions(state, empty_prefixes)
        recorded = recorded_courses()
        now = datetime.now(tz=UTC)
        known = len(recorded)
        fs = s3fs.S3FileSystem(anon=True, use_listings_cache=False)
        live = list_courses(fs, bucket)

        if courses is None:
            refuse_lost_records(state, known)
            unpublished = _unpublished_rows(recorded, live, now)
            if unpublished:
                count_unpublished(state, now, len(unpublished), known)
                yield pa.Table.from_pylist(unpublished, schema=SCHEMA)
            for row in unpublished:
                del recorded[row["course_slug"]]
                empty_prefixes.pop(row["course_slug"], None)
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
                    record = recorded.get(slug)
                    read_state = record.state if record else None
                    if not any(content_kind(slug, key) for key in objects):
                        if courses is not None:
                            logger.info("%s has no data.json; left to the sweep", slug)
                            continue
                        mark = empty_prefixes.get(slug)
                        if mark and read_since(mark, record):
                            mark = None
                        unpublish, held = settle_empty_course(
                            mark or read_state, version, now
                        )
                        if held.startswith(EMPTY_MARK):
                            empty_prefixes[slug] = held
                        else:
                            empty_prefixes.pop(slug, None)
                        if unpublish:
                            count_unpublished(state, now, 1, known)
                            row = _unpublished_row(slug, datetime.now(tz=UTC))
                            yield pa.Table.from_pylist([row], schema=SCHEMA)
                        continue
                    empty_prefixes.pop(slug, None)
                    if (
                        not force
                        and not needs_read(read_state, version, now)
                        and not external_files_changed(
                            fs, bucket, record.external if record else None
                        )
                    ):
                        continue
                    client = client or tika.client_for_profile()
                    try:
                        rows, source_bytes = read_course(
                            fs=fs,
                            pool=pool,
                            client=client,
                            bucket=bucket,
                            slug=slug,
                            objects=objects,
                            version=version,
                        )
                    except FileNotFoundError:
                        # A data.json listed a moment ago is gone: the course
                        # is being published. Nothing is recorded, so the
                        # next load lists it afresh and reads it.
                        logger.warning("%s changed while it was read; skipped", slug)
                        continue
                    yield pa.Table.from_pylist(rows, schema=SCHEMA)
                    spent += source_bytes
                    if spent >= budget_bytes and not force:
                        if courses is None:
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


# The sweep's pipeline, the only one with state that matters.
ocw_content_pipeline = config.pipeline_for(PIPELINE_SOURCE, pipeline_name="ocw_content")
# Loads of named courses. A load saves its pipeline's whole state, so these must
# not share the sweep's pipeline, or one would drop the sweep's resume point.
ocw_course_pipeline = config.pipeline_for(
    PIPELINE_SOURCE, pipeline_name="ocw_content_courses"
)


def build_source(courses: Sequence[str] | None = None) -> Any:  # noqa: ANN401
    """Instantiate the source for the active profile.

    :param courses: Slugs to load on their own; sweeps every course when None.
    """
    return config.with_nullable_load_id(ocw_content_source(courses=courses))


if __name__ == "__main__":
    named = sys.argv[1:] or None
    pipeline = ocw_course_pipeline if named else ocw_content_pipeline
    print(pipeline.run(build_source(named)))  # noqa: T201

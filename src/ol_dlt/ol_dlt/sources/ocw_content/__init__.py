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
the ETags of the objects above and of the files a resource can point at, held
in dlt state. A publish that rewrites identical bytes keeps every ETag, so it
changes nothing here. A changed course appends a full new set of rows stamped
with one ``course_retrieved_at``, and staging keeps each course's newest set.
A course that leaves the bucket appends one ``unpublished`` row, which becomes
its newest set and so empties it downstream.

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
from datetime import UTC, datetime
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

# A course with at least this many files to extract and no text from any of
# them means Tika is down, not that the files are bad.
MIN_FILES_FOR_OUTAGE = 5

KIND_COURSE = "course"
KIND_PAGE = "page"
KIND_RESOURCE = "resource"
KIND_UNPUBLISHED = "unpublished"

STATUS_EXTRACTED = "extracted"
STATUS_EMPTY = "empty"
STATUS_FAILED = "failed"
STATUS_MISSING = "missing"

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
    return unquote("courses" + path.split("courses", maxsplit=1)[1])


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


def _extract_text(
    fs: s3fs.S3FileSystem, client: tika.TikaClient, bucket: str, key: str
) -> tuple[str | None, str]:
    """Return a file's text and how the extraction went."""
    body = fs.cat_file(f"{bucket}/{key}")
    if not body:
        return None, STATUS_EMPTY
    try:
        text = client.extract_text(body)
    except requests.RequestException:
        logger.exception("Tika could not read %s", key)
        return None, STATUS_FAILED
    return (text, STATUS_EXTRACTED) if text else (None, STATUS_EMPTY)


def read_course(  # noqa: PLR0913
    *,
    fs: s3fs.S3FileSystem,
    pool: ThreadPoolExecutor,
    client: tika.TikaClient,
    bucket: str,
    slug: str,
    objects: CourseObjects,
    version: str,
    retrieved_at: datetime,
) -> tuple[list[dict[str, Any]], int]:
    """Read one course's ``data.json`` files and the text of its resource files.

    :returns: The course's rows and how many source bytes they were read from.
    :raises RuntimeError: No file of the course yielded text, which reads as
        a Tika outage rather than a course of unreadable files.
    """
    json_keys = [key for key in sorted(objects) if content_kind(slug, key)]
    bodies = pool.map(lambda key: fs.cat_file(f"{bucket}/{key}"), json_keys)

    rows: list[dict[str, Any]] = []
    for key, body in zip(json_keys, bodies, strict=True):
        data_json = body.decode("utf-8")
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

    file_keys = sorted({row["file_key"] for row in rows if row["file_key"] in objects})
    texts = dict(
        zip(
            file_keys,
            pool.map(lambda key: _extract_text(fs, client, bucket, key), file_keys),
            strict=True,
        )
    )
    failed = sum(status == STATUS_FAILED for _text, status in texts.values())
    if len(file_keys) >= MIN_FILES_FOR_OUTAGE and failed == len(file_keys):
        msg = (
            f"Tika extracted none of the {failed} files of {slug}. Treating it "
            "as a Tika outage, so the course is not recorded as read."
        )
        raise RuntimeError(msg)

    for row in rows:
        file_key = row["file_key"]
        if file_key is None:
            continue
        if file_key not in objects:
            row["extraction_status"] = STATUS_MISSING
            continue
        row["content"], row["extraction_status"] = texts[file_key]
        row["file_etag"] = _etag(objects[file_key])
        row["file_size_bytes"] = objects[file_key]["size"]

    source_bytes = sum(objects[key]["size"] for key in (*json_keys, *file_keys))
    return rows, source_bytes


def _unpublished_rows(
    versions: dict[str, str], live: Sequence[str], retrieved_at: datetime
) -> list[dict[str, Any]]:
    """Build the row that empties each known course no longer in the bucket."""
    gone = sorted(set(versions) - set(live))
    if (
        len(versions) >= MIN_COURSES_FOR_UNPUBLISH_GUARD
        and len(gone) > len(versions) * MAX_UNPUBLISH_FRACTION
    ):
        msg = (
            f"{len(gone)} of the {len(versions)} known OCW courses are missing "
            "from the bucket listing. Refusing to unpublish that many in one "
            "run."
        )
        raise RuntimeError(msg)
    return [
        {
            **dict.fromkeys(SCHEMA.names),
            "course_slug": slug,
            "content_kind": KIND_UNPUBLISHED,
            "course_retrieved_at": retrieved_at,
        }
        for slug in gone
    ]


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
        retrieved_at = datetime.now(tz=UTC)
        fs = s3fs.S3FileSystem(anon=True, use_listings_cache=False)
        live = list_courses(fs, bucket)

        if courses is None:
            unpublished = _unpublished_rows(versions, live, retrieved_at)
            if unpublished:
                yield pa.Table.from_pylist(unpublished, schema=SCHEMA)
            for row in unpublished:
                del versions[row["course_slug"]]
            resume_after = state.get("resume_after", "")
            pending = [slug for slug in live if slug > resume_after]
        else:
            pending = sorted(set(courses) & set(live))

        # Built on this thread: the workers must not touch dlt's config, whose
        # injectable context is not thread-safe (see .dlt/config.toml).
        client: tika.TikaClient | None = None
        spent = 0
        with ThreadPoolExecutor(max_workers=WORKERS) as pool:
            for start in range(0, len(pending), LISTING_WINDOW):
                window = pending[start : start + LISTING_WINDOW]
                listings = pool.map(lambda slug: list_course(fs, bucket, slug), window)
                for slug, objects in zip(window, listings, strict=True):
                    version = course_version(slug, objects)
                    if courses is None and versions.get(slug) == version:
                        continue
                    client = client or tika.client_for_profile()
                    rows, source_bytes = read_course(
                        fs=fs,
                        pool=pool,
                        client=client,
                        bucket=bucket,
                        slug=slug,
                        objects=objects,
                        version=version,
                        retrieved_at=retrieved_at,
                    )
                    yield pa.Table.from_pylist(rows, schema=SCHEMA)
                    versions[slug] = version
                    spent += source_bytes
                    if courses is None and spent >= budget_bytes:
                        state["resume_after"] = slug
                        return
        if courses is None:
            state["resume_after"] = ""

    return course_content


ocw_content_pipeline = config.pipeline_for("ocw", pipeline_name="ocw_content")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source for the active profile."""
    return config.with_nullable_load_id(ocw_content_source())


if __name__ == "__main__":
    print(ocw_content_pipeline.run(build_source()))  # noqa: T201

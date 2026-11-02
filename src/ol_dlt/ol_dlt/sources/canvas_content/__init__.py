"""Canvas course content ingestion via dlt.

``data_loading.defs.canvas`` exports every course in the course ID sheet to the
landing zone every six hours, as a Common Cartridge and a metadata JSON with
the course's URLs:

    canvas/course_content/<course_id>/<version>.imscc
    canvas/course_content/<course_id>/<version>.metadata.json

MIT Learn builds its Canvas course and ContentFiles from the newest export of
each course (``learning_resources/etl/canvas.py`` in mit-learn). This source
reads the same objects and follows the same rules (``cartridge``), so the
warehouse holds what Learn would have parsed:

    the course's settings                   -> content_kind = course
    each published file with a text type    -> content_kind = file
        + the text Tika extracts from it
    -> raw__canvas__s3__course_content

A course is read whole, and only when it changed. Its version is
``cartridge.course_checksum``, held in dlt state, which also moves when a file's
availability date passes. A changed course appends a full new set of rows
stamped with one ``course_retrieved_at``, and staging keeps each course's newest
set. A course whose folder leaves the landing zone appends one ``unpublished``
row, which becomes its newest set and so empties it downstream.

The PDFs differ from Learn. Learn sends a PDF of up to ``OCR_PDF_MAX_PAGE_THRESHOLD``
pages through opendataloader and an LLM (``_extract_content_with_ocr``, with
mit-learn #4126's changes), and falls back to Tika for a longer one or a failed
conversion. Here every PDF goes through Tika. See
``integrations__learn__canvas_content_files``.

One load covers at most ``budget_bytes`` of archive members. The caller re-runs
the source until a load reads nothing (``build_batched_assets`` in the
data_loading code location), and the sweep resumes after the last course a
load finished.

Tika credentials: see ``ol_dlt.tika``.

Run standalone, as a sweep:
    DLT_PROFILE=dev TIKA_ACCESS_TOKEN=... python -m ol_dlt.sources.canvas_content
"""

import hashlib
import io
import json
import logging
import mimetypes
import re
import zipfile
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import Any

import dlt
import pyarrow as pa
import pypdf
import s3fs

from ol_dlt import config, tika
from ol_dlt.extraction import (
    STATUS_EMPTY,
    STATUS_FAILED,
    Extraction,
    collect,
    extract,
    needs_read,
    record_read,
)
from ol_dlt.sources.canvas_content import cartridge

logger = logging.getLogger(__name__)

# Where Learn reads Canvas exports in QA and production alike
# (CANVAS_COURSE_BUCKET_NAME in ol-infrastructure).
BUCKET = "ol-data-lake-landing-zone-production"
PREFIX = "canvas/course_content"
RAW_TABLE = "raw__canvas__s3__course_content"

# VALID_TEXT_FILE_TYPES in mit-learn's learning_resources/constants.py.
VALID_TEXT_FILE_TYPES = frozenset(
    {
        ".doc",
        ".docx",
        ".htm",
        ".html",
        ".json",
        ".md",
        ".pdf",
        ".tex",
        ".ppt",
        ".pptx",
        ".rtf",
        ".sjson",
        ".srt",
        ".txt",
        ".vtt",
        ".xml",
    }
)

# Learn walks the extracted tree and skips any directory with this in its path.
DRAFT_MARKER = "draft"
# The key of a file in a folder of this name is an asset's, not a block's.
STATIC_FOLDER = "static"

# Member bytes one load may read. See ol_dlt.sources.ocw_content, which holds
# the same load of text.
BUDGET_BYTES = 2 * 1024**3

# Archive reads and Tika calls in flight at once.
WORKERS = 8
# A larger member is recorded as failed without being read: eight are held in
# memory at once.
MAX_FILE_BYTES = 256 * 1024**2
# An archive is read in small ranges: the members a read wants are a fraction of
# it, and the rest is video.
ARCHIVE_BLOCK_BYTES = 1024**2

KIND_COURSE = "course"
KIND_FILE = "file"
KIND_UNPUBLISHED = "unpublished"

SCHEMA = pa.schema(
    [
        pa.field("course_id", pa.string()),
        pa.field("content_kind", pa.string()),
        pa.field("archive_key", pa.string()),
        pa.field("run_readable_id", pa.string()),
        # A course row's.
        pa.field("course_code", pa.string()),
        pa.field("course_title", pa.string()),
        pa.field("course_url", pa.string()),
        pa.field("course_start_at", pa.string()),
        pa.field("course_end_at", pa.string()),
        # A file row's.
        pa.field("file_key", pa.string()),
        pa.field("source_path", pa.string()),
        pa.field("file_extension", pa.string()),
        pa.field("title", pa.string()),
        pa.field("content_title", pa.string()),
        pa.field("url", pa.string()),
        pa.field("file_md5", pa.string()),
        pa.field("file_size_bytes", pa.int64()),
        pa.field("content", pa.string()),
        pa.field("extraction_status", pa.string()),
        pa.field("course_version", pa.string()),
        pa.field("course_retrieved_at", pa.timestamp("us", tz="UTC")),
    ]
)

# Without the system's mime.types, so a type does not depend on the image.
_MIMETYPES = mimetypes.MimeTypes(filenames=())


def latest_archives(fs: s3fs.S3FileSystem, bucket: str) -> dict[str, str]:
    """Return the key of each course's newest ``.imscc``, by course id.

    A rewritten export keeps its name, so the newest is the last written.
    """
    newest: dict[str, tuple[datetime, str]] = {}
    for path, info in fs.find(f"{bucket}/{PREFIX}/", detail=True).items():
        if not path.endswith(".imscc"):
            continue
        key = path.removeprefix(f"{bucket}/")
        course_id = key.removeprefix(f"{PREFIX}/").split("/")[0]
        written = info["LastModified"]
        if course_id not in newest or written >= newest[course_id][0]:
            newest[course_id] = (written, key)
    return {course_id: key for course_id, (_written, key) in newest.items()}


def run_id(course_id: str, course_code: str | None) -> str:
    """Return the id of a course's one run in Learn, the scoped pull's key.

    Its ``LearningResource.readable_id`` is this without ``+canvas``.
    """
    return f"{course_id}-{course_code}+canvas"


def file_key(source_path: str, run: str) -> str:
    """Return ``ContentFile.key``, which Learn builds as an edX module id.

    Follows ``get_edx_module_id`` in mit-learn. The extension is not part of it,
    so ``a.pdf`` and ``a.docx`` in one folder are one ContentFile.
    """
    path = source_path.replace(" ", "_")
    folder = path.split("/")[-2]
    if folder == STATIC_FOLDER:
        return f"asset-v1:{run}+type@asset+block@{PurePosixPath(path).name}"
    return f"block-v1:{run}+type@{folder}+block@{PurePosixPath(path).stem}"


def derived_title(source_path: str) -> str:
    """Return the title Learn falls back to, from a file's name."""
    stem = PurePosixPath(source_path).stem
    return stem.replace("_", " ").replace("-", " ").title()


def text_from_srt(content: str) -> str:
    """Remove the timings of an SRT transcript, as Learn does."""
    content = re.sub(
        r"\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}(\n|$)", "", content
    )
    content = re.sub(r"\d+\n", "", content)
    return re.sub(r"\n\s*\n", " ", content)


def text_from_sjson(content: str) -> str | None:
    """Return the text of an SJSON transcript, or None when it is not JSON."""
    try:
        return " ".join(json.loads(content).get("text", []))
    except json.JSONDecodeError:
        logger.warning("A transcript was not valid SJSON")
        return None


def pdf_is_valid(body: bytes) -> bool:
    """Tell whether a PDF can be read, as ``pdf_is_valid`` in mit-learn.

    Learn drops a PDF that fails this, even when Tika could read it.
    """
    try:
        reader = pypdf.PdfReader(io.BytesIO(body))
        if len(reader.pages) > 0:
            reader.pages[0].extract_text()
            return True
    except Exception:  # noqa: BLE001
        # pypdf raises many things on a malformed file, and Learn takes them all
        # to mean the same.
        logger.warning("Could not read a PDF", exc_info=True)
    return False


def documents(
    archive: zipfile.ZipFile, published: dict[str, dict[str, Any]], run: str
) -> dict[str, zipfile.ZipInfo]:
    """Return the archive members Learn would extract text from, by key.

    Of members sharing a key Learn keeps whichever its walk of the tree reached
    last, which is arbitrary. The last by path is kept here.

    :param archive: The course's ``.imscc``.
    :param published: The course's ``cartridge.published_items``.
    :param run: The course's run id.
    :returns: The members, keyed by ``ContentFile.key``.
    """
    members = {}
    for info in sorted(archive.infolist(), key=lambda info: info.filename):
        path = PurePosixPath(info.filename)
        if (
            info.filename in published
            and not info.is_dir()
            and DRAFT_MARKER not in str(path.parent)
            and path.suffix.lower() in VALID_TEXT_FILE_TYPES
        ):
            members[file_key(source_path(info.filename), run)] = info
    return members


def source_path(member: str) -> str:
    """Return ``ContentFile.source_path``, which has a leading ``/`` at the root."""
    directory, _, name = member.rpartition("/")
    return f"{directory}/{name}"


def file_url(
    path: str, item: dict[str, Any], config_by_key: dict[str, dict[str, Any]]
) -> str | None:
    """Return a file's Canvas URL from the export's url config."""
    url_path = path.lstrip(path.split("/")[0])
    config_item = config_by_key.get(url_path) or config_by_key.get(
        item.get("title") or ""
    )
    return (config_item or {}).get("url")


def _read_member(
    client: tika.TikaClient, archive: zipfile.ZipFile, info: zipfile.ZipInfo
) -> tuple[Extraction, str | None]:
    """Return a member's extraction and the MD5 of its bytes."""
    if info.file_size > MAX_FILE_BYTES:
        logger.warning("%s is over the limit; not extracted", info.filename)
        return (None, STATUS_FAILED, False), None
    body = archive.read(info)
    md5 = hashlib.md5(body, usedforsecurity=False).hexdigest()
    extension = PurePosixPath(info.filename).suffix.lower()
    if extension == ".pdf" and body and not pdf_is_valid(body):
        return (None, STATUS_FAILED, False), md5
    text, status, retry = extract(
        client,
        body,
        info.filename,
        _MIMETYPES.types_map[True].get(extension),
    )
    if text is None:
        return (None, status, retry), md5
    # Learn strips what Tika returns before transforming it.
    text = text.strip()
    if extension == ".srt":
        text = text_from_srt(text)
    elif extension == ".sjson":
        text = text_from_sjson(text) or ""
    return (text or None, status if text else STATUS_EMPTY, retry), md5


def course_row(  # noqa: PLR0913
    course_id: str,
    archive_key: str,
    settings: dict[str, Any],
    context: dict[str, str | None],
    version: str,
    retrieved_at: datetime,
) -> dict[str, Any]:
    """Build the row ``run_for_canvas_archive`` makes a course and run from."""
    course_code = settings.get("course_code")
    domain, canvas_id = context.get("canvas_domain"), context.get("course_id")
    return {
        **dict.fromkeys(SCHEMA.names),
        "course_id": course_id,
        "content_kind": KIND_COURSE,
        "archive_key": archive_key,
        "run_readable_id": run_id(course_id, course_code),
        "course_code": course_code,
        "course_title": settings.get("title", f"canvas course {course_id}"),
        "course_url": f"https://{domain}/courses/{canvas_id}/",
        "course_start_at": settings.get("start_at"),
        "course_end_at": settings.get("conclude_at"),
        "course_version": version,
        "course_retrieved_at": retrieved_at,
    }


def read_course(  # noqa: PLR0913
    *,
    pool: ThreadPoolExecutor,
    client: tika.TikaClient,
    archive: zipfile.ZipFile,
    course_id: str,
    archive_key: str,
    published: dict[str, dict[str, Any]],
    config_by_key: dict[str, dict[str, Any]],
    version: str,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Read one course's published files through Tika.

    :returns: The course's rows, how many member bytes they were read from, and
        whether a file failed in a way another read might not.
    :raises RuntimeError: Tika is down.
    """
    retrieved_at = datetime.now(tz=UTC)
    settings = cartridge.parse_settings(archive)
    course = course_row(
        course_id,
        archive_key,
        settings,
        cartridge.parse_context(archive),
        version,
        retrieved_at,
    )
    run = course["run_readable_id"]
    members = documents(archive, published, run)
    results = list(
        pool.map(lambda info: _read_member(client, archive, info), members.values())
    )
    extractions = collect(client, course_id, (result[0] for result in results))

    rows = [course]
    for (key, info), (text, status, _retry), (_, md5) in zip(
        members.items(), extractions, results, strict=True
    ):
        path = source_path(info.filename)
        item = published.get(path, {})
        rows.append(
            {
                **dict.fromkeys(SCHEMA.names),
                "course_id": course_id,
                "content_kind": KIND_FILE,
                "archive_key": archive_key,
                "run_readable_id": run,
                "file_key": key,
                "source_path": path,
                "file_extension": PurePosixPath(info.filename).suffix.lower(),
                # A reused file gets its previous content_title as its title in
                # Learn, so every read after the first ends here. See the model.
                "title": item.get("title") or derived_title(path),
                "content_title": item.get("title"),
                "url": file_url(path, item, config_by_key),
                "file_md5": md5,
                "file_size_bytes": info.file_size,
                "content": text,
                "extraction_status": status,
                "course_version": version,
                "course_retrieved_at": retrieved_at,
            }
        )
    retry = any(may_pass for _text, _status, may_pass in extractions)
    return rows, sum(info.compress_size for info in members.values()), retry


def _unpublished_row(course_id: str, retrieved_at: datetime) -> dict[str, Any]:
    return {
        **dict.fromkeys(SCHEMA.names),
        "course_id": course_id,
        "content_kind": KIND_UNPUBLISHED,
        "course_retrieved_at": retrieved_at,
    }


def _open_archive(fs: s3fs.S3FileSystem, bucket: str, key: str) -> Any:  # noqa: ANN401
    return fs.open(f"{bucket}/{key}", block_size=ARCHIVE_BLOCK_BYTES)


def _load_metadata(fs: s3fs.S3FileSystem, bucket: str, key: str) -> dict[str, Any]:
    metadata_key = key.removesuffix(".imscc") + ".metadata.json"
    return json.loads(fs.cat_file(f"{bucket}/{metadata_key}"))


@dlt.source(name="canvas_content_ingest")
def canvas_content_source(
    bucket: str = BUCKET,
    budget_bytes: int = BUDGET_BYTES,
    table_format: config.TableFormat | None = None,
) -> Any:  # noqa: ANN401
    """Load the Canvas courses that changed since the last load.

    :param bucket: The landing zone.
    :param budget_bytes: How many member bytes one load may read. A course is
        never split, so a load can exceed it by one course.
    :param table_format: ``native`` or ``iceberg``; defaults to the active
        profile's.
    :raises RuntimeError: The landing zone has no Canvas exports.
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
        now = datetime.now(tz=UTC)
        fs = s3fs.S3FileSystem(use_listings_cache=False)
        archives = latest_archives(fs, bucket)
        if not archives:
            # Every course would look gone.
            msg = f"No Canvas exports under s3://{bucket}/{PREFIX}/."
            raise RuntimeError(msg)

        gone = sorted(set(versions) - set(archives))
        if gone:
            yield pa.Table.from_pylist(
                [_unpublished_row(course_id, now) for course_id in gone], schema=SCHEMA
            )
        for course_id in gone:
            del versions[course_id]

        resume_after = state.get("resume_after", "")
        live = sorted(archives)
        pending = [course_id for course_id in live if course_id > resume_after] + [
            course_id for course_id in live if course_id <= resume_after
        ]
        # Built on this thread: the workers must not touch dlt's config, whose
        # injectable context is not thread-safe (see .dlt/config.toml).
        client: tika.TikaClient | None = None
        spent = 0
        pool = ThreadPoolExecutor(max_workers=WORKERS)
        try:
            for course_id in pending:
                key = archives[course_id]
                try:
                    metadata = _load_metadata(fs, bucket, key)
                except FileNotFoundError:
                    # The metadata lands just after the archive. The next load
                    # finds it.
                    logger.warning("%s has no metadata yet; skipped", key)
                    continue
                config_by_key = cartridge.url_config(metadata)
                with (
                    _open_archive(fs, bucket, key) as handle,
                    zipfile.ZipFile(handle) as archive,
                ):
                    published = cartridge.published_items(archive, config_by_key, now)
                    version = cartridge.course_checksum(
                        archive, published, config_by_key
                    )
                    recorded = versions.get(course_id)
                    if not needs_read(recorded, version, now):
                        continue
                    client = client or tika.client_for_profile()
                    rows, read_bytes, retry = read_course(
                        pool=pool,
                        client=client,
                        archive=archive,
                        course_id=course_id,
                        archive_key=key,
                        published=published,
                        config_by_key=config_by_key,
                        version=version,
                    )
                yield pa.Table.from_pylist(rows, schema=SCHEMA)
                versions[course_id] = record_read(recorded, version, now, retry=retry)
                spent += read_bytes
                if spent >= budget_bytes:
                    state["resume_after"] = course_id
                    return
        finally:
            # Not waited for: when Tika is down, the calls in flight would each
            # hold the failure back by a timeout.
            pool.shutdown(wait=False, cancel_futures=True)
        state["resume_after"] = ""

    return course_content


canvas_content_pipeline = config.pipeline_for("canvas", pipeline_name="canvas_content")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source for the active profile."""
    return config.with_nullable_load_id(canvas_content_source())


if __name__ == "__main__":
    print(canvas_content_pipeline.run(build_source()))  # noqa: T201

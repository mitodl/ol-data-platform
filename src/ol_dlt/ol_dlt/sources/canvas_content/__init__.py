"""Canvas course content ingestion via dlt.

``data_loading.defs.canvas`` exports every course in the course ID sheet to the
landing zone every six hours, as a Common Cartridge and a metadata JSON with
the course's URLs:

    canvas/course_content/<course_id>/<version>.imscc
    canvas/course_content/<course_id>/<version>.metadata.json

Every member of each course's newest export lands, whoever can see it:

    the course's settings                   -> content_kind = course
    each file in the archive                -> content_kind = member
        + what the export records about it (``cartridge.member_facts``)
        + the text Tika extracts, for a member of a text type MIT Learn
          loads (``is_text_member``)
    -> raw__canvas__s3__course_content

Who can see a member is left to the reader: the ``files_*``, ``page_*``,
``api_*`` and ``module_items`` columns carry what Canvas recorded, with dates in
UTC, and no row is dropped for being hidden, locked, a draft or an instructor's.
Text is extracted only for what MIT Learn loads (and so serves), because the raw
and staging schemas are readable by every analyst.

MIT Learn builds its Canvas course and ContentFiles from the same exports
(``learning_resources/etl/canvas.py`` in mit-learn). What Learn would treat as
published at the time of the read is in the ``learn_*`` columns, from Learn's own
rules (``cartridge.published_items``), together with the key, title and URL
Learn would give the file. Those columns are Learn-parity data, for
``integrations__learn__canvas_content_files``, which applies Learn's remaining
rules (text types, the ``draft`` folder test, one file per key). Tutor problem
files are members like any other here (``in_tutor_folder``); Learn loads them
by another path (``load_problem_files``) and never as ContentFiles.

A course is read whole, and only when it changed. Its version is
``cartridge.course_checksum``, held in dlt state, which also moves when a file's
availability date passes. A changed course appends a full new set of rows
stamped with one ``course_retrieved_at``, and staging keeps each course's newest
set. A course whose folder leaves the landing zone appends one ``unpublished``
row, which becomes its newest set and so empties it downstream.

The PDFs differ from Learn. Learn sends a PDF of up to ``OCR_PDF_MAX_PAGE_THRESHOLD``
pages through opendataloader and an LLM (``_extract_content_with_ocr``, with
mit-learn #4126's changes), and falls back to Tika for a longer one or a failed
conversion. Here every PDF goes through Tika, including one pypdf cannot read,
which Learn drops (``pdf_is_valid``). See
``integrations__learn__canvas_content_files``.

One load covers at most ``budget_bytes`` of archive members. The caller re-runs
the source until a load reads nothing (``build_batched_assets`` in the
data_loading code location), and the sweep resumes after the last course a
load finished. A course that cannot be parsed is logged and skipped, so it does
not hold back the courses after it, and is tried again by the next load.

Tika credentials: see ``ol_dlt.tika``.

Run standalone, as a sweep (``__main__``):
    DLT_PROFILE=dev TIKA_ACCESS_TOKEN=... python -m ol_dlt.sources.canvas_content
"""

import hashlib
import io
import json
import logging
import mimetypes
import zipfile
import zlib
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
    STATUS_UNREADABLE,
    Extraction,
    TikaUnavailableError,
    collect,
    extract,
    needs_read,
    record_read,
)
from ol_dlt.sources.canvas_content import cartridge
from ol_file_types.srt import text_from_srt

logger = logging.getLogger(__name__)

# Where Learn reads Canvas exports in QA and production alike
# (CANVAS_COURSE_BUCKET_NAME in ol-infrastructure).
BUCKET = "ol-data-lake-landing-zone-production"
PREFIX = "canvas/course_content"
RAW_TABLE = "raw__canvas__s3__course_content"

# The types a member's text is extracted from: VALID_TEXT_FILE_TYPES in
# mit-learn's learning_resources/constants.py, which is also the list the Learn
# view keeps.
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
KIND_MEMBER = "member"
KIND_UNPUBLISHED = "unpublished"

# A member whose text is not extracted: not a text type, or not one MIT Learn
# loads (see is_text_member).
STATUS_NOT_READ = "not_read"

_TIMESTAMP = pa.timestamp("us", tz="UTC")

SCHEMA = pa.schema(
    [
        pa.field("course_id", pa.string()),
        pa.field("content_kind", pa.string()),
        pa.field("archive_key", pa.string()),
        pa.field("parse_rule", pa.int64()),
        # A course row's.
        pa.field("course_code", pa.string()),
        pa.field("course_title", pa.string()),
        pa.field("canvas_domain", pa.string()),
        pa.field("canvas_course_id", pa.string()),
        pa.field("course_start_at", pa.string()),
        pa.field("course_end_at", pa.string()),
        pa.field("files_tab_hidden", pa.bool_()),
        pa.field("public_syllabus", pa.string()),
        pa.field("public_syllabus_to_auth", pa.string()),
        # A member row's.
        pa.field("member_path", pa.string()),
        pa.field("file_extension", pa.string()),
        pa.field("file_md5", pa.string()),
        pa.field("file_size_bytes", pa.int64()),
        pa.field("content", pa.string()),
        pa.field("extraction_status", pa.string()),
        pa.field("pdf_is_valid", pa.bool_()),
        pa.field("in_tutor_folder", pa.bool_()),
        pa.field("draft_in_parent_path", pa.bool_()),
        pa.field("folder_restricted", pa.bool_()),
        pa.field("files_display_name", pa.string()),
        pa.field("files_hidden", pa.bool_()),
        pa.field("files_locked", pa.bool_()),
        pa.field("files_visibility", pa.string()),
        pa.field("files_lock_at", _TIMESTAMP),
        pa.field("files_unlock_at", _TIMESTAMP),
        pa.field("module_items", pa.string()),
        pa.field("page_title", pa.string()),
        pa.field("page_workflow_state", pa.string()),
        pa.field("page_is_assignment", pa.bool_()),
        pa.field("page_intended_role", pa.string()),
        pa.field("page_is_syllabus", pa.bool_()),
        pa.field("page_lock_at", _TIMESTAMP),
        pa.field("page_unlock_at", _TIMESTAMP),
        pa.field("page_only_visible_to_overrides", pa.bool_()),
        pa.field("page_hide_in_gradebook", pa.bool_()),
        pa.field("page_module_locked", pa.bool_()),
        pa.field("linked_from_pages", pa.string()),
        pa.field("api_published", pa.bool_()),
        pa.field("api_hidden", pa.bool_()),
        pa.field("api_locked", pa.bool_()),
        pa.field("api_lock_at", _TIMESTAMP),
        pa.field("api_unlock_at", _TIMESTAMP),
        pa.field("api_folder_hidden", pa.bool_()),
        pa.field("api_folder_locked", pa.bool_()),
        pa.field("api_url", pa.string()),
        # Learn-parity: what MIT Learn's rules make of the member at the read.
        pa.field("learn_published", pa.bool_()),
        pa.field("learn_key", pa.string()),
        pa.field("learn_title", pa.string()),
        pa.field("learn_content_title", pa.string()),
        pa.field("learn_url", pa.string()),
        pa.field("course_version", pa.string()),
        pa.field("course_retrieved_at", _TIMESTAMP),
    ]
)

# The facts cartridge.member_facts returns as lists, landed as JSON text.
_JSON_FACTS = ("module_items", "linked_from_pages")

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


def text_from_sjson(content: str) -> str | None:
    """Return the text of an SJSON transcript, or None when it is not JSON."""
    try:
        return " ".join(json.loads(content).get("text", []))
    except (json.JSONDecodeError, AttributeError, TypeError):
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


def is_text_member(info: zipfile.ZipInfo, published: dict[str, dict[str, Any]]) -> bool:
    """Tell whether a member's text is extracted.

    Only a member of a text type that MIT Learn loads (``published``) is read:
    Learn serves that text already. The text of the rest (instructor-only pages,
    hidden files, tutor solutions) would be readable by everyone with access to
    the raw and staging schemas, so their rows carry facts and no text. Learn's
    rule is not quite "what students can reach": a hidden file linked from a
    page that is locked, or not yet unlocked, is in ``published``.
    """
    return (
        PurePosixPath(info.filename).suffix.lower() in VALID_TEXT_FILE_TYPES
        and info.filename in published
    )


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
    client: tika.TikaClient,
    archive: zipfile.ZipFile,
    info: zipfile.ZipInfo,
    md5s: dict[str, str],
    valid_pdfs: dict[str, bool],
) -> Extraction:
    """Return a member's extraction.

    Notes the MD5 of its bytes in ``md5s`` and, for a PDF, whether pypdf can
    read it in ``valid_pdfs``. Tika reads a PDF either way.
    """
    if info.file_size > MAX_FILE_BYTES:
        logger.warning("%s is over the limit; not extracted", info.filename)
        return None, STATUS_UNREADABLE, False
    try:
        body = archive.read(info)
    # zipfile raises RuntimeError for an encrypted member, and its subclass
    # NotImplementedError for a compression method it does not have.
    except (zipfile.BadZipFile, zlib.error, EOFError, RuntimeError):
        logger.exception("Could not read %s from its archive", info.filename)
        return None, STATUS_UNREADABLE, False
    md5s[info.filename] = hashlib.md5(body, usedforsecurity=False).hexdigest()
    extension = PurePosixPath(info.filename).suffix.lower()
    if extension == ".pdf":
        valid_pdfs[info.filename] = pdf_is_valid(body)
    text, status, retry = extract(
        client,
        body,
        info.filename,
        _MIMETYPES.types_map[True].get(extension),
    )
    if text is None:
        return None, status, retry
    # Learn strips what Tika returns before transforming it.
    text = text.strip()
    if extension == ".srt":
        text = text_from_srt(text)
    elif extension == ".sjson":
        text = text_from_sjson(text) or ""
    return text or None, status if text else STATUS_EMPTY, retry


def course_row(  # noqa: PLR0913
    course_id: str,
    archive_key: str,
    settings: dict[str, Any],
    context: dict[str, str | None],
    version: str,
    retrieved_at: datetime,
) -> dict[str, Any]:
    """Build the course's row from its settings and context."""
    return {
        **dict.fromkeys(SCHEMA.names),
        "course_id": course_id,
        "content_kind": KIND_COURSE,
        "archive_key": archive_key,
        "parse_rule": cartridge.PARSE_RULE,
        **dict(zip(_COURSE_FIELDS, course_fields(settings, context), strict=True)),
        "course_version": version,
        "course_retrieved_at": retrieved_at,
    }


def member_row(  # noqa: PLR0913
    info: zipfile.ZipInfo,
    facts: dict[str, Any],
    extraction: Extraction | None,
    *,
    md5: str | None,
    pdf_valid: bool | None,
    published: dict[str, dict[str, Any]],
    config_by_key: dict[str, dict[str, Any]],
    run: str,
) -> dict[str, Any]:
    """Build a member's row, apart from the course's own columns.

    :param extraction: How the member's text was read; None when it was not.
    :param run: The course's run id in Learn, for ``learn_key``.
    """
    text, status = (None, STATUS_NOT_READ) if extraction is None else extraction[:2]
    path = source_path(info.filename)
    parent = PurePosixPath(info.filename).parent
    row: dict[str, Any] = {
        "member_path": info.filename,
        "file_extension": PurePosixPath(info.filename).suffix.lower(),
        "file_md5": md5,
        "file_size_bytes": info.file_size,
        "content": text,
        "extraction_status": status,
        "pdf_is_valid": pdf_valid,
        "in_tutor_folder": info.filename.startswith(cartridge.TUTOR_FOLDER),
        "draft_in_parent_path": DRAFT_MARKER in str(parent),
        **{
            key: json.dumps(value) if key in _JSON_FACTS else value
            for key, value in facts.items()
        },
        "learn_published": info.filename in published,
    }
    if row["learn_published"]:
        item = published.get(path, {})
        row |= {
            "learn_key": file_key(path, run),
            # A reused file gets its previous content_title as its title in
            # Learn, so every read after the first ends here. See the model.
            "learn_title": item.get("title") or derived_title(path),
            "learn_content_title": item.get("title"),
            "learn_url": file_url(path, item, config_by_key),
        }
    return row


def read_course(  # noqa: PLR0913
    *,
    pool: ThreadPoolExecutor,
    client: tika.TikaClient,
    archive: zipfile.ZipFile,
    course_id: str,
    archive_key: str,
    published: dict[str, dict[str, Any]],
    config_by_key: dict[str, dict[str, Any]],
    facts: dict[str, dict[str, Any]],
    version: str,
) -> tuple[list[dict[str, Any]], int, bool]:
    """Read one course: a row for it and for each member, with text from Tika.

    :param published: The course's ``cartridge.published_items``.
    :param facts: The course's ``cartridge.member_facts``.
    :returns: The course's rows, how many member bytes they were read from, and
        whether a file failed in a way another read might not.
    :raises TikaUnavailableError: Tika is down.
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
    members = sorted(
        (info for info in archive.infolist() if not info.is_dir()),
        key=lambda info: info.filename,
    )
    infos = [info for info in members if is_text_member(info, published)]
    md5s: dict[str, str] = {}
    valid_pdfs: dict[str, bool] = {}
    extractions = collect(
        client,
        course_id,
        pool.map(
            lambda info: _read_member(client, archive, info, md5s, valid_pdfs), infos
        ),
    )
    by_member = dict(zip(infos, extractions, strict=True))

    run = run_id(course_id, settings.get("course_code"))
    rows = [course]
    for info in members:
        rows.append(
            {
                **dict.fromkeys(SCHEMA.names),
                "course_id": course_id,
                "content_kind": KIND_MEMBER,
                "archive_key": archive_key,
                "parse_rule": cartridge.PARSE_RULE,
                **member_row(
                    info,
                    facts.get(info.filename, {}),
                    by_member.get(info),
                    md5=md5s.get(info.filename),
                    pdf_valid=valid_pdfs.get(info.filename),
                    published=published,
                    config_by_key=config_by_key,
                    run=run,
                ),
                "course_version": version,
                "course_retrieved_at": retrieved_at,
            }
        )
    retry = any(may_pass for _text, _status, may_pass in extractions)
    return rows, sum(info.compress_size for info in infos), retry


def _unpublished_row(course_id: str, retrieved_at: datetime) -> dict[str, Any]:
    return {
        **dict.fromkeys(SCHEMA.names),
        "course_id": course_id,
        "content_kind": KIND_UNPUBLISHED,
        "course_retrieved_at": retrieved_at,
    }


# The course row's own columns, in the order course_fields returns them.
_COURSE_FIELDS = (
    "course_title",
    "course_code",
    "course_start_at",
    "course_end_at",
    "canvas_domain",
    "canvas_course_id",
    "files_tab_hidden",
    "public_syllabus",
    "public_syllabus_to_auth",
)


def course_fields(
    settings: dict[str, Any], context: dict[str, str | None]
) -> list[Any]:
    """Return the course row's own columns, which its version also digests."""
    tabs = settings.get("tab_configuration") or {}
    return [
        settings.get("title"),
        settings.get("course_code"),
        settings.get("start_at"),
        settings.get("conclude_at"),
        context.get("canvas_domain"),
        context.get("course_id"),
        bool(tabs.get(cartridge.FILES_TAB_ID, {}).get("hidden", False)),
        settings.get("public_syllabus"),
        settings.get("public_syllabus_to_auth"),
    ]


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
    :raises RuntimeError: The landing zone has no Canvas exports, none of them
        could be parsed, or Tika is down.
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
        client = tika.client_for_profile()
        spent = 0
        unreadable = 0
        pool = ThreadPoolExecutor(max_workers=WORKERS)
        try:
            for course_id in pending:
                key = archives[course_id]
                try:
                    metadata = _load_metadata(fs, bucket, key)
                    config_by_key = cartridge.url_config(metadata)
                    with (
                        _open_archive(fs, bucket, key) as handle,
                        zipfile.ZipFile(handle) as archive,
                    ):
                        published = cartridge.published_items(
                            archive, config_by_key, now
                        )
                        facts = cartridge.member_facts(archive, config_by_key)
                        version = cartridge.course_checksum(
                            archive,
                            published,
                            config_by_key,
                            course_fields(
                                cartridge.parse_settings(archive),
                                cartridge.parse_context(archive),
                            ),
                            facts,
                        )
                        recorded = versions.get(course_id)
                        if not needs_read(recorded, version, now):
                            continue
                        rows, read_bytes, retry = read_course(
                            pool=pool,
                            client=client,
                            archive=archive,
                            course_id=course_id,
                            archive_key=key,
                            published=published,
                            config_by_key=config_by_key,
                            facts=facts,
                            version=version,
                        )
                except FileNotFoundError:
                    # The metadata lands just after the archive. The next load
                    # finds it.
                    logger.warning("%s has no metadata yet; skipped", key)
                    continue
                except TikaUnavailableError:
                    raise
                except Exception:
                    # A failed extract throws away every course this load read,
                    # so one bad export would stop every load at the same place.
                    logger.exception("Could not parse %s; skipped", key)
                    unreadable += 1
                    continue
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
        if unreadable == len(pending):
            # Not one bad export: the landing zone or this code is the problem.
            msg = f"None of the {unreadable} Canvas exports could be parsed."
            raise RuntimeError(msg)
        state["resume_after"] = ""

    return course_content


canvas_content_pipeline = config.pipeline_for("canvas", pipeline_name="canvas_content")


def build_source() -> Any:  # noqa: ANN401
    """Instantiate the source for the active profile."""
    return config.with_nullable_load_id(canvas_content_source())

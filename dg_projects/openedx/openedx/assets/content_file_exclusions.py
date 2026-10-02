"""Which files of an Open edX course export MIT Learn leaves out of its ContentFiles.

MIT Learn skips files the course itself does not use: blocks under a
visible_to_staff_only subtree (with their html bodies and video transcripts),
the asset manifests and the announcement archive, and anything under static/
that nothing in the course refers to (hq#13350). integrations__learn__content_files
has to skip the same files to match it, and the rules need the whole OLX tree
(the course.xml walk, every block's text), which the warehouse does not hold in
a form SQL can walk. So this asset runs Learn's rules over the export and lands
one row per file in it, flagged excluded or not, for dbt to filter on.

The functions below are a port of learning_resources/etl/utils.py on mit-learn
main as of 2026-10-02 (excluded_olx_paths and its helpers). They are kept as
close to the original as the setting allows, so a change there can be diffed
across; the differences are noted where they occur.

Data flow:
    openedx/raw_data/course_xml                        (tar.gz, per course run)
        -> extract_course_file_exclusions              (this asset)
            -> openedx/processed_data/course_file_exclusions  (JSONL in S3)
                -> int__openedx__content_files         (dbt)
"""

import hashlib
import html
import json
import logging
import re
import tarfile
import xml.etree.ElementTree as ET
from bisect import bisect_left
from itertools import accumulate
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any
from urllib.parse import unquote

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

STATIC_DIR = "static"


def _hidden_block_files(root: Path, tag: str, url_name: str, element) -> set[Path]:
    """Files belonging to one hidden block: its own files, html body, transcripts"""
    files = set(root.glob(f"{tag}/{url_name}.*"))
    if tag == "html" and element.get("filename"):
        files.update(root.glob(f"html/{element.get('filename')}.*"))
    if tag == "video":
        files.update(
            root / "static" / transcript.get("src")
            for transcript in element.iter("transcript")
            if transcript.get("src")
        )
    return files


def _parse_olx_block(root: Path, tag: str, url_name: str):
    """
    Parse <tag>/<url_name>.xml, returning None if missing. Malformed XML raises
    so a course is never ingested with unverified staff-only status.
    """
    try:
        return ET.parse(root / tag / f"{url_name}.xml").getroot()  # noqa: S314
    except (FileNotFoundError, NotADirectoryError):
        return None


def staff_only_olx_paths(olx_path: str | Path) -> set[Path]:
    """
    Return the files under visible_to_staff_only="true" subtrees of an OLX
    course tree, including transcripts of hidden videos. Empty when olx_path
    is not an OLX export (no course.xml). Blocks may be pointers to
    <tag>/<url_name>.xml or hold their children inline; both are walked.
    """
    root = Path(olx_path)
    course = _parse_olx_block(root, "", "course")
    if course is None:
        return set()
    hidden: dict[tuple[str, str], set[Path]] = {}
    visible: set[tuple[str, str]] = set()
    seen: set[tuple[str, str, bool]] = set()
    stack = [(course, "course", course.get("url_name"), False)]
    while stack:
        pointer, tag, url_name, staff_only = stack.pop()
        # pointer file wins when present; otherwise the element is the block itself
        element = _parse_olx_block(root, tag, url_name) if url_name else None
        if element is None:
            element = pointer
        staff_only = staff_only or "true" in (
            pointer.get("visible_to_staff_only"),
            element.get("visible_to_staff_only"),
        )
        if url_name:
            if (tag, url_name, staff_only) in seen:
                continue
            seen.add((tag, url_name, staff_only))
            # a block can hang under two parents; seeing it anywhere a learner
            # can reach makes it visible, whichever path the walk took first
            if staff_only:
                hidden[tag, url_name] = _hidden_block_files(
                    root, tag, url_name, element
                )
            else:
                visible.add((tag, url_name))
        stack.extend(
            (child, child.tag, child.get("url_name"), staff_only) for child in element
        )
    return {
        path
        for block, files in hidden.items()
        if block not in visible
        for path in files
    }


REFERENCE_SCAN_EXTENSIONS = frozenset({".xml", ".html", ".htm", ".json", ".txt", ".md"})

# Asset manifests list every file in the export and updates.items.json is mostly
# an archive of deleted announcements. None of them describes current course
# content, and treating them as references keeps every stale asset alive.
NON_CONTENT_OLX_FILES = (
    "policies/assets.json",
    "assets/assets.xml",
    "info/updates.items.json",
)

# A legacy transcript is named for its video's id rather than for anything the
# course text contains, so the id is the only link back to the block using it.
VIDEO_ID_ATTRIBUTES = ("sub", "youtube", "youtube_id_1_0")
LEGACY_TRANSCRIPT_RE = re.compile(
    r"^(?:[a-z]{2}(?:[-_][a-z]{2})?_)?subs_(.+)\.srt\.sjson$", re.IGNORECASE
)


# A reference spells an asset name with the punctuation edX rewrote into it, or
# with none of it: a file stored as "my file.pdf" is linked as my_file.pdf and as
# my%20file.pdf, and a file re-uploaded under a flattened asset key is linked
# with the + and @ of that key written as underscores. So the punctuation cannot
# be part of the comparison, but the places it sat are still where a name starts.
NAME_BREAK = "\x00"
SEPARATORS = re.compile(r"(?:[^\w.\-]|_)+")


def normalize_asset_ref(text: str) -> str:
    """Strip the punctuation of a filename that a reference may spell differently"""
    return SEPARATORS.sub("", unquote(html.unescape(text)).lower())


def _reference_index(texts: list[str]) -> tuple[str, list[int]]:
    """
    Normalize the course text the same way, and return it with the offsets where
    a name can start, i.e. everywhere the text had punctuation but an underscore.
    Underscores do not break a name because that is what edX writes a space or a
    +/@ as, so they are the one thing both sides drop.
    """
    stripped = NAME_BREAK.join(
        SEPARATORS.sub(
            NAME_BREAK, unquote(html.unescape(text)).lower().replace("_", "")
        )
        for text in texts
    )
    segments = stripped.split(NAME_BREAK)
    return "".join(segments), list(accumulate(map(len, segments), initial=0))


def _name_starts_at(blob: str, starts: list[int], name: str) -> bool:
    """
    Whether the course text spells a filename where a name can start, rather than
    only inside a longer one. Without this a reference to final_exam.srt reads as
    a reference to exam.srt too, and pulls that file back out of the staff-only
    set.
    """
    start = blob.find(name)
    while start != -1:
        index = bisect_left(starts, start)
        if index < len(starts) and starts[index] == start:
            return True
        start = blob.find(name, start + 1)
    return False


def _olx_reference_sources(root: Path, skip: set[Path]) -> list[Path]:
    """Files whose text may legitimately refer to a static asset"""
    sources = []
    for path in root.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in REFERENCE_SCAN_EXTENSIONS:
            continue
        relative = path.relative_to(root)
        if (
            relative.parts[0] == "static"
            or relative.as_posix() in NON_CONTENT_OLX_FILES
            or path in skip
            or any("draft" in part for part in relative.parts[:-1])
        ):
            continue
        sources.append(path)
    return sources


def _live_course_updates(root: Path) -> str:
    """
    Text of the announcements the course team has not deleted. The whole file is
    excluded as a reference source because edX keeps deleted announcements in
    the export, but a live one still counts.
    """
    try:
        items = json.loads(
            (root / "info/updates.items.json").read_text(errors="ignore")
        )
    except (OSError, ValueError):
        return ""
    return "\n".join(
        item.get("content") or ""
        for item in items
        if isinstance(item, dict) and item.get("status") != "deleted"
    )


def _olx_video_ids(sources: list[Path]) -> set[str] | None:
    """
    Video ids declared anywhere in the course, or None if any source could not
    be parsed. None means "ids unknown", and callers keep every legacy
    transcript rather than drop one whose video they failed to read.
    """
    ids = set()
    for path in sources:
        if path.suffix.lower() != ".xml":
            continue
        try:
            element = ET.parse(path).getroot()  # noqa: S314
        except ET.ParseError:
            log.warning("Malformed XML in %s, keeping all legacy transcripts", path)
            return None
        # iter() finds <video> whether it has its own file or sits inline
        for video in element.iter("video"):
            for attribute in VIDEO_ID_ATTRIBUTES:
                # youtube is a comma-separated "<speed>:<id>" list, the others
                # are bare ids
                for entry in (video.get(attribute) or "").split(","):
                    value = entry.split(":")[-1].strip()
                    if value:
                        ids.add(normalize_asset_ref(value))
    return ids


def static_olx_references(root: Path, skip: set[Path]) -> tuple[set[Path], set[Path]]:
    """
    Split the files under static/ into the ones the course refers to and the
    ones nothing in it does. Matching is on the filename, as a substring that
    has to begin a name; see _reference_index for how the two are normalized.

    :param root: the root of the OLX tree
    :param skip: files that must not count as references, i.e. the staff-only
        set, so an asset only an answer key mentions is unreferenced
    :returns: referenced and unreferenced static files
    :rtype: tuple[set[Path], set[Path]]
    """
    static_dir = root / "static"
    if not static_dir.is_dir():
        return set(), set()
    sources = _olx_reference_sources(root, skip)
    texts = [path.read_text(errors="ignore") for path in sources]
    texts.append(_live_course_updates(root))
    blob, starts = _reference_index(texts)
    video_ids = _olx_video_ids(sources)

    referenced, unreferenced = set(), set()
    for path in sorted(static_dir.rglob("*")):
        if not path.is_file():
            continue
        if _name_starts_at(blob, starts, normalize_asset_ref(path.name)) or (
            (legacy := LEGACY_TRANSCRIPT_RE.match(path.name))
            and (video_ids is None or normalize_asset_ref(legacy.group(1)) in video_ids)
        ):
            referenced.add(path)
        else:
            unreferenced.add(path)
    return referenced, unreferenced


def excluded_olx_paths(olx_path: str | Path) -> dict[Path, str]:
    """
    Files an OLX export contains that the course itself does not use, each with
    the rule that excluded it: staff-only subtrees, the asset manifests and
    announcement archive, and anything under static/ that nothing refers to.

    Learn returns a set; this returns the same keys with a reason attached, so
    a parity difference can be traced to the rule that produced it.

    :param olx_path: The path to the directory with the OLX data
    :returns: files that should not be ingested, mapped to why
    :rtype: dict[Path, str]
    """
    root = Path(olx_path)
    excluded = dict.fromkeys(staff_only_olx_paths(root), "staff_only")
    if not (root / "course.xml").is_file():
        return excluded
    for name in NON_CONTENT_OLX_FILES:
        if (root / name).is_file():
            excluded[root / name] = "non_content"
    referenced, unreferenced = static_olx_references(root, set(excluded))
    for path in unreferenced:
        excluded.setdefault(path, "unreferenced_static")
    # A hidden video's transcripts are in the staff-only set, but the same file is
    # often also the transcript of the visible copy of that video, so put back
    # anything a visible block still links.
    for path in referenced:
        excluded.pop(path, None)
    return excluded


def _empty_static_files(member: tarfile.TarInfo, path: str) -> tarfile.TarInfo:
    """Apply the "data" extraction filter, emptying every file under static/."""
    member = tarfile.data_filter(member, path)
    parts = Path(member.name).parts
    if member.isfile() and len(parts) > 2 and parts[1] == STATIC_DIR:  # noqa: PLR2004
        # replace() copies but cannot set size, so the copy's is set after.
        member = member.replace(deep=False)
        member.size = 0
    return member


def unpack_olx_tree(archive_path: Path, destination: Path) -> Path:
    """Unpack a course export for the exclusion rules, leaving static/ files empty.

    The rules read every file outside static/ but only the names of the files
    in it, and static/ is ~99% of an export (up to 1286 MiB for one QA course).
    Each static file is written empty, so its path exists for the globs and the
    walk without its bytes ever reaching disk.

    :param archive_path: the course export tarball
    :param destination: an empty directory to unpack into
    :returns: the OLX root, the archive's one top-level directory, which is what
        Learn hands excluded_olx_paths
    :rtype: Path
    """
    with tarfile.open(archive_path, "r") as archive:
        # _empty_static_files applies tarfile.data_filter first.
        archive.extractall(destination, filter=_empty_static_files)  # noqa: S202
    (olx_root,) = destination.iterdir()
    return olx_root


def build_file_rows(
    olx_root: Path, *, course_id: str, source_system: str, course_xml_version: str
) -> list[dict[str, Any]]:
    """One row per file in the export, flagged with whether Learn excludes it.

    Every file gets a row, not only the excluded ones, so that a course whose
    rules excluded nothing still lands a file, and dbt can tell "checked, keep
    everything" apart from "not checked yet".

    Paths are relative to the OLX root (static/handout.pdf, html/intro.xml), as
    the document and transcript text rows are. course_xml_version is the
    export's SHA-256, which also names the course_xml_blocks file parsed from
    the same export, so dbt can tell whether a course's blocks were checked.
    """
    excluded = excluded_olx_paths(olx_root)
    return [
        {
            "course_id": course_id,
            "source_system": source_system,
            "course_xml_version": course_xml_version,
            "file_path": path.relative_to(olx_root).as_posix(),
            "excluded": path in excluded,
            "exclusion_reason": excluded.get(path),
        }
        for path in sorted(olx_root.rglob("*"))
        if path.is_file()
    ]


@asset(
    key=AssetKey(("openedx", "processed_data", "course_file_exclusions")),
    group_name="openedx",
    ins={"course_xml": AssetIn(key=AssetKey(("openedx", "raw_data", "course_xml")))},
    io_manager_key="s3file_io_manager",
    automation_condition=upstream_or_code_changes(),
    required_resource_keys={"openedx"},
    # Each run downloads a whole course export, and a new asset or a code change
    # asks for every partition at once. As with openedx_course_export, naming the
    # pool only makes a limit settable (Deployment -> Concurrency).
    pool="openedx_file_exclusions",
    description=(
        "Every file in a course export, flagged with whether MIT Learn leaves it "
        "out of its ContentFiles (staff-only, asset manifests, unreferenced "
        "static files). One JSONL row per file."
    ),
)
def extract_course_file_exclusions(context: AssetExecutionContext, course_xml: UPath):
    """Run MIT Learn's ContentFile exclusion rules over the course export."""
    source_system = context.resources.openedx.deployment
    course_id = context.partition_key

    output_file = Path(
        NamedTemporaryFile(delete=False, suffix="_file_exclusions.jsonl").name
    )
    try:
        with TemporaryDirectory() as workdir:
            archive_path = Path(workdir, "course.tar.gz")
            course_xml.fs.get_file(str(course_xml), str(archive_path))
            # Hashed as extract_courserun_details hashes it, which names the
            # course_xml_blocks file after it.
            with archive_path.open("rb") as handle:
                course_xml_version = hashlib.file_digest(handle, "sha256").hexdigest()
            tree_dir = Path(workdir, "tree")
            tree_dir.mkdir()
            rows = build_file_rows(
                unpack_olx_tree(archive_path, tree_dir),
                course_id=course_id,
                source_system=source_system,
                course_xml_version=course_xml_version,
            )

        with jsonlines.open(output_file, "w") as writer:
            writer.write_all(rows)
        with output_file.open("rb") as handle:
            data_version = hashlib.file_digest(handle, "sha256").hexdigest()
        object_key = (
            f"{'/'.join(context.asset_key.path)}/{source_system}/"
            f"{course_id}/{data_version}.jsonl"
        )
        excluded = [row for row in rows if row["excluded"]]
        context.log.info(
            "%s: %d of %d files excluded", course_id, len(excluded), len(rows)
        )
        yield Output(
            (output_file, object_key),
            data_version=DataVersion(data_version),
            metadata={
                "course_id": course_id,
                "object_key": object_key,
                "course_xml_version": course_xml_version,
                "file_count": len(rows),
                "excluded_count": len(excluded),
                "excluded_by_reason": json.dumps(
                    {
                        reason: sum(
                            row["exclusion_reason"] == reason for row in excluded
                        )
                        for reason in sorted(
                            {row["exclusion_reason"] for row in excluded}
                        )
                    }
                ),
            },
        )
    finally:
        output_file.unlink(missing_ok=True)

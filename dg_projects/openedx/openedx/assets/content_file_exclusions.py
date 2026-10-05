"""Which files of an Open edX course export MIT Learn leaves out of its ContentFiles.

MIT Learn skips files no learner of the course can reach: blocks under a
visible_to_staff_only subtree (with their html bodies and video transcripts),
tab pages outside the navigation, about pages the platform does not show, the
course settings, the asset manifests and announcements, and anything under
static/ that nothing learners see refers to (hq#13350, mit-learn#4014).
integrations__learn__content_files has to skip the same files to match it, and
the rules need the whole OLX tree (the course.xml walk, every block's text),
which the warehouse does not hold in a form SQL can walk. So this asset runs
Learn's rules over the export and lands one row per file in it, flagged
excluded or not, for dbt to filter on.

The functions below are a port of learning_resources/etl/utils.py on mit-learn
main as of 2026-10-02, after mit-learn#4014 (excluded_olx_paths and its
helpers). They are kept as close to the original as the setting allows, so a
change there can be diffed across; the differences are noted where they occur.

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
from bisect import bisect_left
from collections.abc import Iterable
from itertools import accumulate
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import Any
from urllib.parse import unquote

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    DataVersion,
    Output,
    asset,
)
from defusedxml import ElementTree
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from upath import UPath

from openedx.assets.content_files import write_text_snapshot

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
        return ElementTree.parse(root / tag / f"{url_name}.xml").getroot()
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
            element = ElementTree.parse(path).getroot()
        except ElementTree.ParseError:
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


def unreachable_static_tabs(root: Path) -> set[Path]:
    """
    Tab pages no navigation leads a learner to. Studio exports every static tab
    it stores, but the LMS serves only the ones in the course's tab list, and
    leaves staff-only and hidden ones out of the navigation. Empty when the tab
    list cannot be read, so a tab is never dropped on a guess.
    """
    course = _parse_olx_block(root, "", "course")
    url_name = course.get("url_name") if course is not None else None
    try:
        # A missing url_name raises TypeError here, caught below, as in Learn.
        policy = json.loads(
            (root / "policies" / url_name / "policy.json").read_text(  # type: ignore[operator]
                errors="ignore"
            )
        )
        tabs = policy[f"course/{url_name}"]["tabs"]
    except (TypeError, OSError, ValueError, KeyError):
        return set()
    if not isinstance(tabs, list):
        return set()
    reachable = {
        tab.get("url_slug")
        for tab in tabs
        if isinstance(tab, dict)
        and not (tab.get("course_staff_only") or tab.get("is_hidden"))
    }
    # recursive because edX also reads tab pages from a tabs/<url_name>/ folder
    return {path for path in root.glob("tabs/**/*") if path.stem not in reachable}


# The about page is the only place about/ files show, and only Open Learning
# Library serves it; the other platforms redirect it to the course home. Of what
# it shows, effort and end date are single values rather than prose (effort is
# already on the run as time_commitment), so only the prose is content.
ABOUT_PAGE_FILES = frozenset({"overview.html", "short_description.html"})
# ETLSource.oll in mit-learn's learning_resources/etl/constants.py
OLL_ETL_SOURCE = "oll"


def _exclude(excluded: dict[Path, str], paths: Iterable[Path], reason: str) -> None:
    """Learn's excluded.update(paths), keeping the first rule to name a path."""
    for path in paths:
        excluded.setdefault(path, reason)


def excluded_olx_paths(
    olx_path: str | Path, etl_source: str | None = None
) -> dict[Path, str]:
    """
    Files an OLX export contains that no learner of the course can reach, each
    with the rule that excluded it: staff-only subtrees, tab pages outside the
    navigation, about pages the platform does not show, the course settings,
    the asset manifests, announcements, and anything under static/ that nothing
    learners see refers to.

    Learn returns a set; this returns the same keys with a reason attached, so
    a parity difference can be traced to the rule that produced it. Where Learn
    adds a path twice, the first rule to name it is the reason recorded.

    :param olx_path: The path to the directory with the OLX data
    :param etl_source: The Learn ETL source the archive is from, which decides
        whether the about page is shown. None means one that does not.
    :returns: files that should not be ingested, mapped to why
    :rtype: dict[Path, str]
    """
    root = Path(olx_path)
    excluded = dict.fromkeys(staff_only_olx_paths(root), "staff_only")
    if not (root / "course.xml").is_file():
        return excluded
    _exclude(
        excluded,
        (root / name for name in NON_CONTENT_OLX_FILES if (root / name).is_file()),
        "non_content",
    )
    _exclude(excluded, unreachable_static_tabs(root), "unreachable_tab")
    shown = ABOUT_PAGE_FILES if etl_source == OLL_ETL_SOURCE else ()
    # recursive for the about/<url_name>/ folder edX also reads
    _exclude(
        excluded,
        (path for path in root.glob("about/**/*") if path.name not in shown),
        "about_page",
    )
    referenced, unreferenced = static_olx_references(root, set(excluded))
    _exclude(excluded, unreferenced, "unreferenced_static")
    # A hidden video's transcripts are in the staff-only set, but the same file is
    # often also the transcript of the visible copy of that video, so put back
    # anything a visible block still links.
    for path in referenced:
        excluded.pop(path, None)
    # Settings rather than content, but what they name (textbooks, the course
    # image) is shown, so they were still read as references above
    _exclude(excluded, root.glob("policies/**/*"), "course_settings")
    # Old-style announcements: Studio empties updates.html whenever an
    # announcement is saved, so what is left is legacy announcements or Studio's
    # sample text. Like the live ones, they still counted as references above.
    _exclude(excluded, root.glob("info/**/updates.html"), "legacy_announcements")
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
    # Learn's etl_source for an Open edX run is the deployment name.
    excluded = excluded_olx_paths(olx_root, source_system)
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

        data_version = write_text_snapshot(rows, output_file)
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

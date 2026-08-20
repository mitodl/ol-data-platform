"""Video transcript text extraction from Open edX course archives.

The third layer of Cohort 4, and deliberately separate from Tika document
extraction: transcripts are parsed in-process with no external service, so
pairing them with Tika would let a Tika outage stop transcript extraction too.

Data flow:
    openedx/processed_data/course_static_assets     (tar.gz, per course run)
        -> extract_course_transcript_text           (this asset)
            -> openedx/processed_data/course_transcript_text  (JSONL in S3)
                -> integrations__learn__content_files         (dbt)

The SRT and SJSON parsers below reproduce mit-learn's
``learning_resources/etl/utils.py:text_from_srt_content`` and
``text_from_sjson_content`` exactly, including their quirks. Their test table is
ported verbatim into this project's tests so the parity is enforced rather than
asserted -- a cutover diff should show a difference in the data, never in how
the two sides parse it.

The WebVTT parser is the one deliberate exception. MIT Learn has no VTT
transformer, so it stores whatever Tika returns for a `.vtt`: the `WEBVTT`
header, every cue timing, and the speaker tags. That is not text anyone wants
indexed, and no consumer reads the timings, so `.vtt` is routed here and parsed
properly rather than published as document text. See `text_from_vtt_content` for
the evidence behind that call.
"""

import json
import logging
import re
from collections.abc import Callable, Iterable
from html import unescape
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

from openedx.assets.content_files import (
    SJSON_SUFFIX,
    SRT_SUFFIX,
    VTT_SUFFIX,
    file_extension,
    is_transcript,
    open_bundle_members,
    output_digest,
)

log = logging.getLogger(__name__)

# SRT_SUFFIX, SJSON_SUFFIX and is_transcript are defined alongside the document
# extractor, because it is that asset which has to know these are not documents
# -- mimetypes calls a .srt "text/plain" and Tika would happily accept it.
# Dispatch is on the final suffix, matching how mit-learn's
# _transform_content_by_type keys its transformer map, and how Open edX names
# subtitles: subs_<id>.srt.sjson is SJSON, not SRT.

_SRT_TIMESTAMP = re.compile(
    r"\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3}(\n|$)"
)
_SRT_SEQUENCE = re.compile(r"\d+\n")
_SRT_BLANK_LINES = re.compile(r"\n\s*\n")


def text_from_srt_content(content: str) -> str:
    """Strip timestamps and sequence numbers from SRT subtitle content.

    Reproduces mit-learn's ``text_from_srt_content`` exactly, including the
    sequence-number rule being a bare ``\\d+\\n`` -- which also removes a
    caption line that happens to be only digits. That is a real quirk, kept on
    purpose: diverging here would make every such caption a spurious difference
    during parallel validation. Fix it in both repos together or not at all.
    """
    content = _SRT_TIMESTAMP.sub("", content)
    content = _SRT_SEQUENCE.sub("", content)
    return _SRT_BLANK_LINES.sub(" ", content)


def text_from_sjson_content(content: str) -> str | None:
    """Join the caption strings out of Open edX SJSON content.

    Reproduces mit-learn's ``text_from_sjson_content``: the ``text`` array
    joined by spaces, and ``None`` on a JSON parse error rather than a raise, so
    one malformed transcript does not abort the course.
    """
    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        log.exception("Error parsing sjson content")
        return None

    # Valid JSON that is not a caption track still has to fail per-file rather
    # than escaping. `null`, `[]` and `{"text": null}` all parse cleanly and
    # then raise AttributeError or TypeError on the access below, which would
    # abort the whole course instead of costing it one transcript.
    if not isinstance(data, dict):
        log.warning("sjson content is %s, not an object", type(data).__name__)
        return None
    captions = data.get("text")
    if captions is None:
        return ""
    if not isinstance(captions, list):
        log.warning("sjson 'text' is %s, not a list", type(captions).__name__)
        return None
    return " ".join(caption for caption in captions if isinstance(caption, str))


_VTT_BLOCK_SEPARATOR = re.compile(r"\r?\n[ \t]*\r?\n")
_VTT_TAG = re.compile(r"</?[^>]*>")


def text_from_vtt_content(content: str) -> str:
    """Extract the spoken text from WebVTT subtitle content.

    Unlike the two parsers above, this one has no mit-learn counterpart to
    reproduce. MIT Learn's ``_transform_content_by_type`` registers transformers
    only for ``.srt`` and ``.sjson``, so a ``.vtt`` is handed to Tika and stored
    exactly as it came out -- ``WEBVTT`` header, cue timings, speaker tags and
    all. This is therefore a deliberate divergence, not a parity gap.

    It is safe because no consumer reads the timings. Searching the mit-learn
    codebase, the only occurrence of ``-->`` is the SRT stripping regex itself;
    the three things that consume transcript text are OpenSearch indexing (as
    an English-analysed text field), embedding (a plain recursive character
    splitter, markdown-aware only), and LLM summarisation. None of them is
    cue-aware, and MIT Learn already strips timings from SRT and SJSON, which is
    100% of the transcripts in the archives sampled. A consumer that depended on
    cue timings would already be broken for those.

    Structure is handled by one rule: a block is a cue if and only if it
    contains a ``-->`` line, and its text is the lines after that. The WebVTT
    spec forbids ``-->`` in ``NOTE`` bodies, so headers, comments, ``STYLE`` and
    ``REGION`` blocks are all excluded by the same rule rather than by matching
    each keyword. Cue identifiers sit above the timing line and are dropped with
    it. Inline tags (``<v Speaker>``, ``<i>``, ``<c.loud>``, and mid-cue
    ``<00:00:01.000>`` timestamps) are stripped, then HTML entities are
    unescaped -- in that order, so an escaped ``&lt;b&gt;`` in the caption text
    survives as literal text rather than being re-read as a tag.
    """
    cues = []
    for raw_block in _VTT_BLOCK_SEPARATOR.split(content.lstrip("﻿")):
        block = raw_block.strip()
        if not block:
            continue
        lines = block.splitlines()
        timing_index = next(
            (index for index, line in enumerate(lines) if "-->" in line), None
        )
        if timing_index is None:
            continue
        spoken = " ".join(
            _VTT_TAG.sub("", line).strip() for line in lines[timing_index + 1 :]
        )
        spoken = unescape(spoken).strip()
        if spoken:
            cues.append(spoken)
    return " ".join(cues)


def transcript_text(relative_path: str, raw: bytes) -> str | None:
    """Parse one transcript file, dispatching on its suffix.

    Returns None when the file is not a transcript, or when parsing failed --
    the caller distinguishes the two by checking the suffix itself.
    """
    try:
        # utf-8-sig, not utf-8: MIT Learn runs these through Tika, which does
        # charset detection first, so a BOM'd transcript extracts fine there and
        # would fail here. No BOMs appeared in ~20k srt/sjson files sampled
        # across 49 production exports, so this is insurance rather than a fix.
        content = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        log.exception("Transcript is not valid UTF-8: %s", relative_path)
        return None

    suffix = Path(relative_path).suffix.lower()
    if suffix == SJSON_SUFFIX:
        return text_from_sjson_content(content)
    if suffix == SRT_SUFFIX:
        return text_from_srt_content(content)
    if suffix == VTT_SUFFIX:
        return text_from_vtt_content(content)
    return None


def build_transcript_rows(
    members: Iterable[tuple[str, Callable[[], bytes]]],
    *,
    course_id: str,
    source_system: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Parse every transcript in the bundle, returning rows and counters.

    Split out from the asset so the dispatch and failure accounting are testable
    without a Dagster context.

    ``members`` yields ``(relative_path, read_bytes)``. The bytes stay behind a
    callable so the many non-transcript files in an export are never read.

    A transcript that parses to empty text is still recorded, and so is one that
    fails to parse. An empty caption track, an unparseable file and a file that
    was never seen are three different facts, and only the row carries which.
    """
    rows: list[dict[str, Any]] = []
    counters = {"candidates": 0, "extracted": 0, "empty": 0, "failed": 0}

    for relative_path, read_bytes in members:
        if not is_transcript(relative_path):
            continue

        counters["candidates"] += 1
        raw = read_bytes()
        text = transcript_text(relative_path, raw)

        if text is None:
            counters["failed"] += 1
            # A failed parse still gets a row. Emitting nothing makes it
            # indistinguishable downstream from the file having been deleted
            # from the course, which is what MIT Learn unpublishes on.
            rows.append(
                {
                    "course_id": course_id,
                    "source_system": source_system,
                    "file_path": relative_path,
                    "content_type": (
                        "application/json"
                        if relative_path.lower().endswith(SJSON_SUFFIX)
                        else "text/plain"
                    ),
                    "size_bytes": len(raw),
                    "content": None,
                    "extraction_status": "failed",
                }
            )
            continue

        # Status is decided on the stripped text, so the content must be
        # nulled on that same basis. Keying the content off `text or None`
        # instead let a whitespace-only transcript be stored as a non-null
        # string under an "empty" status, which the document rows never do.
        has_text = bool(text.strip())
        if has_text:
            # "extracted", not "parsed": these rows are unioned with the
            # document rows downstream, and a shared schema whose status
            # vocabulary differs by source is not actually shared.
            counters["extracted"] += 1
            status = "extracted"
        else:
            counters["empty"] += 1
            status = "empty"

        rows.append(
            {
                "course_id": course_id,
                "source_system": source_system,
                "file_path": relative_path,
                # Both row sources are unioned into one model downstream, so a
                # field present on document rows and absent here is a schema
                # inconsistency, not a harmless omission -- every
                # transcript-derived row would read as a null extension.
                "file_extension": file_extension(relative_path),
                "content_type": (
                    "application/json"
                    if relative_path.lower().endswith(SJSON_SUFFIX)
                    else "text/plain"
                ),
                "size_bytes": len(raw),
                "content": text if has_text else None,
                "extraction_status": status,
            }
        )

    return rows, counters


def assert_parsing_healthy(counters: dict[str, int], course_id: str) -> None:
    """Refuse to publish a course whose every transcript failed to parse.

    Without this the asset publishes a JSONL of nothing but failure rows, which
    a consumer that filters on status reads as "this course has no transcripts"
    -- the failure mode that looks like success. The document asset guards the
    same case; this is the total-failure half of it, which is the part that is
    not Tika-specific. There is no success-rate floor here because parsing is
    local: a partial failure means malformed files, not a sick service.
    """
    candidates = counters["candidates"]
    if candidates and not (counters["extracted"] + counters["empty"]):
        msg = (
            f"Refusing to publish transcript text for {course_id}: all "
            f"{candidates} transcripts failed to parse. Publishing would be "
            "indistinguishable from 'this course has no transcripts'."
        )
        raise RuntimeError(msg)


@asset(
    key=AssetKey(("openedx", "processed_data", "course_transcript_text")),
    group_name="openedx",
    ins={
        "course_static_assets": AssetIn(
            key=AssetKey(("openedx", "processed_data", "course_static_assets"))
        )
    },
    io_manager_key="s3file_io_manager",
    automation_condition=upstream_or_code_changes(),
    required_resource_keys={"openedx"},
    description=(
        "Plain text parsed from the video transcripts in a course archive "
        "(SRT and Open edX SJSON). One JSONL row per transcript."
    ),
)
def extract_course_transcript_text(
    context: AssetExecutionContext, course_static_assets: UPath
):
    """Parse every subtitle file in the course bundle into plain text."""
    source_system = context.resources.openedx.deployment
    course_id = context.partition_key

    temp_files: list[Path] = []
    try:
        with open_bundle_members(course_static_assets, temp_files) as members:
            rows, counters = build_transcript_rows(
                members, course_id=course_id, source_system=source_system
            )
        assert_parsing_healthy(counters, course_id)

        output_file = Path(
            NamedTemporaryFile(delete=False, suffix="_transcript_text.jsonl").name
        )
        temp_files.append(output_file)
        with jsonlines.open(output_file, "w") as writer:
            writer.write_all(rows)

        data_version = output_digest(output_file)
        object_key = (
            f"{'/'.join(context.asset_key.path)}/{source_system}/"
            f"{course_id}/{data_version}.jsonl"
        )

        context.log.info(
            "Parsed %d/%d transcripts for %s (%d empty, %d failed)",
            counters["extracted"],
            counters["candidates"],
            course_id,
            counters["empty"],
            counters["failed"],
        )

        yield Output(
            (output_file, object_key),
            data_version=DataVersion(data_version),
            metadata={
                "course_id": course_id,
                "object_key": object_key,
                "transcript_count": len(rows),
                "extracted": counters["extracted"],
                "empty": counters["empty"],
                "failed": counters["failed"],
            },
        )
    finally:
        for temp_file in temp_files:
            temp_file.unlink(missing_ok=True)

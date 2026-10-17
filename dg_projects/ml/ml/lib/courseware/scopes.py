"""Group int__courseware__extraction_sources rows into the scopes sent to the model."""

import hashlib
from dataclasses import dataclass, field

import polars as pl

# Rough characters per token for English courseware, to size calls without a
# tokenizer round trip. Only used against the budgets below.
CHARS_PER_TOKEN = 4
# One call reads at most this much source text; a larger scope is split into parts
# at source boundaries.
SCOPE_INPUT_BUDGET_CHARS = 120_000 * CHARS_PER_TOKEN
# A single source longer than this is cut, and the cut is noted on the extraction.
SOURCE_MAX_CHARS = 100_000 * CHARS_PER_TOKEN
COURSE_DESCRIPTION_MAX_CHARS = 4_000
# about/ pages that describe the course, in the order they are preferred
DESCRIPTION_PAGES = (
    "about/short_description.html",
    "about/overview.html",
    "about/description.html",
)


@dataclass(frozen=True)
class Source:
    source_id: str
    source_ref: str
    source_kind: str
    title: str
    text: str
    total_chars: int

    @property
    def truncated(self) -> bool:
        return len(self.text) < self.total_chars

    @property
    def block(self) -> str:
        return (
            f"=== SOURCE {self.source_id} | kind={self.source_kind} | "
            f"title={self.title!r} ===\n{self.text}\n=== END {self.source_id} ==="
        )


@dataclass(frozen=True)
class Course:
    platform: str
    courserun_readable_id: str
    title: str
    outline: tuple[str, ...]
    description: str


@dataclass(frozen=True)
class Scope:
    course: Course
    scope_id: str
    scope_kind: str
    scope_title: str
    scope_position: int
    part: int
    part_count: int
    sources: tuple[Source, ...] = field(default=())

    @property
    def scope_key(self) -> str:
        """The scope and part, unique within a run."""
        return f"{self.scope_id}#{self.part}"

    @property
    def sources_block(self) -> str:
        return "\n\n".join(source.block for source in self.sources)

    @property
    def input_text_checksum(self) -> str:
        """SHA-256 of exactly the source text the model reads for this scope."""
        return hashlib.sha256(self.sources_block.encode()).hexdigest()

    @property
    def part_note(self) -> str | None:
        if self.part_count == 1:
            return None
        return (
            f"This is part {self.part} of {self.part_count} of the scope; the other "
            "parts are extracted separately."
        )


def _course(rows: pl.DataFrame) -> Course:
    units = (
        rows.filter(pl.col("scope_kind") == "unit")
        .select("scope_position", "scope_title")
        .unique()
        .sort("scope_position")
    )
    context = rows.filter(pl.col("scope_kind") == "context")
    description = ""
    for page in DESCRIPTION_PAGES:
        texts = context.filter(pl.col("source_ref") == page)["source_text"]
        if len(texts):
            description = texts[0][:COURSE_DESCRIPTION_MAX_CHARS]
            break
    first = rows.row(0, named=True)
    return Course(
        platform=first["platform"],
        courserun_readable_id=first["courserun_readable_id"],
        title=first["course_title"] or first["courserun_readable_id"],
        outline=tuple(units["scope_title"]),
        description=description,
    )


def _split(sources: list[Source]) -> list[list[Source]]:
    """Cut an ordered source list into parts that each fit the input budget."""
    parts: list[list[Source]] = [[]]
    size = 0
    for source in sources:
        if parts[-1] and size + len(source.text) > SCOPE_INPUT_BUDGET_CHARS:
            parts.append([])
            size = 0
        parts[-1].append(source)
        size += len(source.text)
    return parts


def build_scopes(rows: pl.DataFrame) -> list[Scope]:
    """Return every scope of one course run, in course order.

    :param rows: the run's int__courseware__extraction_sources rows
    :returns: one Scope per scope part
    :rtype: list[Scope]
    """
    course = _course(rows)
    scopes = []
    for (scope_id,), scope_rows in rows.sort(
        "scope_position", "source_position"
    ).group_by("scope_id", maintain_order=True):
        meta = scope_rows.row(0, named=True)
        sources = [
            Source(
                source_id="",
                source_ref=row["source_ref"],
                source_kind=row["source_kind"],
                title=row["source_title"] or row["source_ref"],
                text=row["source_text"][:SOURCE_MAX_CHARS],
                total_chars=len(row["source_text"]),
            )
            for row in scope_rows.iter_rows(named=True)
        ]
        parts = _split(sources)
        for part_number, part_sources in enumerate(parts, 1):
            # ids restart in each part, so a part's prompt depends only on its sources
            numbered = tuple(
                Source(**{**vars(source), "source_id": f"S{index:02d}"})
                for index, source in enumerate(part_sources, 1)
            )
            scopes.append(
                Scope(
                    course=course,
                    scope_id=str(scope_id),
                    scope_kind=meta["scope_kind"],
                    scope_title=meta["scope_title"],
                    scope_position=meta["scope_position"],
                    part=part_number,
                    part_count=len(parts),
                    sources=numbered,
                )
            )
    return scopes

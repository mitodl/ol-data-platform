"""Tag courseware text with the known concepts it mentions, by dictionary matching.

The dictionary is dim_concept_alias: every normalized label key that resolution has
assigned to a concept. Text is normalized the way int__courseware__claims builds
`concept_label_key` (lowercase, each run of characters outside a-z and 0-9 becomes
one separator), so an alias matches wherever its words appear in order with a token
boundary on both sides: "art" never matches inside "smart", and a letter outside
a-z (the "ï" of "naïve", the "é" of "intérieur") counts as part of the word next to
it, not as a boundary. Two Aho-Corasick automata report every candidate, one over the
lowercased text for caseless phrases and one over the original case for
case-sensitive spellings. The candidates are then resolved leftmost-longest, so
"decision tree" wins over "tree" at the same place, a nested alias is not reported a
second time, and a spelling rejected for its case never hides a shorter match.

Python's lower() and the SQL engines' differ on a few characters whose lowercase
form is ASCII (the Kelvin sign, a dotted capital I), so a label containing one can
normalize to a different key here than in SQL. No label on the pilot has one.

Besides the label itself, a few variants are matched, none of which may replace an
alias that is a label of its own (an explicit label always keeps its assignment):

- plural: the last word plus "s" ("decision trees"), matched the way its base is
  ("Gits" for "Git", never "gits").
- abbreviation: a one-word parenthetical whose letters abbreviate the rest of the
  label, by the Schwartz-Hearst rule (each character occurs in the long form in
  order, and the first starts a word): "MSE" from "mean squared error (MSE)",
  "CART", "int" from "integer (int)". full_name is the label without it.
- without_qualifier: the label without any other parenthetical ("entropy" from
  "entropy (information theory)", "bias" from "bias (ML)"). The qualifier is there
  because the head alone is ambiguous, so these phrases are flagged and matched
  case-sensitively ("None" from "None (special object)" is not "none"), unless some
  other label also gives the phrase as a full name. An empty "()" is ignored.

Each distinct phrase gets a match mode:

- caseless: the default.
- case_sensitive: phrases shorter than CASE_SENSITIVE_BELOW_CHARS ("AUC", "Git"),
  acronyms of any length ("CART", "MNIST") and qualified heads match only where the
  text has the alias's own case, so a shopping cart is not CART.
- skipped: phrases shorter than MIN_ALIAS_CHARS ("R"), which nothing in the text
  can tell apart from a variable or an initial.

and ambiguity flags: short, qualified (a head without its qualifier), single_word
(one lowercase word, e.g. "recall", "kernel", "data") and multiple_concepts (the
phrase belongs to more than one concept, which emits one mention per candidate).
Ambiguous mentions are emitted with the flag rather than dropped. Many single words
are what these courses teach (variance, recall, regression), so dropping them would
cost more recall than the flag costs a consumer, who can filter on it or require
the extractor's corroboration.

These are mentions only. Whether a block teaches a concept is the extractor's call.
"""

import bisect
import hashlib
import json
import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cached_property
from itertools import accumulate
from typing import Literal

import ahocorasick_rs
import polars as pl

TAGGER_ID = "courseware_dictionary_tagger_v2"
MIN_ALIAS_CHARS = 2
CASE_SENSITIVE_BELOW_CHARS = 4
# a plural variant is only added for a last word at least this long
PLURAL_MIN_WORD_CHARS = 3

MatchMode = Literal["caseless", "case_sensitive", "skipped"]
Variant = Literal["label", "full_name", "abbreviation", "without_qualifier", "plural"]
# which entry speaks for a concept when several produce the same phrase
_VARIANT_ORDER: dict[Variant, int] = {
    "label": 0,
    "full_name": 1,
    "abbreviation": 2,
    "without_qualifier": 3,
    "plural": 4,
}

# concept_label_key's token class: int__courseware__claims splits lower() on [^a-z0-9]+
_TOKEN = re.compile(r"[A-Za-z0-9]+")
_PARENTHETICAL = re.compile(r"\(([^)]*)\)")
_ACRONYM = re.compile(r"[^a-z]*[A-Z][^a-z]*")
# tokens are joined by two spaces and padded by one, so every pattern (" a  b ")
# starts and ends on a boundary and adjacent mentions do not compete for a separator
_SEPARATOR = "  "

SOURCE_COLUMNS = (
    "platform",
    "courserun_readable_id",
    "scope_id",
    "scope_kind",
    "source_ref",
    "source_kind",
    "source_text",
)
MENTION_SCHEMA = {
    "tagging_run_id": pl.String,
    "platform": pl.String,
    "courserun_readable_id": pl.String,
    "scope_id": pl.String,
    "scope_kind": pl.String,
    "source_ref": pl.String,
    "source_kind": pl.String,
    "pattern_key": pl.String,
    "concept_id": pl.String,
    "matched_text": pl.String,
    "char_start": pl.Int64,
    "char_end": pl.Int64,
}
ALIAS_SCHEMA = {
    "tagging_run_id": pl.String,
    "pattern_key": pl.String,
    "concept_id": pl.String,
    "concept_label_key": pl.String,
    "variant": pl.String,
    "surface": pl.String,
    "match_mode": pl.String,
    "is_short": pl.Boolean,
    "is_qualified": pl.Boolean,
    "is_single_word": pl.Boolean,
    "has_multiple_concepts": pl.Boolean,
    "is_ambiguous": pl.Boolean,
}
RUN_SCHEMA = {
    "tagging_run_id": pl.String,
    "input_key": pl.String,
    "tagger_id": pl.String,
    "parameters_json": pl.String,
    "dictionary_checksum": pl.String,
    "sources_checksum": pl.String,
    "label_count": pl.Int64,
    "pattern_count": pl.Int64,
    "case_sensitive_pattern_count": pl.Int64,
    "skipped_pattern_count": pl.Int64,
    "ambiguous_pattern_count": pl.Int64,
    "source_count": pl.Int64,
    "char_count": pl.Int64,
    "tagged_source_count": pl.Int64,
    "mention_count": pl.Int64,
    "match_seconds": pl.Float64,
    "elapsed_seconds": pl.Float64,
    "tagged_at": pl.Datetime(time_zone="UTC"),
}


def label_key(label: str) -> str:
    """Normalize a label the way concept_label_key is built in SQL.

    :param label: any text
    :returns: lowercase alphanumeric tokens joined by single spaces
    :rtype: str
    """
    return " ".join(_TOKEN.findall(label)).lower()


@dataclass(frozen=True)
class Entry:
    """One way a concept's label can appear in text."""

    concept_label_key: str
    concept_id: str
    variant: Variant
    surface: str
    # a head without its qualifier, or that head's plural
    is_qualified: bool = False
    # for a plural, the normalized phrase it is the plural of
    base_key: str | None = None


@dataclass(frozen=True)
class Pattern:
    """A distinct normalized phrase the automaton matches, with its entries."""

    key: str
    entries: tuple[Entry, ...]

    @cached_property
    def token_count(self) -> int:
        return self.key.count(" ") + 1

    @property
    def is_short(self) -> bool:
        """The phrase, or the phrase a plural comes from, is short."""
        return (
            min(len(entry.base_key or self.key) for entry in self.entries)
            < CASE_SENSITIVE_BELOW_CHARS
        )

    @property
    def match_mode(self) -> MatchMode:
        if len(self.key) < MIN_ALIAS_CHARS:
            return "skipped"
        if self.is_short or self.is_acronym or self.is_qualified:
            return "case_sensitive"
        return "caseless"

    @property
    def is_qualified(self) -> bool:
        """Every entry is a head without its qualifier, none a name in its own right."""
        return all(entry.is_qualified for entry in self.entries)

    @property
    def is_acronym(self) -> bool:
        """Every spelling is uppercase, apart from a plural "s"."""
        return all(
            _ACRONYM.fullmatch(entry.surface.removesuffix("s"))
            for entry in self.entries
        )

    @property
    def is_single_word(self) -> bool:
        return (
            self.token_count == 1
            and self.key.isalpha()
            and all(entry.surface == entry.surface.lower() for entry in self.entries)
        )

    @cached_property
    def concept_entries(self) -> tuple[Entry, ...]:
        """One entry per concept, the label itself before any variant."""
        chosen: dict[str, Entry] = {}
        for entry in sorted(
            self.entries,
            key=lambda e: (_VARIANT_ORDER[e.variant], e.concept_label_key),
        ):
            chosen.setdefault(entry.concept_id, entry)
        return tuple(chosen[concept_id] for concept_id in sorted(chosen))

    @property
    def has_multiple_concepts(self) -> bool:
        return len(self.concept_entries) > 1

    @property
    def is_ambiguous(self) -> bool:
        return (
            self.is_short
            or self.is_qualified
            or self.is_single_word
            or self.has_multiple_concepts
        )

    @cached_property
    def spellings(self) -> frozenset[tuple[str, ...]]:
        """The spellings a case_sensitive pattern matches, as tokens."""
        return frozenset(tuple(_TOKEN.findall(e.surface)) for e in self.entries)


def _plural(surface: str) -> str | None:
    last = _TOKEN.findall(surface)[-1]
    if (
        surface.endswith(last)
        and last.isalpha()
        and len(last) >= PLURAL_MIN_WORD_CHARS
        and not last.lower().endswith("s")
    ):
        return f"{surface}s"
    return None


def _abbreviates(short: str, long: str) -> bool:
    """Whether `short` abbreviates `long` by the Schwartz-Hearst rule.

    Every alphanumeric character of `short` occurs in `long` in order (matched from
    the right), and the first one starts a word of `long`.
    """
    characters = [char.lower() for char in short if char.isalnum()]
    text = long.lower()
    position = len(text) - 1
    for index in range(len(characters) - 1, -1, -1):
        while position >= 0 and (
            text[position] != characters[index]
            or (index == 0 and position > 0 and text[position - 1].isalnum())
        ):
            position -= 1
        if position < 0:
            return False
        position -= 1
    return bool(characters)


def _entries(concept_label_key: str, concept_id: str, alias: str) -> list[Entry]:
    names: list[Entry] = [Entry(concept_label_key, concept_id, "label", alias)]
    parentheticals = [
        inner.strip() for inner in _PARENTHETICAL.findall(alias) if _TOKEN.search(inner)
    ]
    head = " ".join(_PARENTHETICAL.sub(" ", alias).split())
    if parentheticals and _TOKEN.search(head):
        abbreviations = [
            inner
            for inner in parentheticals
            if len(_TOKEN.findall(inner)) == 1 and _abbreviates(inner, head)
        ]
        qualified = len(abbreviations) < len(parentheticals)
        names.append(
            Entry(
                concept_label_key,
                concept_id,
                "without_qualifier" if qualified else "full_name",
                head,
                is_qualified=qualified,
            )
        )
        names.extend(
            Entry(concept_label_key, concept_id, "abbreviation", abbreviation)
            for abbreviation in abbreviations
        )
    plurals = [
        Entry(
            concept_label_key,
            concept_id,
            "plural",
            plural,
            is_qualified=name.is_qualified,
            base_key=label_key(name.surface),
        )
        for name in names
        if (plural := _plural(name.surface))
    ]
    return names + plurals


class ConceptDictionary:
    """An automaton over every taggable phrase of every concept label."""

    def __init__(self, aliases: pl.DataFrame) -> None:
        """Build the dictionary from dim_concept_alias rows.

        :param aliases: rows with concept_label_key, concept_fk and alias
        """
        rows = sorted(
            aliases.select("concept_label_key", "concept_fk", "alias").iter_rows()
        )
        explicit = {key for key, _, _ in rows}
        self.label_count = len(explicit)
        by_phrase: dict[str, set[Entry]] = defaultdict(set)
        for key, concept_id, alias in rows:
            for entry in _entries(key, concept_id, alias):
                phrase = key if entry.variant == "label" else label_key(entry.surface)
                if phrase and (entry.variant == "label" or phrase not in explicit):
                    by_phrase[phrase].add(entry)
        self.patterns = [
            Pattern(phrase, tuple(sorted(entries, key=_entry_order)))
            for phrase, entries in sorted(by_phrase.items())
        ]
        # (pattern, token count) per automaton pattern: caseless keys, exact spellings
        self._caseless = [
            (p, p.token_count) for p in self.patterns if p.match_mode == "caseless"
        ]
        exact = [
            (pattern, spelling)
            for pattern in self.patterns
            if pattern.match_mode == "case_sensitive"
            for spelling in sorted(pattern.spellings)
        ]
        self._exact = [(pattern, len(spelling)) for pattern, spelling in exact]
        self._caseless_automaton = ahocorasick_rs.AhoCorasick(
            [f" {p.key.replace(' ', _SEPARATOR)} " for p, _ in self._caseless]
        )
        self._exact_automaton = ahocorasick_rs.AhoCorasick(
            [f" {_SEPARATOR.join(spelling)} " for _, spelling in exact]
        )
        self.checksum = hashlib.sha256(json.dumps(rows).encode()).hexdigest()

    def find(self, text: str) -> list[tuple[Pattern, int, int]]:
        """Find every concept mention in a text.

        :param text: the source text
        :returns: (pattern, char_start, char_end) per mention, offsets into `text`
        :rtype: list[tuple[Pattern, int, int]]
        """
        spans = [match.span() for match in _TOKEN.finditer(text)]
        if not spans:
            return []
        joined = f" {_SEPARATOR.join(text[start:end] for start, end in spans)} "
        # where each token starts in `joined`
        token_offsets = list(
            accumulate(
                (end - start + len(_SEPARATOR) for start, end in spans[:-1]), initial=1
            )
        )
        candidates: list[tuple[int, int, Pattern]] = []
        for patterns, haystack, automaton in (
            (self._caseless, joined.lower(), self._caseless_automaton),
            (self._exact, joined, self._exact_automaton),
        ):
            for index, start, _end in automaton.find_matches_as_indexes(
                haystack, overlapping=True
            ):
                pattern, token_count = patterns[index]
                first = bisect.bisect_left(token_offsets, start + 1)
                last = first + token_count - 1
                char_start, char_end = spans[first][0], spans[last][1]
                # [A-Za-z0-9] tokens stop at a non-ASCII letter, which is still a word
                if (char_start > 0 and text[char_start - 1].isalnum()) or (
                    char_end < len(text) and text[char_end].isalnum()
                ):
                    continue
                candidates.append((first, last, pattern))
        # leftmost-longest over every valid candidate
        candidates.sort(key=lambda candidate: (candidate[0], -candidate[1]))
        found = []
        covered_until = -1
        for first, last, pattern in candidates:
            if first > covered_until:
                found.append((pattern, spans[first][0], spans[last][1]))
                covered_until = last
        return found


def _entry_order(entry: Entry) -> tuple[str, int, str, str]:
    return (
        entry.concept_id,
        _VARIANT_ORDER[entry.variant],
        entry.concept_label_key,
        entry.surface,
    )


@dataclass(frozen=True)
class TaggingResult:
    """A full snapshot of one tagging run: its mentions, the dictionary as matched
    (one row per phrase and concept, with its mode and flags), and the run record.
    """

    mentions: pl.DataFrame
    aliases: pl.DataFrame
    run: pl.DataFrame


def _parameters() -> str:
    return json.dumps(
        {
            "min_alias_chars": MIN_ALIAS_CHARS,
            "case_sensitive_below_chars": CASE_SENSITIVE_BELOW_CHARS,
            "plural_min_word_chars": PLURAL_MIN_WORD_CHARS,
        },
        sort_keys=True,
    )


def _sources_checksum(sources: pl.DataFrame) -> str:
    digest = hashlib.sha256()
    for row in sources.select(SOURCE_COLUMNS).sort(SOURCE_COLUMNS[:5]).iter_rows():
        digest.update(json.dumps(row).encode())
    return digest.hexdigest()


def tagging_input_key(dictionary: ConceptDictionary, sources: pl.DataFrame) -> str:
    """Derive the key of everything a run over these sources depends on.

    A caller skips a run whose key equals the newest stored run's. An older run
    with the same key is not enough, since a run in between replaced it.

    :param dictionary: the concept dictionary
    :param sources: int__courseware__extraction_sources rows
    :returns: a sha256 hex digest of the tagger, its parameters and both inputs
    :rtype: str
    """
    return _input_key(dictionary.checksum, _sources_checksum(sources))


def _input_key(dictionary_checksum: str, sources_checksum: str) -> str:
    return hashlib.sha256(
        json.dumps(
            [TAGGER_ID, _parameters(), dictionary_checksum, sources_checksum]
        ).encode()
    ).hexdigest()


def tag_sources(sources: pl.DataFrame, aliases: pl.DataFrame) -> TaggingResult:
    """Tag every source with the concepts its text mentions.

    :param sources: int__courseware__extraction_sources rows (SOURCE_COLUMNS)
    :param aliases: dim_concept_alias rows (concept_label_key, concept_fk, alias)
    :returns: the run's mentions, alias table and run record
    :rtype: TaggingResult
    """
    tagged_at = datetime.now(tz=UTC)
    began = time.perf_counter()
    dictionary = ConceptDictionary(aliases)
    run_id = str(uuid.uuid4())
    sources_checksum = _sources_checksum(sources)

    mentions: list[dict[str, object]] = []
    tagged_sources = 0
    char_count = 0
    started = time.perf_counter()
    for row in sources.select(SOURCE_COLUMNS).iter_rows(named=True):
        text = row["source_text"]
        char_count += len(text)
        found = dictionary.find(text)
        tagged_sources += bool(found)
        mentions.extend(
            {
                "tagging_run_id": run_id,
                "platform": row["platform"],
                "courserun_readable_id": row["courserun_readable_id"],
                "scope_id": row["scope_id"],
                "scope_kind": row["scope_kind"],
                "source_ref": row["source_ref"],
                "source_kind": row["source_kind"],
                "pattern_key": pattern.key,
                "concept_id": entry.concept_id,
                "matched_text": text[start:end],
                "char_start": start,
                "char_end": end,
            }
            for pattern, start, end in found
            for entry in pattern.concept_entries
        )
    match_seconds = time.perf_counter() - started

    alias_rows = [
        {
            "tagging_run_id": run_id,
            "pattern_key": pattern.key,
            "concept_id": entry.concept_id,
            "concept_label_key": entry.concept_label_key,
            "variant": entry.variant,
            "surface": entry.surface,
            "match_mode": pattern.match_mode,
            "is_short": pattern.is_short,
            "is_qualified": pattern.is_qualified,
            "is_single_word": pattern.is_single_word,
            "has_multiple_concepts": pattern.has_multiple_concepts,
            "is_ambiguous": pattern.is_ambiguous,
        }
        for pattern in dictionary.patterns
        for entry in pattern.concept_entries
    ]
    modes = [pattern.match_mode for pattern in dictionary.patterns]
    mention_frame = pl.DataFrame(mentions, schema=MENTION_SCHEMA)
    alias_frame = pl.DataFrame(alias_rows, schema=ALIAS_SCHEMA)
    run = {
        "tagging_run_id": run_id,
        "input_key": _input_key(dictionary.checksum, sources_checksum),
        "tagger_id": TAGGER_ID,
        "parameters_json": _parameters(),
        "dictionary_checksum": dictionary.checksum,
        "sources_checksum": sources_checksum,
        "label_count": dictionary.label_count,
        "pattern_count": len(dictionary.patterns),
        "case_sensitive_pattern_count": modes.count("case_sensitive"),
        "skipped_pattern_count": modes.count("skipped"),
        "ambiguous_pattern_count": sum(p.is_ambiguous for p in dictionary.patterns),
        "source_count": sources.height,
        "char_count": char_count,
        "tagged_source_count": tagged_sources,
        "mention_count": len(mentions),
        "match_seconds": match_seconds,
        "elapsed_seconds": time.perf_counter() - began,
        "tagged_at": tagged_at,
    }
    return TaggingResult(
        mentions=mention_frame,
        aliases=alias_frame,
        run=pl.DataFrame([run], schema=RUN_SCHEMA),
    )

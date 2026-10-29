"""Tag courseware text with the known concepts it mentions, by dictionary matching.

The dictionary is dim_concept_alias: every normalized label key that resolution has
assigned to a concept. Text is tokenized by the rule that builds concept_label_key
(the courseware_label_key macro, and label_key here): lowercase runs of Unicode
letters and numbers, with an attached + or # spelled out as "plus" or "sharp" and a
leading dot as "dot". So "C++" in text is the phrase "c plus plus", ".NET" is "dot
net", and "Schrödinger" one token. An alias matches wherever its tokens appear in
order, so "art" never matches inside "smart" and "intérieur" never matches `int`.
Two Aho-Corasick automata report every candidate, one over the lowercase tokens for
caseless phrases and one over the tokens as spelled for case-sensitive spellings.
The candidates are then resolved leftmost-longest, so "decision tree" wins over
"tree" at the same place, a nested alias is not reported a second time, and a
spelling rejected for its case never hides a shorter match. Each mention's offsets
cover the source characters its tokens came from.

Python's lower() and the SQL engines' differ on a few characters (the Kelvin sign,
a dotted capital I), so a label containing one can normalize to a different key
here than in SQL. No label on the pilot has one.

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
(one word of everyday English, in any case: "recall", "kernel", "data", "None", but
not "pandas"; see COMMON_WORDS) and multiple_concepts (the phrase belongs to more
than one concept, which emits one mention per candidate).
Ambiguous mentions are emitted with the flag rather than dropped. Many single words
are what these courses teach (variance, recall, regression), so dropping them would
cost more recall than the flag costs a consumer. The flags are review signals: which
of a label's concepts a mention means is decided in dbt by the course run's sense of
the label (int__courseware__concept_tags), not here.

These are mentions only. Whether a block teaches a concept is the extractor's call.

A run re-tags only what changed. Tagging a source depends on nothing but its own
text and the dictionary, so while the dictionary, TAGGER_ID and the parameters stay
the same, a run tags only the sources whose text is new or changed, and every other
source keeps the mentions an earlier run wrote. Each run records, per current
source, which run's mention rows hold its mentions (courseware_concept_tagging_source),
and readers take the newest run's records. A run with any other dictionary, tagger
or parameters re-tags every source.
"""

import hashlib
import json
import re
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cached_property
from importlib.resources import files
from typing import Any, Literal

import ahocorasick_rs
import numpy as np
import polars as pl

TAGGER_ID = "courseware_dictionary_tagger_v5"
MIN_ALIAS_CHARS = 2
CASE_SENSITIVE_BELOW_CHARS = 4
# a plural variant is only added for a last word at least this long
PLURAL_MIN_WORD_CHARS = 3
# Words of everyday English, whose concept may not be what the text means by them:
# the most frequent English words in wordfreq (see the file's header for its
# provenance and license). A one-word phrase is single_word when it, or for a
# plural the word it is the plural of, is one of these.
COMMON_WORDS = frozenset(
    line
    for line in files(__package__)
    .joinpath("common_words.txt")
    .read_text(encoding="utf-8")
    .splitlines()
    if line and not line.startswith("#")
)

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

# runs of Unicode letters and numbers, the courseware_label_key macro's \p{L}\p{N}
_WORD = re.compile(r"[^\W_]+")
_TEXT_ITEM = re.compile(r"[^\W_]+|[+#.]")
# the SQL engines' \s (RE2), which is ASCII only
_WHITESPACE = " \t\n\f\r"
_SPELLED_SYMBOLS = {"+": "plus", "#": "sharp"}
_NON_WORD = re.compile(r"[\W_]")
_WORD_SPLIT = re.compile(r"([^\W_]+)")
_ATTACHED_SYMBOL = re.compile(rf"[^{_WHITESPACE}][+#]")
_DOT_AFTER = tuple(f"{char}." for char in f"{_WHITESPACE}+#")
_PARENTHETICAL = re.compile(r"\(([^)]*)\)")
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
# int__courseware__extraction_sources is unique on these
SOURCE_KEY = ("platform", "courserun_readable_id", "scope_id", "source_ref")
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
# One row per run and current source: which run's mention rows hold its mentions
SOURCE_SCHEMA = {
    "tagging_run_id": pl.String,
    "platform": pl.String,
    "courserun_readable_id": pl.String,
    "scope_id": pl.String,
    "source_ref": pl.String,
    "source_checksum": pl.String,
    "mention_run_id": pl.String,
}
RUN_SCHEMA = {
    "tagging_run_id": pl.String,
    "previous_tagging_run_id": pl.String,
    "input_key": pl.String,
    "tagger_id": pl.String,
    "parameters_json": pl.String,
    "dictionary_checksum": pl.String,
    "sources_checksum": pl.String,
    "label_count": pl.Int64,
    "tokenless_label_count": pl.Int64,
    "pattern_count": pl.Int64,
    "case_sensitive_pattern_count": pl.Int64,
    "skipped_pattern_count": pl.Int64,
    "ambiguous_pattern_count": pl.Int64,
    "source_count": pl.Int64,
    "retagged_source_count": pl.Int64,
    "char_count": pl.Int64,
    "tagged_source_count": pl.Int64,
    "mention_count": pl.Int64,
    "match_seconds": pl.Float64,
    "elapsed_seconds": pl.Float64,
    "tagged_at": pl.Datetime(time_zone="UTC"),
}


def label_key(label: str) -> str | None:
    """Normalize a label exactly as the courseware_label_key macro does.

    :param label: any text
    :returns: the key, or None when the label has no letter or number
    :rtype: str | None
    """
    # checked first, as "." and ".+" would otherwise spell out to "dot" and "dot plus"
    if not _WORD.search(label):
        return None
    key = label.lower()
    key = re.sub(r"^[+#]", " ", key)
    key = re.sub(rf"[{_WHITESPACE}][+#]", " ", key)
    key = key.replace("+", " plus ").replace("#", " sharp ")
    key = re.sub(r"^[.]", "dot ", key)
    key = re.sub(rf"[{_WHITESPACE}][.]", " dot ", key)
    return re.sub(r"[\W_]+", " ", key).strip()


@dataclass(frozen=True)
class _Tokenized:
    """A text's tokens as the two haystacks the automata scan, with where each token
    starts in each haystack and in the text.
    """

    caseless: str
    exact: str
    caseless_offsets: np.ndarray
    exact_offsets: np.ndarray
    starts: np.ndarray
    ends: np.ndarray


def _haystack_offsets(words_lengths: np.ndarray) -> np.ndarray:
    # each token follows the leading space, or the previous token and a separator
    offsets = np.ones(len(words_lengths), dtype=np.int64)
    np.cumsum(words_lengths[:-1] + len(_SEPARATOR), out=offsets[1:])
    offsets[1:] += 1
    return offsets


def _spells_out_symbols(text: str) -> bool:
    """Whether label_key would spell a symbol out somewhere in the text.

    A + or # attached to what precedes it, or a dot at the start or after whitespace
    or a symbol. It may say yes when the answer is no (the full tokenizer is exact
    either way), and uses substring tests so the common text is cheap to rule out.
    """
    if ("+" in text or "#" in text) and _ATTACHED_SYMBOL.search(text):
        return True
    return text.startswith(".") or any(pair in text for pair in _DOT_AFTER)


def _tokenize(text: str) -> _Tokenized | None:
    """Tokenize text as _tokens does, into haystacks; None when it has no tokens.

    Text with no symbol label_key spells out (most courseware) takes a fast path:
    its tokens are its runs of letters and numbers, found and measured in C, unless
    a run's lowercase form splits ("İ").
    """
    if not _spells_out_symbols(text):
        # separators and words alternate, starting and ending with a separator
        parts = _WORD_SPLIT.split(text)
        spelled = parts[1::2]
        if not spelled:
            return None
        joined = _SEPARATOR.join(spelled)
        lowered = joined.lower()
        # only "İ" changes length when lowercased, and it splits
        if lowered.isascii() or not _NON_WORD.search(lowered.replace(_SEPARATOR, "")):
            bounds = np.cumsum(np.fromiter(map(len, parts), np.int64, len(parts)))
            starts, ends = bounds[0:-1:2], bounds[1::2]
            offsets = _haystack_offsets(ends - starts)
            return _Tokenized(
                f" {lowered} ", f" {joined} ", offsets, offsets, starts, ends
            )
    tokens = _tokens(text)
    if not tokens:
        return None
    lows = [low for low, _, _, _ in tokens]
    spelled = [spelled for _, spelled, _, _ in tokens]
    return _Tokenized(
        f" {_SEPARATOR.join(lows)} ",
        f" {_SEPARATOR.join(spelled)} ",
        _haystack_offsets(np.fromiter(map(len, lows), np.int64, len(lows))),
        _haystack_offsets(np.fromiter(map(len, spelled), np.int64, len(spelled))),
        np.array([start for _, _, start, _ in tokens], dtype=np.int64),
        np.array([end for _, _, _, end in tokens], dtype=np.int64),
    )


def _tokens(text: str) -> list[tuple[str, str, int, int]]:
    """Tokenize text by the label_key rule, keeping where each token came from.

    For text with a letter or number, " ".join of the lowercase tokens equals
    label_key(text); text without one has no key but keeps its tokens. A symbol
    spelled out becomes its own token ("plus"), and a word whose lowercase
    form splits ("İ" lowers to "i" and a combining dot) gives each part the whole
    word's span.

    :param text: any text
    :returns: (lowercase token, token as spelled, char_start, char_end) per token
    :rtype: list[tuple[str, str, int, int]]
    """
    tokens = []
    for match in _TEXT_ITEM.finditer(text):
        item, start = match.group(), match.start()
        # a symbol right after whitespace or at the start is a separator, and so is
        # one right after a leading one, as the macro's first two passes leave it
        after_space = start == 0 or text[start - 1] in _WHITESPACE
        if item in _SPELLED_SYMBOLS:
            if not (after_space or (start == 1 and text[0] in _SPELLED_SYMBOLS)):
                word = _SPELLED_SYMBOLS[item]
                tokens.append((word, word, start, start + 1))
        elif item == ".":
            # spelled symbols, separators and a leading "dot " all leave a space
            if (
                after_space
                or text[start - 1] in _SPELLED_SYMBOLS
                or (start == 1 and text[0] == ".")
            ):
                tokens.append(("dot", "dot", start, start + 1))
        elif item.isascii():
            tokens.append((item.lower(), item, start, match.end()))
        else:
            lowered = _WORD.findall(item.lower())
            spelled = [item] if len(lowered) == 1 else lowered
            tokens.extend(
                (low, spell, start, match.end())
                for low, spell in zip(lowered, spelled, strict=True)
            )
    return tokens


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
            _is_acronym(entry.surface.removesuffix("s")) for entry in self.entries
        )

    @property
    def is_single_word(self) -> bool:
        """The phrase is one common word, or the plural of one, in any case."""
        return self.token_count == 1 and any(
            (entry.base_key or self.key) in COMMON_WORDS for entry in self.entries
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
        return frozenset(
            tuple(spelled for _, spelled, _, _ in _tokens(entry.surface))
            for entry in self.entries
        )


def _is_acronym(surface: str) -> bool:
    # any lowercase letter counts, not only a-z, so "NAïVE" is not an acronym
    return any(char.isupper() for char in surface) and not any(
        char.islower() for char in surface
    )


def _plural(surface: str) -> str | None:
    words = _WORD.findall(surface)
    if not words:
        return None
    last = words[-1]
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
        inner.strip() for inner in _PARENTHETICAL.findall(alias) if _WORD.search(inner)
    ]
    head = " ".join(_PARENTHETICAL.sub(" ", alias).split())
    if parentheticals and _WORD.search(head):
        abbreviations = [
            inner
            for inner in parentheticals
            if len(_WORD.findall(inner)) == 1 and _abbreviates(inner, head)
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
        # a label with no letter or number has a null key since courseware_label_key
        rows = sorted(
            aliases.select("concept_label_key", "concept_fk", "alias").iter_rows(),
            key=lambda row: (row[0] or "", row[1], row[2]),
        )
        explicit = {key for key, _, _ in rows if key is not None}
        self.label_count = len(explicit)
        self.tokenless_label_count = 0
        by_phrase: dict[str, set[Entry]] = defaultdict(set)
        for key, concept_id, alias in rows:
            # An alias with no letter or number ("∇", "." or ".+") has nothing to match
            if key is None:
                self.tokenless_label_count += 1
                continue
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
        tokenized = _tokenize(text)
        if tokenized is None:
            return []
        # (first token, last token, pattern)
        candidates: list[tuple[int, int, Pattern]] = []
        for patterns, haystack, offsets, automaton in (
            (
                self._caseless,
                tokenized.caseless,
                tokenized.caseless_offsets,
                self._caseless_automaton,
            ),
            (
                self._exact,
                tokenized.exact,
                tokenized.exact_offsets,
                self._exact_automaton,
            ),
        ):
            matches = automaton.find_matches_as_indexes(haystack, overlapping=True)
            if not matches:
                continue
            # a match starts on the separator before its first token
            match_starts = np.fromiter(
                (start for _, start, _ in matches), np.int64, len(matches)
            )
            firsts = np.searchsorted(offsets, match_starts + 1).tolist()
            for (index, _start, _end), first in zip(matches, firsts, strict=True):
                pattern, token_count = patterns[index]
                candidates.append((first, first + token_count - 1, pattern))
        # leftmost-longest over every candidate
        candidates.sort(key=lambda candidate: (candidate[0], -candidate[1]))
        found = []
        covered_until = -1
        for first, last, pattern in candidates:
            if first > covered_until:
                found.append(
                    (pattern, int(tokenized.starts[first]), int(tokenized.ends[last]))
                )
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
    """One tagging run: the mentions of the sources it tagged, which run holds each
    current source's mentions, the dictionary as matched (one row per phrase and
    concept, with its mode and flags), and the run record.
    """

    mentions: pl.DataFrame
    sources: pl.DataFrame
    aliases: pl.DataFrame
    run: pl.DataFrame


@dataclass(frozen=True)
class PreviousRun:
    """The newest stored run, and its courseware_concept_tagging_source rows."""

    run: dict[str, Any]
    sources: pl.DataFrame


def newest_run(runs: pl.DataFrame | None) -> dict[str, Any] | None:
    """Return the newest courseware_concept_tagging_run row, or None for no run.

    :param runs: the stored runs, or None before the first
    :rtype: dict[str, Any] | None
    """
    if runs is None or runs.is_empty():
        return None
    return runs.sort("tagged_at").row(-1, named=True)


def _parameters() -> str:
    return json.dumps(
        {
            "min_alias_chars": MIN_ALIAS_CHARS,
            "case_sensitive_below_chars": CASE_SENSITIVE_BELOW_CHARS,
            "plural_min_word_chars": PLURAL_MIN_WORD_CHARS,
        },
        sort_keys=True,
    )


def _source_checksums(sources: pl.DataFrame) -> pl.Series:
    return pl.Series(
        "source_checksum",
        [
            hashlib.sha256(json.dumps(row).encode()).hexdigest()
            for row in sources.select(SOURCE_COLUMNS).iter_rows()
        ],
        dtype=pl.String,
    )


def _same_tagger(run: dict[str, Any], dictionary: ConceptDictionary) -> bool:
    """Whether a run would tag any text exactly as this code and dictionary do."""
    return (
        run["tagger_id"] == TAGGER_ID
        and run["parameters_json"] == _parameters()
        and run["dictionary_checksum"] == dictionary.checksum
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


def tag_sources(
    sources: pl.DataFrame,
    aliases: pl.DataFrame,
    previous: PreviousRun | None = None,
) -> TaggingResult:
    """Tag the sources no stored run holds current mentions for.

    A source keeps the mention run its record in the previous run names when its
    checksum is unchanged and the previous run had this dictionary, TAGGER_ID and
    parameters; every other source is tagged. Without a previous run every source
    is tagged.

    :param sources: int__courseware__extraction_sources rows (SOURCE_COLUMNS)
    :param aliases: dim_concept_alias rows (concept_label_key, concept_fk, alias)
    :param previous: the newest stored run and its source records
    :returns: the run's mentions, source records, alias table and run record
    :rtype: TaggingResult
    """
    tagged_at = datetime.now(tz=UTC)
    began = time.perf_counter()
    dictionary = ConceptDictionary(aliases)
    run_id = str(uuid.uuid4())
    sources_checksum = _sources_checksum(sources)
    checked = sources.select(SOURCE_COLUMNS).with_columns(_source_checksums(sources))
    if previous is not None and _same_tagger(previous.run, dictionary):
        checked = checked.join(
            previous.sources.select(*SOURCE_KEY, "source_checksum", "mention_run_id"),
            on=[*SOURCE_KEY, "source_checksum"],
            how="left",
        )
    else:
        checked = checked.with_columns(mention_run_id=pl.lit(None, dtype=pl.String))
    to_tag = checked.filter(pl.col("mention_run_id").is_null())
    carried = checked.height - to_tag.height
    source_records = checked.select(
        pl.lit(run_id).alias("tagging_run_id"),
        *SOURCE_KEY,
        "source_checksum",
        pl.col("mention_run_id").fill_null(run_id),
    )

    mentions: list[dict[str, object]] = []
    tagged_sources = 0
    char_count = 0
    started = time.perf_counter()
    for row in to_tag.select(SOURCE_COLUMNS).iter_rows(named=True):
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
        # None when no source's mentions were carried forward
        "previous_tagging_run_id": previous.run["tagging_run_id"]
        if previous is not None and carried
        else None,
        "input_key": _input_key(dictionary.checksum, sources_checksum),
        "tagger_id": TAGGER_ID,
        "parameters_json": _parameters(),
        "dictionary_checksum": dictionary.checksum,
        "sources_checksum": sources_checksum,
        "label_count": dictionary.label_count,
        "tokenless_label_count": dictionary.tokenless_label_count,
        "pattern_count": len(dictionary.patterns),
        "case_sensitive_pattern_count": modes.count("case_sensitive"),
        "skipped_pattern_count": modes.count("skipped"),
        "ambiguous_pattern_count": sum(p.is_ambiguous for p in dictionary.patterns),
        "source_count": sources.height,
        "retagged_source_count": to_tag.height,
        "char_count": char_count,
        "tagged_source_count": tagged_sources,
        "mention_count": len(mentions),
        "match_seconds": match_seconds,
        "elapsed_seconds": time.perf_counter() - began,
        "tagged_at": tagged_at,
    }
    return TaggingResult(
        mentions=mention_frame,
        sources=source_records.cast(SOURCE_SCHEMA),
        aliases=alias_frame,
        run=pl.DataFrame([run], schema=RUN_SCHEMA),
    )

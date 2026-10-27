"""Check a quoted evidence passage against the source text the model was given.

The offsets are into the warehouse text (int__courseware__extraction_sources.
source_text), so a claim's evidence can be located from SQL without this code.
"""

import re
import unicodedata
from collections import Counter, defaultdict
from functools import lru_cache
from typing import Literal, NamedTuple

VerificationStatus = Literal["exact", "fuzzy", "unverified", "unknown_source"]

# share of a passage's word trigrams that must occur in the source for a near match
FUZZY_TRIGRAM_RATIO = 0.8
# below this many words one wrong word decides the match, so no near match is tried
FUZZY_MIN_WORDS = 6
# how far, in words, a near match may drift from the passage's own word order; each
# dropped, added or corrected word shifts what follows it by one
FUZZY_MAX_DRIFT_WORDS = 3
_WORD = re.compile(r"\w+")


class PassageMatch(NamedTuple):
    status: VerificationStatus
    char_start: int | None = None
    char_end: int | None = None


class _Normalized(NamedTuple):
    text: str
    # for each character of text, the span of the original it came from
    starts: list[int]
    ends: list[int]


@lru_cache(maxsize=64)
def _normalize(text: str) -> _Normalized:
    """NFKC and casefold, one base character and its combining marks at a time.

    Normalizing in pieces keeps a map back to the original offsets, which a
    normalized copy of the whole text would lose when a character expands
    ("\u00df" to "ss") or a ligature splits.
    """
    pieces: list[str] = []
    starts: list[int] = []
    ends: list[int] = []
    start = 0
    for end in range(1, len(text) + 1):
        if end < len(text) and unicodedata.combining(text[end]):
            continue
        piece = unicodedata.normalize("NFKC", text[start:end]).casefold()
        pieces.append(piece)
        starts.extend([start] * len(piece))
        ends.extend([end] * len(piece))
        start = end
    return _Normalized("".join(pieces), starts, ends)


def _words(text: str) -> list[str]:
    return _WORD.findall(_normalize(text).text)


def _span(passage_words: list[str], source: str) -> tuple[int, int] | None:
    """Find the passage's words in order in the source, ignoring everything between.

    The source is normalized the same way as the passage, and the match is mapped
    back so the offsets index the original source_text.
    """
    pattern = r"\b" + r"\W+".join(re.escape(word) for word in passage_words) + r"\b"
    normalized = _normalize(source)
    match = re.search(pattern, normalized.text)
    if not match:
        return None
    return normalized.starts[match.start()], normalized.ends[match.end() - 1]


def _near_span(passage_words: list[str], source: str) -> tuple[int, int] | None:
    """Find one stretch of the source holding most of the passage's word trigrams.

    Trigram i of the passage found at source word p lies on diagonal p - i. A near
    quote keeps its trigrams within FUZZY_MAX_DRIFT_WORDS of one diagonal; a quote
    stitched together from distant sentences spreads them over several, so no one
    stretch of the source holds enough of them.
    """
    normalized = _normalize(source)
    words = list(_WORD.finditer(normalized.text))
    positions: defaultdict[tuple[str, ...], list[int]] = defaultdict(list)
    for index in range(len(words) - 2):
        positions[tuple(word.group() for word in words[index : index + 3])].append(
            index
        )
    wanted = [
        tuple(passage_words[index : index + 3])
        for index in range(len(passage_words) - 2)
    ]
    hits = sorted(
        (position - index, index, position)
        for index, trigram in enumerate(wanted)
        for position in positions.get(trigram, ())
    )
    found: Counter[int] = Counter()
    best = (0, 0, 0)
    first = 0
    for last, (diagonal, index, _) in enumerate(hits):
        found[index] += 1
        while hits[first][0] < diagonal - 2 * FUZZY_MAX_DRIFT_WORDS:
            found[hits[first][1]] -= 1
            if not found[hits[first][1]]:
                del found[hits[first][1]]
            first += 1
        if len(found) > best[0]:
            window = [position for *_, position in hits[first : last + 1]]
            best = (len(found), min(window), max(window))
    count, start_word, end_word = best
    if count / len(wanted) < FUZZY_TRIGRAM_RATIO:
        return None
    # the last trigram ends two words after the word it starts on
    return (
        normalized.starts[words[start_word].start()],
        normalized.ends[words[end_word + 2].end() - 1],
    )


def verify_passage(passage: str, source: str | None) -> PassageMatch:
    """Classify how a passage matches its source.

    exact: every word of the passage occurs in order in the source, differing only in
    case, punctuation or whitespace. fuzzy: at least FUZZY_TRIGRAM_RATIO of its word
    trigrams occur in one stretch of the source, in about the passage's order, which
    tolerates a dropped or corrected word. unverified: neither. Both matches return
    the span of the source they found.

    :param passage: the passage the model quoted
    :param source: the text of the source it named, None when the id was not sent
    :returns: the match status and, for exact and fuzzy matches, character offsets
    :rtype: PassageMatch
    """
    if source is None:
        return PassageMatch("unknown_source")
    passage_words = _words(passage)
    if not passage_words:
        return PassageMatch("unverified")
    span = _span(passage_words, source)
    if span:
        return PassageMatch("exact", *span)
    if len(passage_words) < FUZZY_MIN_WORDS:
        return PassageMatch("unverified")
    near = _near_span(passage_words, source)
    if near:
        return PassageMatch("fuzzy", *near)
    return PassageMatch("unverified")

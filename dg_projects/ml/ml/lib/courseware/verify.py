"""Check a quoted evidence passage against the source text the model was given.

The offsets are into the warehouse text (int__courseware__extraction_sources.
source_text), so a claim's evidence can be located from SQL without this code.
"""

import re
import unicodedata
from typing import Literal, NamedTuple

VerificationStatus = Literal["exact", "fuzzy", "unverified", "unknown_source"]

# share of a passage's word trigrams that must occur in the source for a near match
FUZZY_TRIGRAM_RATIO = 0.8
# below this many words one wrong word decides the match, so no near match is tried
FUZZY_MIN_WORDS = 6
_WORD = re.compile(r"\w+")


class PassageMatch(NamedTuple):
    status: VerificationStatus
    char_start: int | None = None
    char_end: int | None = None


def _words(text: str) -> list[str]:
    return _WORD.findall(unicodedata.normalize("NFKC", text).casefold())


def _span(passage_words: list[str], source: str) -> tuple[int, int] | None:
    """Find the passage's words in order in the source, ignoring everything between."""
    pattern = r"\W+".join(re.escape(word) for word in passage_words)
    # the original text, not a normalized copy, so the offsets index source_text
    match = re.search(pattern, source, re.IGNORECASE)
    return (match.start(), match.end()) if match else None


def _trigrams(words: list[str]) -> set[tuple[str, ...]]:
    return {tuple(words[index : index + 3]) for index in range(len(words) - 2)}


def verify_passage(passage: str, source: str | None) -> PassageMatch:
    """Classify how a passage matches its source.

    exact: every word of the passage occurs in order in the source, differing only in
    case, punctuation or whitespace, and the span is returned. fuzzy: at least
    FUZZY_TRIGRAM_RATIO of its word trigrams occur in the source, which tolerates a
    dropped or corrected word. unverified: neither.

    :param passage: the passage the model quoted
    :param source: the text of the source it named, None when the id was not sent
    :returns: the match status and, for exact matches, character offsets
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
    wanted = _trigrams(passage_words)
    found = wanted & _trigrams(_words(source))
    if len(found) / len(wanted) >= FUZZY_TRIGRAM_RATIO:
        return PassageMatch("fuzzy")
    return PassageMatch("unverified")

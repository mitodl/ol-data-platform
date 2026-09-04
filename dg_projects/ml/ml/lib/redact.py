"""Presidio-based PII redaction for feedback title/text."""

import polars as pl
from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
from presidio_anonymizer import AnonymizerEngine

JOIN_COLS = ["source_slug", "source_record_ref"]

EXCLUDED_ENTITIES = {"DATE_TIME", "URL"}

# The only types that must be redacted even when fully contained in a URL/date span
# (e.g. a reset link's ?email=... query param) -- real PII someone could paste into
# feedback text. Everything else stays exempted: pattern recognizers for driver's
# license/passport/bank numbers etc. routinely false-positive on UUIDs, order
# numbers, and course IDs, which are common and harmless inside a URL.
ALWAYS_REDACT_EVEN_IN_URL = {"EMAIL_ADDRESS", "PHONE_NUMBER"}

# Presidio's built-in EmailRecognizer's local-part character class includes URL
# delimiters (/ ? = &), so an email right after a URL's domain/path (e.g. a reset
# link's ?email=jane.doe@mit.edu) matches starting from the URL itself, redacting
# the whole URL along with the email. This tighter pattern stops at the first '@'.
_STRICT_EMAIL_PATTERN = Pattern(
    name="strict_email", regex=r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b", score=0.85
)
_STRICT_EMAIL_RECOGNIZER = PatternRecognizer(
    supported_entity="EMAIL_ADDRESS", patterns=[_STRICT_EMAIL_PATTERN]
)

_analyzer: AnalyzerEngine | None = None
_anonymizer: AnonymizerEngine | None = None


def _get_analyzer() -> AnalyzerEngine:
    # Built lazily, not at import time: constructing it loads the spaCy model,
    # which is only present once the Dockerfile's `spacy download` step has run
    # (it is not a pip dependency), so importing this module must not require it.
    global _analyzer  # noqa: PLW0603
    if _analyzer is None:
        _analyzer = AnalyzerEngine()
        _analyzer.registry.remove_recognizer("EmailRecognizer")
        _analyzer.registry.add_recognizer(_STRICT_EMAIL_RECOGNIZER)
    return _analyzer


def _get_anonymizer() -> AnonymizerEngine:
    global _anonymizer  # noqa: PLW0603
    if _anonymizer is None:
        _anonymizer = AnonymizerEngine()
    return _anonymizer


def _redact_text(value: str | None) -> str | None:
    if value is None:
        return None
    # Analyze with the full entity set rather than restricting `entities` to exclude
    # EXCLUDED_ENTITIES: a URL recognizer that never runs can't protect its span from
    # an overlapping NER false positive (e.g. spaCy tagging a URL path as PERSON,
    # often with higher confidence than the URL match itself). So detect everything,
    # then drop any result overlapping an excluded-entity span, not just that
    # entity's own result.
    results = _get_analyzer().analyze(text=value, language="en")
    excluded_spans = [
        (result.start, result.end)
        for result in results
        if result.entity_type in EXCLUDED_ENTITIES
    ]
    # Containment, not any overlap: a PII match that only partially overlaps an
    # excluded span (e.g. a name immediately followed by a URL) extends beyond it
    # and is still real PII outside that span, so it's never exempted regardless
    # of type. A match fully inside the span is only redacted if its type is in
    # ALWAYS_REDACT_EVEN_IN_URL -- everything else (including a URL/date span's own
    # NER false positives) is exempted.
    filtered_results = [
        result
        for result in results
        if result.entity_type not in EXCLUDED_ENTITIES
        and not (
            result.entity_type not in ALWAYS_REDACT_EVEN_IN_URL
            and any(
                start <= result.start and result.end <= end
                for start, end in excluded_spans
            )
        )
    ]
    return (
        _get_anonymizer().anonymize(text=value, analyzer_results=filtered_results).text
    )


def filter_unredacted(
    source_df: pl.DataFrame, already_redacted_df: pl.DataFrame
) -> pl.DataFrame:
    """Drop rows already present in the feedback_redacted output."""
    return source_df.join(
        already_redacted_df.select(JOIN_COLS), on=JOIN_COLS, how="anti"
    )


def redact_titles_and_text(df: pl.DataFrame) -> pl.DataFrame:
    """Mask PII in the title/text columns of a feedback frame.

    Args:
        df: a frame with (at least) source_slug, source_record_ref, title, text
            columns, e.g. int__feedback__unioned.

    Returns:
        pl.DataFrame: source_slug, source_record_ref, title_redacted, text_redacted -
            keyed the same way feedback_pk is minted, for tfact_feedback to left-join.
    """
    return df.select(
        pl.col("source_slug"),
        pl.col("source_record_ref"),
        pl.col("title")
        .map_elements(_redact_text, return_dtype=pl.String)
        .alias("title_redacted"),
        pl.col("text")
        .map_elements(_redact_text, return_dtype=pl.String)
        .alias("text_redacted"),
    )

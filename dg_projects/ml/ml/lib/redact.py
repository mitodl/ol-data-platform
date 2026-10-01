"""Presidio-based PII redaction for feedback title/text."""

import re

import polars as pl
from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
from presidio_anonymizer import AnonymizerEngine

JOIN_COLS = ["source_slug", "source_record_ref"]

EXCLUDED_ENTITIES = {"DATE_TIME", "URL"}

# Allowlist, not a denylist: the English NER model tags ordinary words in non-English
# text as LOCATION/NRP, and the national-ID patterns match problem numbers like
# "PS2.3.5". Neither identifies a learner, so only these types are masked.
REDACTED_ENTITIES = {
    "PERSON",
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "CREDIT_CARD",
    "IBAN_CODE",
    "US_BANK_NUMBER",
    "IP_ADDRESS",
    "US_SSN",
    "STREET_ADDRESS",
}

# US_BANK_NUMBER is any 8-17 digit run (score 0.05), which in feedback matched ticket
# IDs, meeting IDs and decimals. Presidio raises it to 0.4 only next to a context word
# like "account" or "bank", so mask it only then.
MIN_SCORE_BY_ENTITY = {"US_BANK_NUMBER": 0.4}

_US_STATES = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|"
    "MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|PR"
)
_STREET_TYPES = (
    "Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|"
    "Place|Pl|Square|Sq|Terrace|Parkway|Pkwy|Highway|Hwy"
)
_LATAM_STREET_TYPES = "Calle|Carrera|Avenida|Diagonal|Transversal|Cra|Cl|Av"
# Presidio has no address recognizer; LOCATION only ever caught the city, leaving the
# street and ZIP that place someone. Case-sensitive (Presidio defaults to IGNORECASE)
# so "ma 02139" or "3 ways st" in prose don't match.
_STREET_ADDRESS_RECOGNIZER = PatternRecognizer(
    supported_entity="STREET_ADDRESS",
    patterns=[
        Pattern(
            name="street_line",
            regex=rf"\b\d{{1,6}}[A-Z]?(?:\s+[A-Z][\w'.-]*){{1,4}}\s+(?:{_STREET_TYPES})\b\.?",
            score=0.85,
        ),
        Pattern(
            name="latam_street_line",
            regex=rf"\b(?:{_LATAM_STREET_TYPES})\.?\s+\d+[A-Z]?\s*(?:#|No\.?)\s*\d+[A-Z]?(?:\s*-\s*\d+)?",
            score=0.85,
        ),
        Pattern(
            name="us_state_zip",
            regex=rf"\b(?:{_US_STATES})\s+\d{{5}}(?:-\d{{4}})?\b",
            score=0.85,
        ),
        Pattern(
            name="uk_postcode",
            regex=r"\b[A-Z]{1,2}\d[A-Z\d]?\s+\d[A-Z]{2}\b",
            score=0.85,
        ),
    ],
    global_regex_flags=re.MULTILINE,
)

# The only types that must be redacted even when fully contained in a URL/date span
# (e.g. a reset link's ?email=... query param) -- real PII someone could paste into
# feedback text. Everything else stays exempted: a NER or pattern match inside a URL
# is usually a false positive on a path segment, UUID, or course ID. Financial types
# are listed because spaCy tags a bare account number as DATE_TIME, and they already
# need a checksum or a context word to match.
ALWAYS_REDACT_EVEN_IN_URL = {
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "CREDIT_CARD",
    "IBAN_CODE",
    "US_BANK_NUMBER",
}

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
        _analyzer.registry.add_recognizer(_STREET_ADDRESS_RECOGNIZER)
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
        if result.entity_type in REDACTED_ENTITIES
        and result.score >= MIN_SCORE_BY_ENTITY.get(result.entity_type, 0)
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

"""Presidio-based PII redaction for feedback title/text."""

import os
import re

import polars as pl
from dagster import get_dagster_logger
from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
from presidio_anonymizer import AnonymizerEngine
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import And, BooleanExpression, EqualTo, In, Or

JOIN_COLS = ["source_slug", "source_record_ref"]

# Bump when the masking rules or the upstream text change. A full refresh
# re-redacts only rows stored with another version, so it resumes after a crash.
REDACTION_VERSION = "v3"

# Rows redacted and written together. A crash loses at most one batch. Each write
# scans the table once (~10s at 600k rows), so larger batches are cheaper.
REDACT_CHECKPOINT_BATCH_SIZE = int(
    os.environ.get("REDACT_CHECKPOINT_BATCH_SIZE", "20000")
)

REDACTED_SCHEMA = {
    "source_slug": pl.String,
    "source_record_ref": pl.String,
    "title_redacted": pl.String,
    "text_redacted": pl.String,
    "redaction_version": pl.String,
}

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

# US_BANK_NUMBER (any 8-17 digit run) and US_SSN's weak patterns (any 9 digits) score
# 0.05; in feedback they matched ticket IDs, tracking params, ZIP+4 codes and decimals.
# Presidio raises them to 0.4 only next to a context word ("account", "ssn"), and a
# formatted SSN like 234-56-7890 scores 0.5, so mask only at 0.4 and up.
MIN_SCORE_BY_ENTITY = {"US_BANK_NUMBER": 0.4, "US_SSN": 0.4}

_US_STATES = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|DC|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|"
    "MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|PR"
)
_STREET_TYPES = (
    "Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Way|Court|Ct|"
    "Place|Pl|Square|Sq|Terrace|Parkway|Pkwy|Highway|Hwy"
)
# State codes that are also common words (in, or, me, de...) only match in uppercase.
_LOWERCASE_US_STATES = "|".join(
    s
    for s in _US_STATES.split("|")
    if s not in {"AL", "DE", "HI", "ID", "IN", "LA", "ME", "OH", "OK", "OR", "PA"}
)
_LATAM_STREET_TYPES = "Calle|Carrera|Avenida|Diagonal|Transversal|Cra|Cl|Av"
# Lowercase matching uses only street types that are rare in prose, and needs a
# delimiter after them: "77 massachusetts ave, ..." but not "12 videos on the way".
_LOWERCASE_STREET_TYPES = (
    "street|st|avenue|ave|road|rd|boulevard|blvd|lane|ln|parkway|pkwy|highway|hwy|"
    "terrace"
)
# Presidio has no address recognizer; LOCATION only ever caught the city, leaving the
# street and ZIP that place someone. Case-sensitive by default (Presidio defaults to
# IGNORECASE) so "in 12345 cases" doesn't match; the lowercase patterns need a stronger
# signal instead: a delimiter after the street type, or a comma before the state.
_STREET_ADDRESS_RECOGNIZER = PatternRecognizer(
    supported_entity="STREET_ADDRESS",
    patterns=[
        Pattern(
            name="street_line",
            regex=rf"\b\d{{1,6}}[A-Z]?(?:\s+[A-Z][\w'.-]*){{1,4}}\s+(?:{_STREET_TYPES})\b\.?",
            score=0.85,
        ),
        Pattern(
            name="lowercase_street_line",
            regex=(
                rf"(?i:\b\d{{1,6}}[a-z]?(?:\s+[a-z][\w'.-]*){{1,4}}\s+"
                rf"(?:{_LOWERCASE_STREET_TYPES})\b\.?"
                rf"(?=\s*(?:,|#|apt\b|suite\b|unit\b|$)))"
            ),
            score=0.85,
        ),
        Pattern(
            name="latam_street_line",
            regex=(
                rf"(?i:\b(?:{_LATAM_STREET_TYPES})\.?\s+\d+[a-z]?\s*(?:#|No\.?)"
                rf"\s*\d+[a-z]?(?:\s*-\s*\d+)?)"
            ),
            score=0.85,
        ),
        Pattern(
            name="us_state_zip",
            regex=rf"\b(?:{_US_STATES})\s+\d{{5}}(?:-\d{{4}})?\b",
            score=0.85,
        ),
        Pattern(
            name="lowercase_us_state_zip",
            regex=rf"(?<=,\s)(?i:(?:{_LOWERCASE_US_STATES})\s+\d{{5}}(?:-\d{{4}})?\b)",
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
# is usually a false positive on a path segment, UUID, or course ID. Financial and
# SSN types are listed because spaCy tags a bare account number as DATE_TIME, and
# they already need a checksum, a format or a context word to match.
ALWAYS_REDACT_EVEN_IN_URL = {
    "EMAIL_ADDRESS",
    "PHONE_NUMBER",
    "CREDIT_CARD",
    "IBAN_CODE",
    "US_BANK_NUMBER",
    "US_SSN",
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
    # Built lazily, not at import time: constructing it loads the large spaCy
    # model, so loading Dagster definitions or tests stays fast.
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
        pl.DataFrame: source_slug, source_record_ref, title_redacted, text_redacted,
            redaction_version - keyed the same way feedback_pk is minted, for
            tfact_feedback to left-join.
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
        pl.lit(REDACTION_VERSION).alias("redaction_version"),
    )


def _key_filter(chunk_df: pl.DataFrame) -> BooleanExpression:
    # Not pyiceberg's upsert: it ORs one (slug AND ref) pair per row, and at 5,000
    # rows its scan ran for over 19 minutes. One IN list per source takes ~10s.
    parts: list[BooleanExpression] = [
        And(EqualTo("source_slug", slug), In("source_record_ref", refs))
        for slug, refs in chunk_df.group_by("source_slug")
        .agg(pl.col("source_record_ref"))
        .iter_rows()
    ]
    return parts[0] if len(parts) == 1 else Or(*parts)


def checkpoint_redacted_chunk(
    catalog: Catalog, table_identifier: str, chunk_df: pl.DataFrame
) -> None:
    """Replace one redacted chunk's rows in the feedback_redacted table."""
    if chunk_df.height == 0:
        return
    if chunk_df.select(JOIN_COLS).is_duplicated().any():
        msg = f"Duplicate {JOIN_COLS} in a redacted chunk; overwrite would keep both"
        raise ValueError(msg)
    table = catalog.create_table_if_not_exists(
        table_identifier, schema=chunk_df.to_arrow().schema
    )
    # Adds redaction_version to a table created before it existed.
    with table.update_schema() as update:
        update.union_by_name(chunk_df.to_arrow().schema)
    # The write casts by position, so match the table's column order.
    ordered_chunk_df = chunk_df.select([field.name for field in table.schema().fields])
    table.overwrite(
        df=ordered_chunk_df.to_arrow(), overwrite_filter=_key_filter(chunk_df)
    )


def redact_and_checkpoint(
    df: pl.DataFrame,
    checkpoint_target: tuple[Catalog, str],
    batch_size: int = REDACT_CHECKPOINT_BATCH_SIZE,
) -> int:
    """Redact df in chunks, writing each as it completes. Returns rows written."""
    log = get_dagster_logger()
    catalog, table_identifier = checkpoint_target
    chunk_starts = range(0, df.height, batch_size)
    written = 0
    for chunk_index, chunk_start in enumerate(chunk_starts, start=1):
        chunk_df = redact_titles_and_text(df.slice(chunk_start, batch_size))
        checkpoint_redacted_chunk(catalog, table_identifier, chunk_df)
        written += chunk_df.height
        log.info(
            "Wrote chunk %d/%d (%d rows) to %s",
            chunk_index,
            len(chunk_starts),
            chunk_df.height,
            table_identifier,
        )
    return written

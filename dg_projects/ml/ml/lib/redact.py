"""Presidio-based PII redaction for feedback title/text."""

import polars as pl
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine

JOIN_COLS = ["source_slug", "source_record_ref"]

EXCLUDED_ENTITIES = {"DATE_TIME", "URL"}

_analyzer: AnalyzerEngine | None = None
_anonymizer: AnonymizerEngine | None = None
_redact_entities: list[str] | None = None


def _get_analyzer() -> AnalyzerEngine:
    # Built lazily, not at import time: constructing it loads the spaCy model,
    # which is only present once the Dockerfile's `spacy download` step has run
    # (it is not a pip dependency), so importing this module must not require it.
    global _analyzer  # noqa: PLW0603
    if _analyzer is None:
        _analyzer = AnalyzerEngine()
    return _analyzer


def _get_anonymizer() -> AnonymizerEngine:
    global _anonymizer  # noqa: PLW0603
    if _anonymizer is None:
        _anonymizer = AnonymizerEngine()
    return _anonymizer


def _get_redact_entities() -> list[str]:
    global _redact_entities  # noqa: PLW0603
    if _redact_entities is None:
        _redact_entities = [
            entity
            for entity in _get_analyzer().get_supported_entities("en")
            if entity not in EXCLUDED_ENTITIES
        ]
    return _redact_entities


def _redact_text(value: str | None) -> str | None:
    if value is None:
        return None
    results = _get_analyzer().analyze(
        text=value, language="en", entities=_get_redact_entities()
    )
    return _get_anonymizer().anonymize(text=value, analyzer_results=results).text


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

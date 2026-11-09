"""Presidio PII redaction for text columns, and a factory for keyed redaction assets."""

import os
import re

import polars as pl
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    Config,
    MetadataValue,
    asset,
    get_dagster_logger,
)
from ml.lib.warehouse import read_table, warehouse_database
from ol_orchestrate.lib.automation_policies import upstream_or_code_changes
from ol_orchestrate.lib.glue_helper import get_dbt_model_as_dataframe
from ol_orchestrate.lib.iceberg_maintenance import get_glue_catalog
from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
from presidio_anonymizer import AnonymizerEngine
from pydantic import Field
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import And, BooleanExpression, EqualTo, In, Or

# The feedback_redacted defaults; other assets pass their own key and text columns.
JOIN_COLS = ["source_slug", "source_record_ref"]
TEXT_COLS = ["title", "text"]

# Bump when the masking rules or the upstream text change. A full refresh
# re-redacts only rows stored with another version, so it resumes after a crash.
REDACTION_VERSION = "v3"

# Rows redacted and written together. A crash loses at most one batch. Each write
# scans the table once (~10s at 600k rows), so larger batches are cheaper.
REDACT_CHECKPOINT_BATCH_SIZE = int(
    os.environ.get("REDACT_CHECKPOINT_BATCH_SIZE", "20000")
)

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
    source_df: pl.DataFrame,
    already_redacted_df: pl.DataFrame,
    key_cols: list[str] = JOIN_COLS,
) -> pl.DataFrame:
    """Drop rows already present in the redacted output."""
    return source_df.join(already_redacted_df.select(key_cols), on=key_cols, how="anti")


def redact_columns(
    df: pl.DataFrame,
    key_cols: list[str] = JOIN_COLS,
    text_cols: list[str] = TEXT_COLS,
) -> pl.DataFrame:
    """Mask PII in the text columns of a frame.

    Args:
        df: a frame with (at least) the key and text columns.
        key_cols: the columns that identify a row, kept as they are.
        text_cols: the columns to redact. Each becomes <column>_redacted.

    Returns:
        pl.DataFrame: the key columns, one <column>_redacted per text column, and
            redaction_version.
    """
    return df.select(
        *key_cols,
        *(
            pl.col(column)
            .map_elements(_redact_text, return_dtype=pl.String)
            .alias(f"{column}_redacted")
            for column in text_cols
        ),
        pl.lit(REDACTION_VERSION).alias("redaction_version"),
    )


def _key_filter(
    chunk_df: pl.DataFrame, key_cols: list[str] = JOIN_COLS
) -> BooleanExpression:
    # Not pyiceberg's upsert: it ORs one (slug AND ref) pair per row, and at 5,000
    # rows its scan ran for over 19 minutes. One IN list per group takes ~10s.
    *group_cols, last_col = key_cols
    if not group_cols:
        return In(last_col, chunk_df[last_col].to_list())
    parts: list[BooleanExpression] = []
    for row in chunk_df.group_by(group_cols).agg(pl.col(last_col)).iter_rows():
        *group_values, values = row
        condition: BooleanExpression = In(last_col, values)
        for column, value in zip(group_cols, group_values, strict=True):
            condition = And(EqualTo(column, value), condition)
        parts.append(condition)
    return parts[0] if len(parts) == 1 else Or(*parts)


def checkpoint_redacted_chunk(
    catalog: Catalog,
    table_identifier: str,
    chunk_df: pl.DataFrame,
    key_cols: list[str] = JOIN_COLS,
) -> None:
    """Replace one redacted chunk's rows in the redacted table."""
    if chunk_df.height == 0:
        return
    if chunk_df.select(key_cols).is_duplicated().any():
        msg = f"Duplicate {key_cols} in a redacted chunk; overwrite would keep both"
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
        df=ordered_chunk_df.to_arrow(),
        overwrite_filter=_key_filter(chunk_df, key_cols),
    )


def redact_and_checkpoint(
    df: pl.DataFrame,
    checkpoint_target: tuple[Catalog, str],
    batch_size: int = REDACT_CHECKPOINT_BATCH_SIZE,
    key_cols: list[str] = JOIN_COLS,
    text_cols: list[str] = TEXT_COLS,
) -> int:
    """Redact df in chunks, writing each as it completes. Returns rows written."""
    log = get_dagster_logger()
    catalog, table_identifier = checkpoint_target
    chunk_starts = range(0, df.height, batch_size)
    written = 0
    for chunk_index, chunk_start in enumerate(chunk_starts, start=1):
        chunk_df = redact_columns(
            df.slice(chunk_start, batch_size), key_cols, text_cols
        )
        checkpoint_redacted_chunk(catalog, table_identifier, chunk_df, key_cols)
        written += chunk_df.height
        log.info(
            "Wrote chunk %d/%d (%d rows) to %s",
            chunk_index,
            len(chunk_starts),
            chunk_df.height,
            table_identifier,
        )
    return written


class RedactedAssetConfig(Config):
    full_refresh: bool = Field(
        default=False,
        description=(
            "Re-redact every row not yet redacted with the current rules "
            "(REDACTION_VERSION in ml.lib.redact), not only rows missing from the "
            "table. Each batch is saved as it finishes, so a rerun after a crash "
            "continues where it stopped."
        ),
    )
    batch_size: int = Field(
        default=REDACT_CHECKPOINT_BATCH_SIZE,
        ge=1,
        description="Rows redacted and written together.",
    )
    sample_limit: int | None = Field(
        default=None,
        description="Cap the number of upstream rows read, for fast local testing.",
    )
    keys: dict[str, list[str]] | None = Field(
        default=None,
        description=(
            "Only process rows whose columns match these values, for spot-checking "
            'known cases, e.g. {"source_record_ref": ["123"], "source_slug": '
            '["zendesk"]}. Values are compared as strings.'
        ),
    )


def build_redacted_asset(  # noqa: PLR0913
    *,
    name: str,
    source: AssetKey,
    key_cols: list[str],
    text_cols: list[str],
    code_version: str,
    group_name: str,
    pool: str,
    source_filter: pl.Expr | None = None,
    description: str | None = None,
) -> AssetsDefinition:
    """Build an asset that writes <name> with each text column's PII masked.

    The output has the key columns, one <column>_redacted per text column, and
    redaction_version. A run redacts only rows missing from the output, and writes
    each batch as it finishes.

    Args:
        name: the asset and output table name, in the intermediate schema.
        source: the dbt model's asset key, [<layer>, <model>]; the asset reads the
            model from that layer's warehouse database.
        key_cols: the columns that identify a row. They must be unique and
            non-null. Put the column with the most distinct values last: the
            overwrite filter has one IN list per value of the other columns.
        text_cols: the columns to redact.
        code_version: the asset's code version.
        group_name: the Dagster asset group.
        pool: the Dagster concurrency pool.
        source_filter: keeps only the source rows to redact.
        description: the asset description.

    Returns:
        AssetsDefinition: the redaction asset.
    """
    source_layer, source_table = source.path
    database_name = warehouse_database("intermediate")

    @asset(
        name=name,
        key_prefix=["intermediate"],
        code_version=code_version,
        group_name=group_name,
        description=description,
        deps=[source],
        automation_condition=upstream_or_code_changes(),
        io_manager_key="io_manager",
        pool=pool,
        metadata={
            "schema": database_name,
            "write_mode": "upsert",
            "upsert_options": {"join_cols": key_cols},
            "schema_update_mode": "update",
        },
    )
    def _redacted(
        context: AssetExecutionContext, config: RedactedAssetConfig
    ) -> pl.DataFrame:
        source_lazy = get_dbt_model_as_dataframe(
            database_name=warehouse_database(source_layer), table_name=source_table
        )
        if source_filter is not None:
            source_lazy = source_lazy.filter(source_filter)
        for column, values in (config.keys or {}).items():
            source_lazy = source_lazy.filter(
                pl.col(column).cast(pl.String).is_in(values)
            )
        if config.sample_limit is not None:
            source_lazy = source_lazy.limit(config.sample_limit)
        source_df = source_lazy.collect()

        already_redacted_df = source_df.select(key_cols).clear()
        existing_lazy = read_table("intermediate", name)
        if existing_lazy is not None:
            if config.full_refresh:
                # Rows from before redaction_version existed have no version, so a
                # full refresh redoes them.
                current = (
                    pl.col("redaction_version") == REDACTION_VERSION
                    if "redaction_version" in existing_lazy.collect_schema().names()
                    else pl.lit(False)  # noqa: FBT003
                )
                existing_lazy = existing_lazy.filter(current)
            already_redacted_df = existing_lazy.select(key_cols).collect()

        unredacted_df = filter_unredacted(source_df, already_redacted_df, key_cols)
        redacted_count = redact_and_checkpoint(
            unredacted_df,
            (get_glue_catalog(), f"{database_name}.{name}"),
            batch_size=config.batch_size,
            key_cols=key_cols,
            text_cols=text_cols,
        )

        context.log.info(
            "Redacted %d rows (%d already redacted, %d total upstream)",
            redacted_count,
            already_redacted_df.height,
            source_df.height,
        )
        context.add_output_metadata(
            {
                "redacted_count": MetadataValue.int(redacted_count),
                "already_redacted_count": MetadataValue.int(already_redacted_df.height),
                "redaction_version": MetadataValue.text(REDACTION_VERSION),
                "full_refresh": MetadataValue.bool(config.full_refresh),
            }
        )
        # Every row is already written chunk by chunk. Returning them would make the
        # IO manager upsert all of them again in one write, the step that failed at
        # about 594k rows.
        return pl.DataFrame(
            schema={
                **source_df.select(key_cols).schema,
                **{f"{column}_redacted": pl.String for column in text_cols},
                "redaction_version": pl.String,
            }
        )

    return _redacted

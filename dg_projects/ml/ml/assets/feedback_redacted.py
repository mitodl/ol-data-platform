from dagster import AssetKey
from ml.lib.redact import JOIN_COLS, TEXT_COLS
from ml.lib.redacted_asset import build_redacted_asset

feedback_redacted = build_redacted_asset(
    name="feedback_redacted",
    source=AssetKey(["intermediate", "int__feedback__unioned"]),
    key_cols=JOIN_COLS,
    text_cols=TEXT_COLS,
    code_version="feedback_redacted_v5",
    group_name="feedback",
    pool="feedback_redacted",
    description="Mask PII in raw feedback title/text via Presidio.",
)

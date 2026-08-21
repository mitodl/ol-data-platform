"""Tests for ml.lib.redact.

Presidio's real AnalyzerEngine loads a spaCy model that is only present once the
Dockerfile's `spacy download` step has run, not via a pip dependency, so these
stub the analyzer/anonymizer rather than exercising the real NLP pipeline.
"""

import polars as pl
import pytest
from ml.lib import redact


class _Result:
    def __init__(self, text: str) -> None:
        self.text = text


_FAKE_SUPPORTED_ENTITIES = [
    "PERSON",
    "EMAIL_ADDRESS",
    "LOCATION",
    "PHONE_NUMBER",
    "DATE_TIME",
    "URL",
]


class _FakeAnalyzer:
    """Flags any text containing 'PII' as a single entity."""

    def get_supported_entities(self, language: str) -> list[str]:  # noqa: ARG002
        return _FAKE_SUPPORTED_ENTITIES

    def analyze(self, text: str, language: str, entities: list[str]) -> list[str]:  # noqa: ARG002
        return ["PII"] if "PII" in text else []


class _FakeAnonymizer:
    def anonymize(self, text: str, analyzer_results: list[str]) -> _Result:
        if analyzer_results:
            return _Result(text.replace("PII", "<REDACTED>"))
        return _Result(text)


@pytest.fixture
def fake_analyzer(monkeypatch: pytest.MonkeyPatch) -> _FakeAnalyzer:
    analyzer = _FakeAnalyzer()
    monkeypatch.setattr(redact, "_get_analyzer", lambda: analyzer)
    monkeypatch.setattr(redact, "_get_anonymizer", _FakeAnonymizer)
    monkeypatch.setattr(redact, "_redact_entities", None)
    return analyzer


@pytest.mark.usefixtures("fake_analyzer")
def test_redact_titles_and_text_masks_pii_and_keeps_the_join_key() -> None:
    """PII is masked, and source_slug/source_record_ref survive for the left-join."""
    df = pl.DataFrame(
        {
            "source_slug": ["zendesk"],
            "source_record_ref": ["123"],
            "title": ["Contact me at PII"],
            "text": ["The video is broken"],
        }
    )

    result = redact.redact_titles_and_text(df)

    row = result.row(0, named=True)
    assert row["source_slug"] == "zendesk"
    assert row["source_record_ref"] == "123"
    assert row["title_redacted"] == "Contact me at <REDACTED>"
    assert row["text_redacted"] == "The video is broken"


def test_filter_unredacted_drops_already_redacted_rows() -> None:
    """Only rows missing from the feedback_redacted output should get re-run."""
    source_df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "zendesk"],
            "source_record_ref": ["1", "2"],
            "title": ["already done", "new comment"],
            "text": ["already done", "new comment"],
        }
    )
    already_redacted_df = pl.DataFrame(
        {"source_slug": ["zendesk"], "source_record_ref": ["1"]}
    )

    result = redact.filter_unredacted(source_df, already_redacted_df)

    assert result["source_record_ref"].to_list() == ["2"]

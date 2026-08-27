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


class _AnalyzerResult:
    def __init__(self, entity_type: str, start: int, end: int) -> None:
        self.entity_type = entity_type
        self.start = start
        self.end = end


class _FakeAnalyzer:
    """Flags any text containing 'PII' as a PERSON entity."""

    def analyze(self, text: str, language: str) -> list[_AnalyzerResult]:  # noqa: ARG002
        if "PII" not in text:
            return []
        start = text.index("PII")
        return [_AnalyzerResult("PERSON", start, start + len("PII"))]


class _FakeAnonymizer:
    """Redacts each result's actual [start:end] span, not a hardcoded substring.

    A fake keyed on a literal like "PII" can't fail when the real bug is a result
    surviving filtering that it never mentions: replace by span so a leftover
    result always changes the output.
    """

    def anonymize(self, text: str, analyzer_results: list[_AnalyzerResult]) -> _Result:
        for result in sorted(analyzer_results, key=lambda r: r.start, reverse=True):
            placeholder = f"<{result.entity_type}>"
            text = text[: result.start] + placeholder + text[result.end :]
        return _Result(text)


@pytest.fixture
def fake_analyzer(monkeypatch: pytest.MonkeyPatch) -> _FakeAnalyzer:
    analyzer = _FakeAnalyzer()
    monkeypatch.setattr(redact, "_get_analyzer", lambda: analyzer)
    monkeypatch.setattr(redact, "_get_anonymizer", _FakeAnonymizer)
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
    assert row["title_redacted"] == "Contact me at <PERSON>"
    assert row["text_redacted"] == "The video is broken"


def test_redact_text_keeps_a_span_misclassified_as_person_when_also_a_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A URL a NER pass mistakes for a PERSON must not get redacted.

    Regression for a real bug: excluding URL from analyze()'s entities list stopped
    the URL recognizer from running at all, so an overlapping (and higher-confidence)
    PERSON false-positive on the same span had nothing to compete with.
    """
    text = "See https://micromasters.mit.edu/dedp/learners/ for details"
    url_start = text.index("https://")
    url_end = url_start + len("https://micromasters.mit.edu/dedp/learners/")

    class _OverlappingAnalyzer:
        def analyze(self, text: str, language: str) -> list[_AnalyzerResult]:  # noqa: ARG002
            return [
                _AnalyzerResult("PERSON", url_start, url_end),
                _AnalyzerResult("URL", url_start, url_end),
            ]

    monkeypatch.setattr(redact, "_get_analyzer", _OverlappingAnalyzer)
    monkeypatch.setattr(redact, "_get_anonymizer", _FakeAnonymizer)

    assert redact._redact_text(text) == text


def test_redact_text_still_redacts_pii_that_only_partially_overlaps_a_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A PII match extending beyond an excluded span must still be redacted.

    Regression: a one-character overlap with a URL/date span used to exempt the
    entire overlapping result, even when most of it was real PII outside that span.
    """
    text = "Contact Jane at https://example.com/jane for details"
    person_start = text.index("Jane")
    person_end = text.index("https") + len("https://example.com/jane")
    url_start = text.index("https")
    url_end = url_start + len("https://example.com/jane")

    class _PartiallyOverlappingAnalyzer:
        def analyze(self, text: str, language: str) -> list[_AnalyzerResult]:  # noqa: ARG002
            return [
                _AnalyzerResult("PERSON", person_start, person_end),
                _AnalyzerResult("URL", url_start, url_end),
            ]

    monkeypatch.setattr(redact, "_get_analyzer", _PartiallyOverlappingAnalyzer)
    monkeypatch.setattr(redact, "_get_anonymizer", _FakeAnonymizer)

    assert redact._redact_text(text) != text
    assert "<PERSON>" in redact._redact_text(text)


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

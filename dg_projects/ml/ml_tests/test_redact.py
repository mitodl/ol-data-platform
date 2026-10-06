"""Tests for ml.lib.redact.

Presidio's real AnalyzerEngine loads a large spaCy model, so these stub the
analyzer/anonymizer rather than exercising the real NLP pipeline.
"""

from typing import Any, Self

import polars as pl
import pyarrow as pa
import pytest
from ml.lib import redact
from pyiceberg.expressions.visitors import bind
from pyiceberg.io.pyarrow import expression_to_pyarrow
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType


class _Result:
    def __init__(self, text: str) -> None:
        self.text = text


class _AnalyzerResult:
    def __init__(
        self, entity_type: str, start: int, end: int, score: float = 0.85
    ) -> None:
        self.entity_type = entity_type
        self.start = start
        self.end = end
        self.score = score


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


def test_redact_text_still_redacts_an_email_and_phone_contained_in_a_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An email or phone number inside a URL (e.g. a reset link's ?email=...&
    phone=... query params) must still be redacted, on two fronts: the strict
    pattern matches only the email, not the URL text preceding it (Presidio's
    built-in EmailRecognizer regex allows URL delimiters / ? = & in the local
    part and would over-match), and _redact_text keeps both despite being fully
    contained in an excluded URL span, since they're real PII rather than a NER
    false positive.
    """
    text = (
        "See https://example.com/support?email=jane.doe@mit.edu&phone=617-555-0100 "
        "for help"
    )
    url_start = text.index("https")
    url_end = len(text) - len(" for help")
    email_start = text.index("jane.doe@mit.edu")
    email_end = email_start + len("jane.doe@mit.edu")
    phone_start = text.index("617-555-0100")
    phone_end = phone_start + len("617-555-0100")

    results = redact._STRICT_EMAIL_RECOGNIZER.analyze(
        text=text, entities=["EMAIL_ADDRESS"]
    )
    assert len(results) == 1
    assert text[results[0].start : results[0].end] == "jane.doe@mit.edu"

    class _EmailAndPhoneInUrlAnalyzer:
        def analyze(self, text: str, language: str) -> list[_AnalyzerResult]:  # noqa: ARG002
            return [
                _AnalyzerResult("URL", url_start, url_end),
                _AnalyzerResult("EMAIL_ADDRESS", email_start, email_end),
                _AnalyzerResult("PHONE_NUMBER", phone_start, phone_end),
            ]

    monkeypatch.setattr(redact, "_get_analyzer", _EmailAndPhoneInUrlAnalyzer)
    monkeypatch.setattr(redact, "_get_anonymizer", _FakeAnonymizer)

    redacted = redact._redact_text(text)
    assert "<EMAIL_ADDRESS>" in redacted
    assert "<PHONE_NUMBER>" in redacted


def test_redact_text_masks_only_identifying_entity_types(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """LOCATION, NRP and national-ID matches stay as written; PERSON and IBAN are
    masked.

    The English NER model tags ordinary Spanish words as LOCATION/NRP, and the
    driver's-license pattern matches problem numbers like "2.3.5".
    """
    text = "Problema 2.3.5 de Colombia, pregunta de Ana, IBAN DE89370400440532013000"

    def span(word: str) -> tuple[int, int]:
        start = text.index(word)
        return start, start + len(word)

    class _MixedAnalyzer:
        def analyze(self, text: str, language: str) -> list[_AnalyzerResult]:  # noqa: ARG002
            return [
                _AnalyzerResult("US_DRIVER_LICENSE", *span("2.3.5")),
                _AnalyzerResult("LOCATION", *span("Colombia")),
                _AnalyzerResult("NRP", *span("pregunta")),
                _AnalyzerResult("PERSON", *span("Ana")),
                _AnalyzerResult("IBAN_CODE", *span("DE89370400440532013000")),
            ]

    monkeypatch.setattr(redact, "_get_analyzer", _MixedAnalyzer)
    monkeypatch.setattr(redact, "_get_anonymizer", _FakeAnonymizer)

    assert redact._redact_text(text) == (
        "Problema 2.3.5 de Colombia, pregunta de <PERSON>, IBAN <IBAN_CODE>"
    )


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "Ship it to 77 Massachusetts Ave, Cambridge, MA 02139.",
            ["77 Massachusetts Ave", "MA 02139"],
        ),
        (
            "1234 Elm Street, Apt 5B, IL 62704-1234",
            ["1234 Elm Street", "IL 62704-1234"],
        ),
        (
            "I live at 221B Baker Street, London NW1 6XE",
            ["221B Baker Street", "NW1 6XE"],
        ),
        ("Mi dirección es Calle 45 #12-30, Bogotá", ["Calle 45 #12-30"]),
        (
            "ship to 77 massachusetts ave, cambridge, ma 02139",
            ["77 massachusetts ave", "ma 02139"],
        ),
        ("vivo en calle 45 #12-30, bogotá", ["calle 45 #12-30"]),
    ],
)
def test_street_address_recognizer_matches_the_street_and_postal_code(
    text: str, expected: list[str]
) -> None:
    """The street and ZIP/postcode are masked; a city or country alone is not."""
    results = redact._STREET_ADDRESS_RECOGNIZER.analyze(
        text=text, entities=["STREET_ADDRESS"]
    )

    assert sorted(text[r.start : r.end] for r in results) == sorted(expected)


@pytest.mark.parametrize(
    "text",
    [
        "Problem PS2.3.5 asks about 3 ways to sort",
        "I watched 12 videos in week 3",
        "my ma 02139 and 50 states",
        "Vivo en Bogotá, Colombia",
        "I watched 12 videos on the way to work",
        "it took 3 tries on the road to the answer",
        "in 12345 cases, or 23456",
    ],
)
def test_street_address_recognizer_ignores_text_that_is_not_an_address(
    text: str,
) -> None:
    results = redact._STREET_ADDRESS_RECOGNIZER.analyze(
        text=text, entities=["STREET_ADDRESS"]
    )

    assert results == []


def test_redact_text_masks_a_bank_number_only_next_to_a_context_word(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Presidio scores a bare 8-17 digit run 0.05 and one near "account" 0.4.

    The bare runs in feedback are ticket and meeting IDs, so only the second masks,
    even though spaCy also tags it DATE_TIME.
    """
    text = "Ticket EDX-12345678, bank account 987654321"

    def span(word: str) -> tuple[int, int]:
        start = text.index(word)
        return start, start + len(word)

    class _BankAnalyzer:
        def analyze(self, text: str, language: str) -> list[_AnalyzerResult]:  # noqa: ARG002
            return [
                _AnalyzerResult("US_BANK_NUMBER", *span("12345678"), score=0.05),
                _AnalyzerResult("US_BANK_NUMBER", *span("987654321"), score=0.4),
                _AnalyzerResult("DATE_TIME", *span("987654321")),
            ]

    monkeypatch.setattr(redact, "_get_analyzer", _BankAnalyzer)
    monkeypatch.setattr(redact, "_get_anonymizer", _FakeAnonymizer)

    assert redact._redact_text(text) == (
        "Ticket EDX-12345678, bank account <US_BANK_NUMBER>"
    )


def test_redact_text_masks_a_formatted_ssn_even_inside_a_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A formatted SSN (score 0.5) is masked inside a URL; a bare 9-digit order ID
    (weak SSN pattern, score 0.05) in the same kind of span is not.
    """
    text = (
        "See https://example.test/?ssn=234-56-7890 and https://example.test/r/512345678"
    )

    def span(word: str) -> tuple[int, int]:
        start = text.index(word)
        return start, start + len(word)

    class _SsnInUrlAnalyzer:
        def analyze(self, text: str, language: str) -> list[_AnalyzerResult]:  # noqa: ARG002
            return [
                _AnalyzerResult("URL", *span("https://example.test/?ssn=234-56-7890")),
                _AnalyzerResult("US_SSN", *span("234-56-7890"), score=0.5),
                _AnalyzerResult("URL", *span("https://example.test/r/512345678")),
                _AnalyzerResult("US_SSN", *span("512345678"), score=0.05),
            ]

    monkeypatch.setattr(redact, "_get_analyzer", _SsnInUrlAnalyzer)
    monkeypatch.setattr(redact, "_get_anonymizer", _FakeAnonymizer)

    assert redact._redact_text(text) == (
        "See https://example.test/?ssn=<US_SSN> and https://example.test/r/512345678"
    )


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


@pytest.mark.usefixtures("fake_analyzer")
def test_redact_and_checkpoint_writes_each_batch_with_the_current_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Each batch is written as it finishes, so a crash keeps the earlier ones."""
    written: list[pl.DataFrame] = []
    monkeypatch.setattr(
        redact,
        "checkpoint_redacted_chunk",
        lambda _catalog, _table, chunk_df: written.append(chunk_df),
    )
    df = pl.DataFrame(
        {
            "source_slug": ["zendesk"] * 5,
            "source_record_ref": [str(i) for i in range(5)],
            "title": ["PII"] * 5,
            "text": ["ok"] * 5,
        }
    )

    count = redact.redact_and_checkpoint(
        df, (None, "db.feedback_redacted"), batch_size=2
    )

    assert count == 5
    assert [chunk.height for chunk in written] == [2, 2, 1]
    assert all(
        (chunk["redaction_version"] == redact.REDACTION_VERSION).all()
        for chunk in written
    )


def test_key_filter_matches_only_the_batch_keys_across_three_sources() -> None:
    """A batch with 3+ sources builds a valid filter that matches only its own keys."""
    chunk_df = pl.DataFrame(
        {
            "source_slug": ["zendesk", "learn_ai_tutor", "discussion_forum"],
            "source_record_ref": ["1", "2", "3"],
        }
    )
    table = pa.table(
        {
            "source_slug": [
                "zendesk",
                "zendesk",
                "learn_ai_tutor",
                "discussion_forum",
                "discussion_forum",
                "content_feedback",
            ],
            "source_record_ref": ["1", "2", "2", "3", "1", "1"],
        }
    )
    schema = Schema(
        NestedField(1, "source_slug", StringType(), required=False),
        NestedField(2, "source_record_ref", StringType(), required=False),
    )

    key_filter = bind(schema, redact._key_filter(chunk_df), case_sensitive=True)
    matched = table.filter(expression_to_pyarrow(key_filter)).to_pylist()

    assert matched == [
        {"source_slug": "zendesk", "source_record_ref": "1"},
        {"source_slug": "learn_ai_tutor", "source_record_ref": "2"},
        {"source_slug": "discussion_forum", "source_record_ref": "3"},
    ]


class _FakeField:
    def __init__(self, name: str) -> None:
        self.name = name


class _FakeSchema:
    def __init__(self, column_names: list[str]) -> None:
        self.fields = [_FakeField(name) for name in column_names]


class _FakeSchemaUpdate:
    def __init__(self, table: "_FakeTable") -> None:
        self._table = table

    def union_by_name(self, schema: Any) -> None:
        # Like the real one: new columns go at the end of the table's order.
        for name in schema.names:
            if name not in self._table.column_names:
                self._table.column_names.append(name)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        pass


class _FakeTable:
    def __init__(self, column_names: list[str]) -> None:
        self.column_names = column_names
        self.overwrites: list[dict[str, Any]] = []

    def update_schema(self) -> _FakeSchemaUpdate:
        return _FakeSchemaUpdate(self)

    def schema(self) -> _FakeSchema:
        return _FakeSchema(self.column_names)

    def overwrite(self, **kwargs: Any) -> None:
        self.overwrites.append(kwargs)


class _FakeCatalog:
    def __init__(self, table: _FakeTable) -> None:
        self.table = table
        self.create_calls: list[str] = []

    def create_table_if_not_exists(self, identifier: str, **_: Any) -> _FakeTable:
        self.create_calls.append(identifier)
        return self.table


def _redacted_chunk(refs: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "source_slug": ["zendesk"] * len(refs),
            "source_record_ref": refs,
            "title_redacted": [f"title {ref}" for ref in refs],
            "text_redacted": [f"text {ref}" for ref in refs],
            "redaction_version": [redact.REDACTION_VERSION] * len(refs),
        }
    )


def test_checkpoint_redacted_chunk_writes_in_table_order() -> None:
    """A table from before redaction_version gets the column, and the write's
    columns follow the table's order, because the write casts by position.
    """
    table = _FakeTable(
        ["text_redacted", "source_slug", "source_record_ref", "title_redacted"]
    )
    catalog = _FakeCatalog(table)
    chunk_df = _redacted_chunk(["1", "2"])

    redact.checkpoint_redacted_chunk(catalog, "db.feedback_redacted", chunk_df)

    assert table.column_names[-1] == "redaction_version"
    assert len(table.overwrites) == 1
    written = table.overwrites[0]["df"]
    assert written.column_names == table.column_names
    assert written.column("text_redacted").to_pylist() == ["text 1", "text 2"]
    assert table.overwrites[0]["overwrite_filter"] == redact._key_filter(chunk_df)


def test_checkpoint_redacted_chunk_skips_an_empty_chunk() -> None:
    catalog = _FakeCatalog(_FakeTable([]))

    redact.checkpoint_redacted_chunk(
        catalog, "db.feedback_redacted", _redacted_chunk([])
    )

    assert catalog.create_calls == []


def test_checkpoint_redacted_chunk_rejects_duplicate_keys() -> None:
    """Overwrite would keep both copies, so a duplicate key must fail first."""
    table = _FakeTable(list(redact.REDACTED_SCHEMA))

    with pytest.raises(ValueError, match="Duplicate"):
        redact.checkpoint_redacted_chunk(
            _FakeCatalog(table), "db.feedback_redacted", _redacted_chunk(["1", "1"])
        )
    assert table.overwrites == []

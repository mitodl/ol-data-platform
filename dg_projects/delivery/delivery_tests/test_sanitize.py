"""Tests for the shared HTML sanitizer used by MIT Learn webhook delivery."""

import pytest
from delivery.assets.mitpe import _row_to_resource
from delivery.lib.sanitize import (
    ALLOWED_HTML_ATTRIBUTES_WITH_LINKS,
    ALLOWED_HTML_TAGS_WITH_LINKS,
    clean_html,
)


def test_clean_html_strips_script_tags():
    """Script tags and their contents must not survive sanitization."""
    cleaned = clean_html("<p>Intro</p><script>alert('xss')</script>")
    assert "script" not in cleaned
    assert "alert" not in cleaned
    assert cleaned == "<p>Intro</p>"


def test_clean_html_strips_event_handler_attributes():
    """Event handler attributes are dropped even on allowed tags."""
    cleaned = clean_html('<img src="x" onerror="alert(1)">Body')
    assert "onerror" not in cleaned
    assert "<img" not in cleaned
    assert "Body" in cleaned


def test_clean_html_strips_links_by_default():
    """The default allowlist has no <a>, matching clean_data's own default.

    mit-learn's MIT PE ETL calls ``clean_data(description)`` with no overrides,
    so anchors never reached the database on the legacy path either. Keeping
    them here would make webhook delivery diverge from the ETL it replaces.
    """
    cleaned = clean_html('<a href="https://example.com" title="Docs">Docs</a>')
    assert "<a" not in cleaned
    assert "href" not in cleaned
    assert cleaned == "Docs"


def test_clean_html_preserves_links_when_asked():
    """The WITH_LINKS allowlists keep href and title, for the podcast path."""
    cleaned = clean_html(
        '<a href="https://example.com" title="Docs">Docs</a>',
        tags=ALLOWED_HTML_TAGS_WITH_LINKS,
        attributes=ALLOWED_HTML_ATTRIBUTES_WITH_LINKS,
    )
    assert 'href="https://example.com"' in cleaned
    assert 'title="Docs"' in cleaned


def test_clean_html_drops_disallowed_link_attributes():
    """Under WITH_LINKS, attributes outside href/title are still stripped."""
    cleaned = clean_html(
        '<a href="https://example.com" onclick="x()">Docs</a>',
        tags=ALLOWED_HTML_TAGS_WITH_LINKS,
        attributes=ALLOWED_HTML_ATTRIBUTES_WITH_LINKS,
    )
    assert "onclick" not in cleaned
    assert 'href="https://example.com"' in cleaned


@pytest.mark.parametrize(
    "markup",
    [
        "<p>A paragraph</p>",
        "<ul><li>One</li><li>Two</li></ul>",
        "<strong>Bold</strong> and <em>italic</em>",
    ],
)
def test_clean_html_preserves_allowlisted_formatting(markup):
    """Formatting tags on mit-learn's allowlist pass through untouched."""
    assert clean_html(markup) == markup


def test_clean_html_leaves_plain_text_unchanged():
    """Plain text is returned verbatim."""
    text = "A perfectly ordinary course description."
    assert clean_html(text) == text


def test_clean_html_preserves_none():
    """None must stay None, not become an empty string.

    ``clean_data`` returns "" for both None and "", collapsing the two.
    ``_row_to_resource`` always emits a ``description`` key, so whichever value
    this returns lands in ``update_or_create(defaults=...)`` and is written:
    None writes SQL NULL, "" writes an empty string. Neither leaves an existing
    description alone. Passing both through unchanged means MIT Learn records
    the source's own null-versus-empty distinction instead of one this function
    invented.
    """
    result = clean_html(None)
    assert result is None


def test_clean_html_preserves_empty_string():
    """An empty string stays an empty string rather than being rewritten."""
    assert clean_html("") == ""


def test_row_to_resource_sanitizes_description():
    """The mitpe payload builder delivers a sanitized description."""
    resource = _row_to_resource(
        {
            "readable_id": "mitpe-course-1",
            "title": "A Course",
            "description": "<p>Real copy</p><script>alert('xss')</script>",
        }
    )
    assert resource["description"] == "<p>Real copy</p>"


def test_row_to_resource_keeps_missing_description_none():
    """A row with no description delivers None, not an empty string."""
    resource = _row_to_resource({"readable_id": "mitpe-course-2", "title": "A Course"})
    assert resource["description"] is None

"""Tests for shaping MIT Climate integration rows into MIT Learn's webhook payload."""

from typing import Any

from delivery.assets.mit_climate import build_resources


def article_row(**overrides: Any) -> dict[str, Any]:
    """Build an integrations__learn__mit_climate_articles row."""
    return {
        "readable_id": "3111352e-e6cf-4834-8197-ed3da88771cd",
        "title": "Energy use in buildings",
        "last_modified": "2025-07-08T04:00:00.000Z",
        "etl_source": "mit_climate",
        "description": "Summary",
        "url": "https://climate.mit.edu/explainers/buildings",
        "image_url": "https://climate.mit.edu/buildings.png",
        "image_alt": "A building",
        "topics": ["Energy", "Urban Studies"],
        "footnotes": "<p>1 Source</p>",
        "byline": "By someone",
        "feed_type": "explainer",
        "published": True,
        "platform": "climate",
        **overrides,
    }


def test_article_is_a_document_with_its_topics_and_created_date() -> None:
    """Articles load as Article documents, with topics as the model resolved them."""
    [resource] = build_resources([article_row()])
    assert resource["resource_type"] == "document"
    assert resource["resource_category"] == "Article"
    assert resource["etl_source"] == "mit_climate"
    assert resource["platform"] == "climate"
    assert resource["offered_by"] == {"code": "climate"}
    assert resource["topics"] == [{"name": "Energy"}, {"name": "Urban Studies"}]
    assert resource["created_on"] == "2025-07-08T04:00:00.000Z"
    assert resource["image"] == {
        "url": "https://climate.mit.edu/buildings.png",
        "alt": "A building",
    }


def test_full_description_joins_summary_footnotes_and_byline() -> None:
    """The legacy ETL joined the three with newlines, keeping the empty ones."""
    [full, bare] = build_resources(
        [article_row(), article_row(footnotes=None, byline=None)]
    )
    assert full["full_description"] == "Summary\n<p>1 Source</p>\nBy someone"
    assert bare["full_description"] == "Summary\n\n"


def test_no_image_and_no_topics() -> None:
    """An unresolved article sends [] so MIT Learn picks topics by similarity."""
    [resource] = build_resources([article_row(image_url=None, topics=None)])
    assert resource["image"] is None
    assert resource["topics"] == []

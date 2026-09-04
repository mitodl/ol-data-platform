"""HTML sanitization for MIT Learn webhook delivery payloads.

MIT Learn's loaders pass webhook payload keys straight through to
``LearningResource.objects.update_or_create(defaults=...)`` (see
``learning_resources/etl/loaders.py::upsert_course_or_program`` in mit-learn),
so nothing downstream strips HTML. Any sanitization the legacy Celery ETL
performed has to be performed here instead, or the migration to webhook
delivery silently starts persisting raw upstream HTML.
"""

import nh3

# Mirrors ALLOWED_HTML_TAGS / ALLOWED_HTML_ATTRIBUTES and their _WITH_LINKS
# counterparts in mit-learn's main/constants.py.
#
# Duplicated rather than imported: there is no shared package between
# ol-data-platform and mit-learn. If MIT Learn changes its allowlist this must
# change with it -- a drift here is a sanitization difference, not a formatting
# one.
ALLOWED_HTML_TAGS = {
    "b",
    "blockquote",
    "br",
    "caption",
    "center",
    "cite",
    "code",
    "div",
    "em",
    "hr",
    "i",
    "li",
    "ol",
    "p",
    "pre",
    "q",
    "small",
    "span",
    "strike",
    "strong",
    "sub",
    "sup",
    "u",
    "ul",
}
ALLOWED_HTML_ATTRIBUTES: dict[str, set[str]] = {}

# Podcast RSS "show notes" carry resource links MIT Learn keeps; only the
# podcast ETL passes these.
ALLOWED_HTML_TAGS_WITH_LINKS = ALLOWED_HTML_TAGS | {"a"}
ALLOWED_HTML_ATTRIBUTES_WITH_LINKS = {"a": {"href", "title"}}


def clean_html(
    value: str | None,
    tags: set[str] | None = None,
    attributes: dict[str, set[str]] | None = None,
) -> str | None:
    """Strip disallowed HTML, mirroring mit-learn's ``main.utils.clean_data``.

    Defaults to the no-links allowlist, as ``clean_data`` does. Callers whose
    mit-learn counterpart passes the ``_WITH_LINKS`` allowlists (the podcast
    ETL) pass them here too; anything else would deliver anchors the legacy
    path stripped.

    Deliberately diverges from ``clean_data`` on falsy input: ``clean_data``
    returns ``""`` for both ``None`` and ``""``, collapsing the two. Both are
    delivered verbatim here so the payload carries the source's own
    null-versus-empty distinction rather than one this function invented.
    """
    if not value:
        return value
    return nh3.clean(
        value,
        tags=ALLOWED_HTML_TAGS if tags is None else tags,
        attributes=ALLOWED_HTML_ATTRIBUTES if attributes is None else attributes,
    )

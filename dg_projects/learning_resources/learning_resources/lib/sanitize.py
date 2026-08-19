"""HTML sanitization for MIT Learn webhook delivery payloads.

MIT Learn's loaders pass webhook payload keys straight through to
``LearningResource.objects.update_or_create(defaults=...)`` (see
``learning_resources/etl/loaders.py::upsert_course_or_program`` in mit-learn),
so nothing downstream strips HTML. Any sanitization the legacy Celery ETL
performed has to be performed here instead, or the migration to webhook
delivery silently starts persisting raw upstream HTML.
"""

import nh3

# Mirrors ALLOWED_HTML_TAGS_WITH_LINKS / ALLOWED_HTML_ATTRIBUTES_WITH_LINKS in
# mit-learn's main/constants.py -- ALLOWED_HTML_TAGS plus "a", with href/title
# preserved on links.
#
# Duplicated rather than imported: there is no shared package between
# ol-data-platform and mit-learn. If MIT Learn changes its allowlist this must
# change with it -- a drift here is a sanitization difference, not a formatting
# one.
ALLOWED_HTML_TAGS_WITH_LINKS = {
    "a",
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
ALLOWED_HTML_ATTRIBUTES_WITH_LINKS = {"a": {"href", "title"}}


def clean_html(value: str | None) -> str | None:
    """Strip disallowed HTML, mirroring mit-learn's ``main.utils.clean_data``.

    Deliberately diverges from ``clean_data`` on falsy input: ``clean_data``
    returns ``""``, but webhook payloads are handed to ``update_or_create`` as
    ``defaults``, so delivering ``""`` would overwrite an already-populated
    field on the existing resource. An absent value stays absent instead.
    """
    if not value:
        return value
    return nh3.clean(
        value,
        tags=ALLOWED_HTML_TAGS_WITH_LINKS,
        attributes=ALLOWED_HTML_ATTRIBUTES_WITH_LINKS,
    )

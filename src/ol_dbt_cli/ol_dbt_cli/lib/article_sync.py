"""What `ol-dbt articles sync` sends to OpenMetadata for an article file, and when.

An article is a Context Center page (``/v1/contextCenter/pages``) of type
Article whose description is the source markdown rendered to HTML. It is
rendered here because OpenMetadata runs every description through an HTML
sanitizer (DescriptionSanitizer, OWASP), which would entity-encode markdown's
backticks and drop text like ``<source>``. Relative links are pointed at the
file on GitHub, since they mean nothing in OpenMetadata.

A missing article is created by POST. After that sync sends a JSON Patch, not
a PUT: OpenMetadata keeps a stored description against a bot's PUT
(EntityUpdater.keepsStoredDescription), so a PUT would never change one. The
patch puts back the description when the SHA-256 in its last line differs from
the source file's, and the related entities when they differ from the declared
ones. The digest is compared rather than the text because the stored HTML is
the sanitizer's rewrite of what was sent. So an edit made in the OpenMetadata
UI is overwritten at the next change to the source, and belongs in a pull
request. displayName and owners are set only on create; a bot may not change a
stored displayName (DefaultBotPolicy denies EditDisplayName).

An article none of whose related entities OpenMetadata has catalogued yet is
not written at all, because OpenMetadata attaches a page created without any
to the Organization team (KnowledgePageMapper.createToEntity).
"""

from __future__ import annotations

import fnmatch
import hashlib
import html
import posixpath
from typing import TYPE_CHECKING, Any

from markdown_it import MarkdownIt

from ol_dbt_cli.lib.data_contracts import EntityBinding, om_fqn
from ol_dbt_cli.lib.metric_sync import STARROCKS_TAG, LineageSource, starrocks_fqn

if TYPE_CHECKING:
    from pathlib import PurePosixPath

    from ol_dbt_cli.lib.article_registry import Article
    from ol_dbt_cli.lib.manifest import ManifestModel, ManifestRegistry

PAGES_PATH = "/v1/contextCenter/pages"
PAGE_FIELDS = "relatedEntities"
REPO_BLOB_URL = "https://github.com/mitodl/ol-data-platform/blob/main"
# Images need the file itself, not GitHub's page for it, and the sanitizer
# drops any image whose src is not an http(s) or raster data URL.
REPO_RAW_URL = "https://raw.githubusercontent.com/mitodl/ol-data-platform/main"
# Prefixes of a link target that already means something outside the repository.
_ABSOLUTE_LINK_PREFIXES = ("http://", "https://", "mailto:", "#", "/")


def source_digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def digest_marker(digest: str) -> str:
    """Return the text in an article's last line that identifies the source it was rendered from."""
    return f"sha256 {digest}"


def _repo_url(base: str, source: PurePosixPath, target: str) -> str:
    """Resolve a link *target* written in *source* to the file under *base* on GitHub."""
    path, _, fragment = target.partition("#")
    resolved = posixpath.normpath(posixpath.join(str(source.parent), path))
    return f"{base}/{resolved}" + (f"#{fragment}" if fragment else "")


def render_body(text: str, source: PurePosixPath) -> str:
    """Render the article's markdown to the HTML OpenMetadata stores, with a closing source line.

    :param source: the markdown's repository-relative path, which relative links resolve against.
    """
    md = MarkdownIt("commonmark", {"html": False}).enable("table").enable("strikethrough")
    tokens = md.parse(text)
    targets = {"link_open": ("href", REPO_BLOB_URL), "image": ("src", REPO_RAW_URL)}
    for token in tokens:
        for child in token.children or []:
            attr, base = targets.get(child.type, (None, ""))
            target = child.attrGet(attr) if attr else None
            if isinstance(target, str) and not target.startswith(_ABSOLUTE_LINK_PREFIXES):
                child.attrSet(attr, _repo_url(base, source, target))
    body = md.renderer.render(tokens, md.options, {})
    path = html.escape(str(source))
    footer = (
        f'<p><em>Published from <a href="{REPO_BLOB_URL}/{path}">{path}</a> '
        f"({digest_marker(source_digest(text))}) by ol-dbt articles sync. "
        "Edits made here are overwritten when that file changes.</em></p>"
    )
    return f"{body}{footer}"


def _model_node(name: str, manifest: ManifestRegistry) -> ManifestModel | None:
    node = manifest.get_model(name)
    if node is not None:
        return node if node.is_model else None
    disabled = manifest.disabled.get(name)
    # A model disabled for any other reason is built nowhere.
    return disabled if disabled is not None and STARROCKS_TAG in disabled.tags else None


def related_entities(
    article: Article, manifest: ManifestRegistry, service: str, starrocks_service: str
) -> list[LineageSource]:
    """Resolve the article's ``related`` bindings to the entities OpenMetadata catalogs, in file order.

    A ``dbt_model`` pattern expands to every matching model, a dbt model tagged
    ``starrocks`` resolves under *starrocks_service* and every other dbt
    binding under *service*.

    :raises ValueError: when a ``dbt_model`` name or pattern matches no model
        built anywhere, or a ``dbt_source`` is not in the manifest.
    :rtype: list[LineageSource]
    """
    sources: list[LineageSource] = []
    for binding in article.related:
        if binding.kind != "dbt_model":
            sources.append(LineageSource(binding.entity_type, om_fqn(binding, manifest, service)))
            continue
        names = sorted(
            n for n in manifest.by_name.keys() | manifest.disabled.keys() if fnmatch.fnmatchcase(n, binding.target)
        )
        nodes = [node for node in (_model_node(name, manifest) for name in names) if node is not None]
        if not nodes:
            msg = f"{article.path.name}: dbt_model {binding.target!r} matches no model in the manifest"
            raise ValueError(msg)
        for node in nodes:
            if STARROCKS_TAG in node.tags:
                sources.append(
                    LineageSource(binding.entity_type, starrocks_fqn(node, starrocks_service), starrocks=True)
                )
            else:
                exact = EntityBinding(binding.entity_type, "dbt_model", node.name)
                sources.append(LineageSource(binding.entity_type, om_fqn(exact, manifest, service)))
    return list(dict.fromkeys(sources))


def patch_ops(
    live: dict[str, Any], digest: str, description: str, related: list[dict[str, str]]
) -> list[dict[str, Any]]:
    """Return the JSON Patch operations that bring the live page back to its source and related entities.

    :param digest: the source file's :func:`source_digest`.
    :param description: the rendered body, sent only when the live one came from another version of the source.
    :param related: the declared related entities as ``{id, type}`` references.
    """
    ops: list[dict[str, Any]] = []
    if digest_marker(digest) not in (live.get("description") or ""):
        ops.append({"op": "add", "path": "/description", "value": description})
    if {ref["id"] for ref in live.get("relatedEntities") or []} != {ref["id"] for ref in related}:
        ops.append({"op": "add", "path": "/relatedEntities", "value": related})
    return ops


def page_body(
    article: Article, description: str, owners: list[dict[str, str]], related: list[dict[str, str]]
) -> dict[str, Any]:
    """Build the CreatePage body, with owners and related entities as ``{id, type}`` references."""
    return {
        "name": article.spec.name,
        "displayName": article.spec.displayName,
        "description": description,
        "pageType": "Article",
        "page": {},
        "owners": owners,
        "relatedEntities": related,
    }

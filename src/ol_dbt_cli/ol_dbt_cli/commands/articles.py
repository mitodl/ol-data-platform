"""`ol-dbt articles` — publish the documents listed in ``articles/`` as OpenMetadata Context Center articles.

See ``ol_dbt_cli.lib.article_registry`` for the file format and
``ol_dbt_cli.lib.article_sync`` for what sync writes and when.
"""

from __future__ import annotations

import sys
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Annotated

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.article_registry import DEFAULT_ARTICLES_DIR, Article, load_articles
from ol_dbt_cli.lib.article_sync import (
    PAGE_FIELDS,
    PAGES_PATH,
    page_body,
    patch_ops,
    related_entities,
    render_body,
    source_digest,
)
from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.openmetadata import (
    JSON_PATCH_CONTENT_TYPE,
    OpenMetadataClient,
    load_catalog_manifest,
    quote_fqn,
)

if TYPE_CHECKING:
    from ol_dbt_cli.lib.manifest import ManifestRegistry

console = Console()
err_console = Console(stderr=True)

articles_app = App(
    name="articles",
    help="Publish the documents listed in articles/ as OpenMetadata Context Center articles.",
)


def _sync_one(  # noqa: PLR0913
    client: OpenMetadataClient,
    article: Article,
    repo_root: Path,
    manifest: ManifestRegistry,
    service: str,
    starrocks_service: str,
    *,
    dry_run: bool,
) -> None:
    """Create one article, or patch it where its source or related entities changed.

    :raises ValueError: when a related entity cannot be resolved.
    :raises OpenMetadataError: when OpenMetadata refuses a request.
    """
    name = escape(article.spec.name)
    related = []
    for source in related_entities(article, manifest, service, starrocks_service):
        entity_id = client.entity_id_or_none(source.entity_type, source.fqn)
        if entity_id is None:
            # Catalogued only after the next ingestion of its service, as for a new model.
            console.print(f"[yellow]skipped[/]   {name}: {escape(source.fqn)} is not in OpenMetadata yet")
            continue
        related.append({"id": entity_id, "type": source.entity_type})
    if not related:
        console.print(f"[yellow]skipped[/]   {name}: none of its related entities is in OpenMetadata yet")
        return
    text = (repo_root / article.spec.source).read_text()
    description = render_body(text, PurePosixPath(article.spec.source.as_posix()))
    live = client.get_or_none(f"{PAGES_PATH}/name/{quote_fqn(article.spec.name)}", query={"fields": PAGE_FIELDS})
    if live is None:
        if dry_run:
            console.print(f"[green]would[/]     {name}: create, attached to {len(related)} entities")
            return
        owners = client.named_refs([owner.model_dump() for owner in article.spec.owners])
        client.request("POST", PAGES_PATH, page_body(article, description, owners, related))
        console.print(f"[green]created[/]   {name}: attached to {len(related)} entities")
        return
    ops = patch_ops(live, source_digest(text), description, related)
    if not ops:
        console.print(f"[green]unchanged[/] {name}: matches {article.spec.source}")
        return
    fields = ", ".join(op["path"].lstrip("/") for op in ops)
    if dry_run:
        console.print(f"[green]would[/]     {name}: PATCH {fields}")
        return
    client.request("PATCH", f"{PAGES_PATH}/{live['id']}", ops, content_type=JSON_PATCH_CONTENT_TYPE)
    console.print(f"[green]patched[/]   {name}: {fields}")


@articles_app.command
def sync(  # noqa: PLR0913
    *,
    service: Annotated[
        str,
        Parameter(
            name=["--service"],
            help='OpenMetadata database service that dbt relations live under, e.g. "Starburst Galaxy".',
        ),
    ],
    starrocks_service: Annotated[
        str,
        Parameter(
            name=["--starrocks-service"],
            help="OpenMetadata database service that the StarRocks views (dbt models tagged starrocks) live under.",
        ),
    ] = "StarRocks",
    manifest_path: Annotated[
        str | None,
        Parameter(
            name=["--manifest"],
            help="Production dbt manifest. Defaults to <dbt-dir>/target/manifest.json.",
        ),
    ] = None,
    dbt_dir: Annotated[
        str,
        Parameter(name=["--dbt-dir"], help="dbt project directory."),
    ] = "src/ol_dbt",
    articles_dir_path: Annotated[
        str | None,
        Parameter(name=["--articles-dir"], help="Articles directory. Defaults to articles/ in the repository."),
    ] = None,
    dry_run: Annotated[
        bool,
        Parameter(name=["--dry-run"], help="Read OpenMetadata and report what would change; write nothing."),
    ] = False,
) -> None:
    """Create each article in OpenMetadata, or patch it where its source file or related entities changed.

    Reads OM_SERVER_URL and OM_BOT_JWT_TOKEN. Each article's body is its
    source markdown rendered to HTML, and its related entities are the assets
    its file lists. A related entity OpenMetadata has not catalogued yet is
    skipped with a warning and attached by a later sync. Sources resolve
    against the repository the articles directory is in, so both come from the
    same commit. A file that does not load stops the sync before anything is
    written; past that, exits non-zero when any article failed to sync, and
    the others still sync.
    """
    repo_root = get_repo_root()
    articles_dir = Path(articles_dir_path).resolve() if articles_dir_path else repo_root / DEFAULT_ARTICLES_DIR
    source_root = articles_dir.parent
    try:
        articles = load_articles(articles_dir, source_root)
        manifest = load_catalog_manifest(repo_root / dbt_dir, manifest_path)
    except ValueError as exc:
        err_console.print(f"[red]Error:[/] {escape(str(exc))}")
        sys.exit(1)
    client = OpenMetadataClient.from_env()
    failures = 0
    for article in articles:
        try:
            _sync_one(client, article, source_root, manifest, service, starrocks_service, dry_run=dry_run)
        except (ValueError, RuntimeError, OSError) as exc:
            console.print(f"[bold red]failed[/]    {escape(article.spec.name)}: {escape(str(exc))}")
            failures += 1
    if failures:
        sys.exit(1)

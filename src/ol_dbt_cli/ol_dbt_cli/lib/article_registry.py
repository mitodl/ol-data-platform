"""OpenMetadata Context Center articles, kept as code in the repository's ``articles/`` directory.

Each file publishes one markdown document already in the repository as an
article, attached to the assets it describes:

    name: learn-marts-contract          # the article's name and FQN in OpenMetadata
    displayName: Learn integration schema contract
    source: docs/learn_marts_contract.md  # repository-relative markdown, the article body
    owners: [{type: team, name: data-platform}]
    related:                            # the assets the article is attached to
    - {type: table, dbt_model: "integrations__learn__*"}
    - {type: table, dbt_source: ol_warehouse_raw_data.raw__mitlearn__app__postgres__learning_resources_learningresource}
    - {type: table, fqn: "Glue.610119931565.ol_warehouse_production_external.some_table"}

The markdown stays where it is, so the file is the one copy, edited in pull
requests. A ``dbt_model`` may be an ``fnmatch`` pattern, which sync expands
against the manifest's models.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from ol_dbt_cli.lib.data_contracts import EntityBinding, parse_binding
from ol_dbt_cli.lib.glossary_registry import NamedPrincipal

DEFAULT_ARTICLES_DIR = Path("articles")
_ARTICLE_SUFFIXES = (".yml", ".yaml")


class ArticleSpec(BaseModel):
    """One article. ``related`` entries are parsed into bindings by :func:`load_articles`."""

    model_config = ConfigDict(extra="forbid")

    # The page's FQN is its name, so a dot would read as a parent page.
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9-]*$")
    displayName: str = Field(min_length=1)  # noqa: N815
    source: Path
    owners: list[NamedPrincipal] = Field(min_length=1)
    related: list[dict[str, Any]] = Field(min_length=1)

    @field_validator("source")
    @classmethod
    def _markdown_in_repo(cls, source: Path) -> Path:
        if source.is_absolute() or ".." in source.parts or source.suffix != ".md":
            msg = f"must be a repository-relative .md path, got {source}"
            raise ValueError(msg)
        return source


class Article(BaseModel):
    """A loaded article: its file, its spec and its parsed ``related`` bindings."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    path: Path
    spec: ArticleSpec
    related: tuple[EntityBinding, ...]


def load_articles(articles_dir: Path, repo_root: Path) -> list[Article]:
    """Load every article file in *articles_dir*, sorted by file name.

    :param repo_root: what each ``source`` is relative to.
    :raises ValueError: when a file does not parse, names a source that does
        not exist, or reuses another file's name.
    :rtype: list[Article]
    """
    if not articles_dir.is_dir():
        msg = f"{articles_dir} is not a directory"
        raise ValueError(msg)
    articles = []
    for path in sorted(p for p in articles_dir.iterdir() if p.suffix in _ARTICLE_SUFFIXES):
        try:
            spec = ArticleSpec.model_validate(yaml.safe_load(path.read_text()))
        except ValidationError as exc:
            msg = f"{path}: {exc}"
            raise ValueError(msg) from exc
        if not (repo_root / spec.source).is_file():
            msg = f"{path}: source {spec.source} does not exist"
            raise ValueError(msg)
        related = tuple(parse_binding(path, raw, label=f"`related[{index}]`") for index, raw in enumerate(spec.related))
        articles.append(Article(path=path, spec=spec, related=related))
    duplicates = sorted(name for name, count in Counter(a.spec.name for a in articles).items() if count > 1)
    if duplicates:
        msg = f"{articles_dir}: article names used by more than one file: {', '.join(duplicates)}"
        raise ValueError(msg)
    return articles

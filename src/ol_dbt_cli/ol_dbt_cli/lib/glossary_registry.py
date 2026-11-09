"""The OpenMetadata business glossary, kept as code in the repository's ``glossary/`` directory.

Each file declares one glossary and its terms:

    glossary:                     # OpenMetadata CreateGlossary fields
      name: open-learning
      displayName: Open Learning
      description: ...
      owners: [{type: team, name: data-platform}]
      reviewers: [{type: team, name: data-platform}]
    terms:                        # OpenMetadata CreateGlossaryTerm fields, in creation order
    - name: identifiability
      displayName: Identifiability
      description: ...
      mutuallyExclusive: true
    - name: direct-identifier
      parent: identifiability     # a term declared earlier in the file
      ...

dbt columns name a term by its fully qualified name in
``config.meta.openmetadata.glossary``, which the om-ingest-dbt job attaches in
OpenMetadata.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

DEFAULT_GLOSSARY_DIR = Path("glossary")


class NamedPrincipal(BaseModel):
    """An owner or reviewer by name. OpenMetadata takes an id, which sync resolves."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["team", "user"]
    name: str = Field(min_length=1)


class TermReference(BaseModel):
    """A link to an outside definition of a term (OpenMetadata's TermReference)."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    endpoint: str = Field(pattern=r"^https?://")


class GlossarySpec(BaseModel):
    """The glossary itself."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, pattern=r"^[^.]+$")
    displayName: str  # noqa: N815
    description: str = Field(min_length=1)
    mutuallyExclusive: bool = False  # noqa: N815
    owners: list[NamedPrincipal] = Field(min_length=1)
    reviewers: list[NamedPrincipal] = Field(min_length=1)


class GlossaryTermSpec(BaseModel):
    """One term. ``parent`` and ``relatedTerms`` name terms declared earlier in the same file."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, pattern=r"^[^.]+$")
    displayName: str  # noqa: N815
    description: str = Field(min_length=1)
    synonyms: list[str] = []
    references: list[TermReference] = []
    mutuallyExclusive: bool | None = None  # noqa: N815
    parent: str | None = None
    relatedTerms: list[str] = []  # noqa: N815


class GlossaryFile(BaseModel):
    """A file in ``glossary/``: one glossary and its terms, parents before children."""

    model_config = ConfigDict(extra="forbid")

    glossary: GlossarySpec
    terms: list[GlossaryTermSpec]

    @model_validator(mode="after")
    def _terms_refer_backwards(self) -> Self:
        # OpenMetadata holds term names unique across a glossary, case-insensitively.
        seen: dict[str, str] = {}
        for term in self.terms:
            if term.name.lower() in seen:
                msg = f"term {term.name!r} is declared twice (names are unique across a glossary, ignoring case)"
                raise ValueError(msg)
            for label, other in [("parent", term.parent), *(("relatedTerms", r) for r in term.relatedTerms)]:
                if other is not None and other not in seen.values():
                    msg = (
                        f"term {term.name!r} names {other!r} in {label}, which is not a term declared above it. "
                        "Terms are created in file order, so a parent or related term comes first."
                    )
                    raise ValueError(msg)
            seen[term.name.lower()] = term.name
        return self

    def term_fqn(self, name: str) -> str:
        """Return the FQN of the term declared as *name*: the glossary, its ancestors, then the term."""
        term = next(term for term in self.terms if term.name == name)
        return f"{self.term_fqn(term.parent) if term.parent else self.glossary.name}.{name}"

    def term_fqns(self) -> set[str]:
        return {self.term_fqn(term.name) for term in self.terms}


def load_glossaries(glossary_dir: Path) -> list[tuple[Path, GlossaryFile]]:
    """Load every ``*.yaml`` / ``*.yml`` glossary in *glossary_dir*, sorted by path.

    :param glossary_dir: directory holding one file per glossary.
    :returns: each file's path and parsed glossary; empty when the directory does not exist.
    :rtype: list[tuple[Path, GlossaryFile]]
    :raises ValueError: when a file does not fit ``GlossaryFile``, or two files declare one glossary.
    """
    if not glossary_dir.is_dir():
        return []
    glossaries: list[tuple[Path, GlossaryFile]] = []
    for path in sorted(p for p in glossary_dir.iterdir() if p.suffix in {".yaml", ".yml"}):
        try:
            parsed = GlossaryFile.model_validate(yaml.safe_load(path.read_text()))
        except ValidationError as exc:
            msg = f"{path}: {exc}"
            raise ValueError(msg) from exc
        glossaries.append((path, parsed))
    names = [g.glossary.name for _, g in glossaries]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        msg = f"{glossary_dir}: glossary {', '.join(duplicates)} is declared in more than one file"
        raise ValueError(msg)
    return glossaries

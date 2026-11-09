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
OpenMetadata. It skips an FQN OpenMetadata does not have, with only a warning
in its log, so ``check_glossary_terms`` fails CI on one this directory does not
declare.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, Literal, Self

import yaml
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, ValidationError, model_validator

from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from collections.abc import Iterator

GLOSSARY_TERMS_CHECK = "glossary_terms"
DEFAULT_GLOSSARY_DIR = Path("glossary")

# Where dbt reads YAML that can carry column or model meta.
_DBT_YAML_ROOTS = ("models", "snapshots", "seeds", "analyses")


def _no_angle_bracket(description: str) -> str:
    # OpenMetadata runs a description containing "<" through an HTML sanitizer
    # (it drops e.g. "<platform>" and entity-encodes backticks), so the stored
    # text never equals the declared one and every sync would patch the term,
    # sending it back to In Review.
    if "<" in description:
        msg = "contains '<', which OpenMetadata's HTML sanitizer rewrites; reword it"
        raise ValueError(msg)
    return description


Description = Annotated[str, Field(min_length=1), AfterValidator(_no_angle_bracket)]


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
    description: Description
    mutuallyExclusive: bool = False  # noqa: N815
    owners: list[NamedPrincipal] = Field(min_length=1)
    reviewers: list[NamedPrincipal] = Field(min_length=1)


class GlossaryTermSpec(BaseModel):
    """One term. ``parent`` and ``relatedTerms`` name terms declared earlier in the same file."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, pattern=r"^[^.]+$")
    displayName: str  # noqa: N815
    description: Description
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
    :returns: each file's path and parsed glossary.
    :rtype: list[tuple[Path, GlossaryFile]]
    :raises ValueError: when the directory does not exist, a file does not fit ``GlossaryFile``,
        or two files declare one glossary.
    """
    if not glossary_dir.is_dir():
        msg = f"{glossary_dir}: no such glossary directory"
        raise ValueError(msg)
    glossaries: list[tuple[Path, GlossaryFile]] = []
    for path in sorted(p for p in glossary_dir.iterdir() if p.suffix in {".yaml", ".yml"}):
        try:
            parsed = GlossaryFile.model_validate(yaml.safe_load(path.read_text()))
        except ValidationError as exc:
            msg = f"{path}: {exc}"
            raise ValueError(msg) from exc
        glossaries.append((path, parsed))
    counts = Counter(g.glossary.name for _, g in glossaries)
    duplicates = sorted(n for n, count in counts.items() if count > 1)
    if duplicates:
        msg = f"{glossary_dir}: glossary {', '.join(duplicates)} is declared in more than one file"
        raise ValueError(msg)
    return glossaries


def _glossary_meta(node: Any, names: tuple[str, ...] = ()) -> Iterator[tuple[tuple[str, ...], Any]]:
    """Yield every ``openmetadata.glossary`` value under *node*, with the ``name``s of the mappings above it.

    It finds the block whether it sits under ``config.meta``, the older
    column-level ``meta`` or a ``+meta`` in dbt_project.yml.
    """
    if isinstance(node, dict):
        if isinstance(node.get("name"), str):
            names = (*names, node["name"])
        openmetadata = node.get("openmetadata")
        if isinstance(openmetadata, dict) and "glossary" in openmetadata:
            yield names, openmetadata["glossary"]
        for value in node.values():
            yield from _glossary_meta(value, names)
    elif isinstance(node, list):
        for item in node:
            yield from _glossary_meta(item, names)


def check_glossary_terms(dbt_dir: Path, glossary_dir: Path, report: ValidationReport) -> None:
    """Fail on every ``openmetadata.glossary`` FQN in the dbt project that ``glossary/`` does not declare.

    :param dbt_dir: The dbt project directory.
    :param glossary_dir: The directory of glossary files.
    :param report: The report to add an ERROR to for each unknown or malformed entry.
    """
    # With no glossary directory every FQN is unknown, which the loop below reports.
    glossaries = glossary_dir.is_dir()
    try:
        declared = (
            {fqn for _, glossary in load_glossaries(glossary_dir) for fqn in glossary.term_fqns()}
            if glossaries
            else set()
        )
    except ValueError as exc:
        report.add(GLOSSARY_TERMS_CHECK, Severity.ERROR, glossary_dir.name, str(exc))
        return
    paths = [dbt_dir / "dbt_project.yml"] + sorted(
        p for root in _DBT_YAML_ROOTS for pattern in ("*.yml", "*.yaml") for p in (dbt_dir / root).rglob(pattern)
    )
    for path in paths:
        relative = path.relative_to(dbt_dir).as_posix()
        for names, terms in _glossary_meta(yaml.safe_load(path.read_text())):
            where = ".".join(names) or relative
            if not isinstance(terms, list) or not all(isinstance(t, str) for t in terms):
                report.add(
                    GLOSSARY_TERMS_CHECK,
                    Severity.ERROR,
                    where,
                    f"openmetadata.glossary must be a list of glossary term FQNs, got {terms!r}",
                    f"Declared in {relative}.",
                )
                continue
            for fqn in terms:
                if fqn not in declared:
                    report.add(
                        GLOSSARY_TERMS_CHECK,
                        Severity.ERROR,
                        where,
                        f"Glossary term {fqn!r} is not declared in {glossary_dir.name}/",
                        f"Declared in {relative}. Use the term's fully qualified name, e.g. "
                        "open-learning.identifiability.direct-identifier, and add a new term to the glossary "
                        "file in the same change. OpenMetadata's dbt ingestion skips an unknown term with only "
                        "a warning.",
                    )

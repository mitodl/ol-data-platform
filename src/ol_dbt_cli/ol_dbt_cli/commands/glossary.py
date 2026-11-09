"""`ol-dbt glossary` — maintain the OpenMetadata business glossary in ``glossary/``.

The credential-free check is ``ol-dbt validate --only glossary_terms``; see
``ol_dbt_cli.lib.glossary_registry``. ``sync`` publishes the files to
OpenMetadata; see ``ol_dbt_cli.lib.glossary_sync`` for what it creates and
patches.
"""

from __future__ import annotations

import sys
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any

from cyclopts import App, Parameter
from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.git_utils import get_repo_root
from ol_dbt_cli.lib.glossary_registry import DEFAULT_GLOSSARY_DIR, GlossaryFile, GlossaryTermSpec, load_glossaries
from ol_dbt_cli.lib.glossary_sync import (
    RELATED_TERMS_FIELD,
    glossary_create_body,
    glossary_ops,
    related_terms_op,
    related_terms_plan,
    term_create_body,
    term_field_ops,
)
from ol_dbt_cli.lib.openmetadata import JSON_PATCH_CONTENT_TYPE, OpenMetadataClient, OpenMetadataError, quote_fqn

if TYPE_CHECKING:
    from collections.abc import Callable

console = Console()
err_console = Console(stderr=True)

glossary_app = App(
    name="glossary",
    help="Maintain the OpenMetadata business glossary in glossary/.",
)

# Sync output labels are padded to this many columns.
_LABEL_WIDTH = 10
HTTP_BAD_REQUEST = 400
# The id a dry run gives a term it would create, so later terms can link to it.
_PLANNED_ID = "<created by this sync>"


def _say(style: str, label: str, subject: str, message: str) -> None:
    console.print(f"[{style}]{label.ljust(_LABEL_WIDTH)}[/]{escape(subject)}: {escape(message)}")


def _deleted(subject: str, entity: dict[str, Any]) -> bool:
    """Whether a steward soft-deleted the glossary or term, which sync then leaves alone."""
    if entity.get("deleted"):
        _say("yellow", "skipped", subject, "deleted in OpenMetadata, left alone")
        return True
    return False


def _apply(client: OpenMetadataClient, subject: str, path: str, ops: list[dict[str, Any]], *, dry_run: bool) -> None:
    if not ops:
        _say("green", "unchanged", subject, "matches the file")
        return
    fields = ", ".join(op["path"].lstrip("/") for op in ops)
    if dry_run:
        _say("green", "would", subject, f"PATCH {fields}")
        return
    client.request("PATCH", path, ops, content_type=JSON_PATCH_CONTENT_TYPE)
    _say("green", "patched", subject, fields)


def _create(client: OpenMetadataClient, collection: str, subject: str, body: dict[str, Any], *, dry_run: bool) -> None:
    if dry_run:
        _say("green", "would", subject, "create")
        return
    client.request("POST", f"/v1/{collection}", body)
    _say("green", "created", subject, "created")


def _sync_glossary(client: OpenMetadataClient, glossary: GlossaryFile, planned: set[str], *, dry_run: bool) -> None:
    """Create the glossary with its owners and reviewers, or patch its description."""
    spec = glossary.glossary
    subject = f"glossary {spec.name}"
    live = client.get_or_none(f"/v1/glossaries/name/{quote_fqn(spec.name)}", query={"include": "all"})
    if live is not None:
        if not _deleted(subject, live):
            _apply(client, subject, f"/v1/glossaries/{live['id']}", glossary_ops(spec, live), dry_run=dry_run)
        return
    owners = client.named_refs([p.model_dump() for p in spec.owners])
    reviewers = client.named_refs([p.model_dump() for p in spec.reviewers])
    _create(client, "glossaries", subject, glossary_create_body(spec, owners, reviewers), dry_run=dry_run)
    if dry_run:
        planned.add(spec.name)


def _related_term_id(client: OpenMetadataClient, fqn: str, planned: set[str]) -> str:
    if fqn in planned:
        return _PLANNED_ID
    related = client.get_or_none(f"/v1/glossaryTerms/name/{quote_fqn(fqn)}")
    if related is None:
        msg = f"related term {fqn} is missing or deleted in OpenMetadata"
        raise ValueError(msg)
    return related["id"]


def _sync_term(
    client: OpenMetadataClient, glossary: GlossaryFile, term: GlossaryTermSpec, planned: set[str], *, dry_run: bool
) -> None:
    """Create the term when it is missing and its parent and glossary exist, else patch the fields that differ."""
    fqn = glossary.term_fqn(term.name)
    subject = f"glossary term {fqn}"
    live = client.get_or_none(
        f"/v1/glossaryTerms/name/{quote_fqn(fqn)}", query={"include": "all", "fields": RELATED_TERMS_FIELD}
    )
    if live is not None:
        if _deleted(subject, live):
            return
        ops = term_field_ops(term, live)
        plan = related_terms_plan(glossary, term, live.get(RELATED_TERMS_FIELD) or [])
        if plan is not None:
            kept, added = plan
            ops.append(related_terms_op(kept, [_related_term_id(client, f, planned) for f in added]))
        _apply(client, subject, f"/v1/glossaryTerms/{live['id']}", ops, dry_run=dry_run)
        return

    # The server answers 404 for a parent or glossary that is missing or
    # soft-deleted before its duplicate-name check, e.g. for the children of a
    # term a steward moved, which the file still declares under the old parent.
    ancestors = [("glossaries", glossary.glossary.name)]
    if term.parent:
        ancestors.append(("glossaryTerms", glossary.term_fqn(term.parent)))
    for collection, ancestor_fqn in ancestors:
        if ancestor_fqn in planned:
            continue
        ancestor = client.get_or_none(f"/v1/{collection}/name/{quote_fqn(ancestor_fqn)}", query={"include": "all"})
        if ancestor is None or ancestor.get("deleted"):
            _say("yellow", "skipped", subject, f"{ancestor_fqn} is missing or deleted in OpenMetadata, left alone")
            return
    try:
        _create(client, "glossaryTerms", subject, term_create_body(glossary, term), dry_run=dry_run)
    except OpenMetadataError as exc:
        # Term names are unique across a glossary, case-insensitively. A term a
        # steward moved under another parent is missing at this FQN but still
        # holds the name, and the server answers 400.
        if exc.status != HTTP_BAD_REQUEST or "already exists" not in str(exc):
            raise
        _say("yellow", "skipped", subject, "name held elsewhere in the glossary, left alone")
        return
    if dry_run:
        planned.add(fqn)


@glossary_app.command
def sync(
    *,
    dry_run: Annotated[
        bool,
        Parameter(name=["--dry-run"], help="Read the glossary and report what would change; write nothing."),
    ] = False,
    glossary_dir_path: Annotated[
        str | None,
        Parameter(name=["--glossary-dir"], help="Glossary directory. Defaults to glossary/ in the repository."),
    ] = None,
) -> None:
    """Create each glossary and term in OpenMetadata that is missing, and put back the fields the file owns.

    Reads OM_SERVER_URL and OM_BOT_JWT_TOKEN. The glossary's description, and
    each term's description, synonyms, references and related terms, are
    patched back to the file wherever they differ, overwriting an edit made in
    OpenMetadata. displayName, mutuallyExclusive and the glossary's owners and
    reviewers are set only on create, and entityStatus is never written, so
    approval stays with the glossary's reviewers in OpenMetadata. A
    soft-deleted glossary or term is left alone, as is a missing term whose
    parent is missing or deleted. Exits non-zero when any glossary or term
    failed to sync; the others still sync.
    """
    glossary_dir = Path(glossary_dir_path).resolve() if glossary_dir_path else get_repo_root() / DEFAULT_GLOSSARY_DIR
    try:
        glossaries = load_glossaries(glossary_dir)
    except ValueError as exc:
        err_console.print(f"[red]Error:[/] {escape(str(exc))}")
        sys.exit(1)
    client = OpenMetadataClient.from_env()
    failures = 0
    for _, glossary in glossaries:
        # What a dry run would create, so the terms under it read as creatable.
        planned: set[str] = set()
        steps: list[tuple[str, Callable[[], None]]] = [
            (
                f"glossary {glossary.glossary.name}",
                partial(_sync_glossary, client, glossary, planned, dry_run=dry_run),
            ),
            *(
                (
                    f"glossary term {glossary.term_fqn(term.name)}",
                    partial(_sync_term, client, glossary, term, planned, dry_run=dry_run),
                )
                for term in glossary.terms
            ),
        ]
        for subject, step in steps:
            try:
                step()
            except (ValueError, RuntimeError) as exc:
                _say("bold red", "failed", subject, str(exc))
                failures += 1
    if failures:
        sys.exit(1)

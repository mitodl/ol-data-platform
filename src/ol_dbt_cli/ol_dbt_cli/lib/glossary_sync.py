"""What `ol-dbt glossary sync` sends to OpenMetadata for a glossary file, and when.

The file is the source of truth. A missing glossary or term is created by
POST. After that only these fields are put back, by JSON Patch, whenever they
differ: the glossary's description, and each term's description, synonyms,
references and related terms. An edit to them in the OpenMetadata UI is
overwritten on the next sync, so it belongs in a pull request.

displayName and mutuallyExclusive are set only on create: every bot is denied
EditDisplayName (DefaultBotPolicy), and OpenMetadata ignores a change to
mutuallyExclusive on a glossary or term that exists. So are the glossary's
owners and reviewers. Sync never writes ``entityStatus``. The sync bot is not
a glossary reviewer, so OpenMetadata's glossary approval workflow creates each
term In Review, and puts a term whose fields sync patched back In Review with
an approval task for the reviewers; rejecting it rolls the change back until
the next sync.

A glossary or term a steward soft-deleted is left alone, as is a missing term
whose parent or glossary is missing or soft-deleted, and one whose name the
glossary already holds under another parent (a term moved in the UI). Renaming
a term or hard-deleting it frees its name, so the next sync creates it again.

This is ol-infrastructure's ``om_server_config`` glossary reconciliation moved
here unchanged, so the terms and the dbt columns that name them change in one
pull request.
"""

from __future__ import annotations

from collections.abc import Callable, Hashable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ol_dbt_cli.lib.glossary_registry import GlossaryFile, GlossarySpec, GlossaryTermSpec

GLOSSARY_PATCHED_FIELDS = ("description",)
# relatedTerms is reconciled on its own, because the server returns links
# rather than the names a file declares.
TERM_PATCHED_FIELDS = ("description", "synonyms", "references")
RELATED_TERMS_FIELD = "relatedTerms"

# How the server matches the entries of a term's list fields (stringMatch and
# termReferenceMatch in OpenMetadata's EntityUtil). It compares them as sets
# and stores nothing for a change that only reorders one, so a PATCH sent for
# a different order would be sent again on every sync.
_MEMBER_KEYS: dict[str, Callable[[Any], Hashable]] = {
    "synonyms": lambda synonym: synonym,
    "references": lambda reference: (reference["name"], reference["endpoint"]),
}


def add_ops(current: dict[str, Any], wanted: dict[str, Any], fields: tuple[str, ...]) -> list[dict[str, Any]]:
    """Build JSON Patch ``add`` operations for the *fields* whose value differs.

    ``add`` rather than ``replace``, because the server leaves an empty field
    out of the entity and ``replace`` fails on a missing member.
    """
    return [
        {"op": "add", "path": f"/{field}", "value": wanted[field]}
        for field in fields
        if current.get(field) != wanted[field]
    ]


def glossary_ops(spec: GlossarySpec, live: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the operations that put the live glossary's patched fields back to the file's."""
    return add_ops(live, spec.model_dump(), GLOSSARY_PATCHED_FIELDS)


def glossary_create_body(
    spec: GlossarySpec, owners: list[dict[str, str]], reviewers: list[dict[str, str]]
) -> dict[str, Any]:
    """Build the CreateGlossary body, with *owners* and *reviewers* resolved to ``{id, type}`` references."""
    return {**spec.model_dump(exclude={"owners", "reviewers"}), "owners": owners, "reviewers": reviewers}


def term_field_ops(term: GlossaryTermSpec, live: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the operations that put the live term's description, synonyms and references back to the file's."""
    wanted = term.model_dump(include=set(TERM_PATCHED_FIELDS))
    current = dict(live)
    for field, key in _MEMBER_KEYS.items():
        # The server leaves an empty list out of the entity.
        if {key(value) for value in live.get(field) or []} == {key(value) for value in wanted[field]}:
            current[field] = wanted[field]
    return add_ops(current, wanted, TERM_PATCHED_FIELDS)


def term_create_body(glossary: GlossaryFile, term: GlossaryTermSpec) -> dict[str, Any]:
    """Build the CreateGlossaryTerm body: the fields the file sets, with parent and related terms as FQNs."""
    body: dict[str, Any] = {
        "glossary": glossary.glossary.name,
        **term.model_dump(exclude_unset=True, exclude={"parent", RELATED_TERMS_FIELD}),
    }
    if term.parent:
        body["parent"] = glossary.term_fqn(term.parent)
    if term.relatedTerms:
        body[RELATED_TERMS_FIELD] = [glossary.term_fqn(name) for name in term.relatedTerms]
    return body


def related_terms_plan(
    glossary: GlossaryFile, term: GlossaryTermSpec, current: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], list[str]] | None:
    """Work out how the term's related-term links have to change.

    OpenMetadata stores a link on both of its terms, and a file declares it
    once, on the later term. So links to terms declared after *term* belong to
    them and are kept as they are. Links are compared by the related term's
    FQN, so a relation type a steward set on a declared link is kept too.

    :param current: the term's relatedTerms as the server returned them, each
        a TermRelation (``relationType`` and a ``term`` entity reference).
    :returns: the links to keep and the FQNs of the terms to link, or ``None``
        when the links already match.
    :rtype: tuple[list[dict[str, Any]], list[str]] | None
    """
    index = glossary.terms.index(term)
    later = {glossary.term_fqn(other.name) for other in glossary.terms[index + 1 :]}
    wanted = {glossary.term_fqn(name) for name in term.relatedTerms}
    owned = {link["term"]["fullyQualifiedName"] for link in current} - later
    if owned == wanted:
        return None
    kept = [link for link in current if link["term"]["fullyQualifiedName"] in later | wanted]
    return kept, sorted(wanted - owned)


def related_terms_op(kept: list[dict[str, Any]], added_ids: list[str]) -> dict[str, Any]:
    """Build the operation that sets relatedTerms to *kept* plus a ``relatedTo`` link to each of *added_ids*."""
    added = [{"relationType": "relatedTo", "term": {"id": term_id, "type": "glossaryTerm"}} for term_id in added_ids]
    return {"op": "add", "path": f"/{RELATED_TERMS_FIELD}", "value": kept + added}

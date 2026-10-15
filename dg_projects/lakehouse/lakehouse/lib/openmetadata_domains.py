"""OpenMetadata domains for dbt manifest nodes, assigned by name rule.

OpenMetadata's dbt ingestion sets a table's domain from the node's
``meta.openmetadata.domain``. The rules in src/ol_dbt/openmetadata_domains.yml
fill that key in the manifest uploaded for OpenMetadata, so a new model gets a
domain without anyone editing its config. See that file for why the rules are
not dbt config.
"""

import fnmatch
from collections import Counter
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, model_validator

# The node types OpenMetadata turns into tables. Tests, analyses and operations
# have no table to carry a domain.
_TABLE_RESOURCE_TYPES = frozenset({"model", "snapshot", "seed"})


class DomainRule(BaseModel):
    """Nodes whose name matches any of ``match`` belong to ``domain``.

    :param domain: Domain name, or None for a node that belongs to no domain.
    :param match: Shell-style globs, compared case-insensitively.
    """

    domain: str | None
    match: list[str]

    def matches(self, name: str) -> bool:
        """Whether *name* matches any of this rule's globs."""
        return any(
            fnmatch.fnmatchcase(name.lower(), pattern.lower()) for pattern in self.match
        )


class DomainRules(BaseModel):
    """The ordered rules and the domain names they may use."""

    domains: list[str]
    rules: list[DomainRule]

    @model_validator(mode="after")
    def rules_use_known_domains(self) -> "DomainRules":
        """Reject a rule naming a domain missing from ``domains``.

        OpenMetadata skips an unknown domain with a warning in a nightly job's
        log, so a typo here would otherwise go unnoticed.
        """
        if unknown := {
            rule.domain
            for rule in self.rules
            if rule.domain is not None and rule.domain not in self.domains
        }:
            msg = f"rules name domains not listed in `domains`: {sorted(unknown)}"
            raise ValueError(msg)
        return self

    def rule_for(self, name: str) -> DomainRule | None:
        """Return the first rule matching *name*, or None if no rule does."""
        return next((rule for rule in self.rules if rule.matches(name)), None)


def load_domain_rules(path: Path) -> DomainRules:
    """Read and validate the rules file at *path*."""
    return DomainRules.model_validate(yaml.safe_load(path.read_text()))


def _table_nodes(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the manifest nodes OpenMetadata ingests as tables, sources included."""
    nodes = [
        node
        for node in manifest["nodes"].values()
        if node["resource_type"] in _TABLE_RESOURCE_TYPES
    ]
    nodes.extend(manifest["sources"].values())
    return nodes


def assign_domains(
    manifest: dict[str, Any], rules: DomainRules
) -> tuple[Counter[str], list[str]]:
    """Set ``meta.openmetadata.domain`` on each table node that has none.

    A node that already declares a domain keeps it. Modifies *manifest* in
    place.

    :param manifest: A parsed dbt manifest.json.
    :param rules: The domain rules.
    :returns: Nodes per domain, counting a node set by its own config under its
        domain and a node a ``domain: null`` rule matched under ``"(none)"``;
        and the sorted names of the nodes no rule matched.
    """
    counts: Counter[str] = Counter()
    unassigned: list[str] = []
    for node in _table_nodes(manifest):
        openmetadata = node["meta"].get("openmetadata", {})
        if domain := openmetadata.get("domain"):
            counts[domain] += 1
            continue
        rule = rules.rule_for(node["name"])
        if rule is None:
            unassigned.append(node["name"])
        elif rule.domain is None:
            counts["(none)"] += 1
        else:
            node["meta"]["openmetadata"] = {**openmetadata, "domain": rule.domain}
            counts[rule.domain] += 1
    return counts, sorted(unassigned)

"""Per-model QA branch contracts — RFC 12711 step 4.

Models above staging union several ingestion units. A QA build of one does not
fail when a branch is empty; it emits a partial result that downstream joins
silently lose rows against. The contract makes the model say which branches it
expects in QA, as ``config.meta``:

    qa_branches: [mitxonline/app_postgres, xpro/app_postgres]   # expected in QA
    qa_buildable: false                                          # no QA form at all

Branches are inventory unit keys (``deployment/layer``), never source or
platform names: ``(deployment, layer)`` is the unit that actually lapses. See
docs/specs/QA_DATA_TOPOLOGY_SPEC.md §2.

This module makes the declaration complete and consistent with lineage. Checking
it against the inventory's QA strategies and against what QA actually holds is
step 5, which extends the same check.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ol_dbt_cli.lib.dimensional_layering import classify_layer
from ol_dbt_cli.lib.inventory import tables_by_raw_name
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from ol_dbt_cli.lib.inventory import Unit
    from ol_dbt_cli.lib.manifest import ManifestRegistry

QA_CONTRACT_CHECK = "qa_branch_contract"

UNCONTRACTED_LAYERS = frozenset({"staging"})
"""Staging models read exactly one source table, so they are never a union."""

_BRANCH = re.compile(r"^[a-z][a-z0-9_]*/[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class Contract:
    branches: list[str] | None
    buildable: bool | None


def upstream_units(manifest: ManifestRegistry, units: list[Unit]) -> dict[str, set[str]]:
    """Map each model name to every inventory unit its lineage reads from.

    A source whose table no unit declares (a retired table, a dbt-built source
    like the feedback tables) contributes nothing: it is not an ingestion branch
    QA could hold or lack.
    """
    owners = tables_by_raw_name(units)
    memo: dict[str, set[str]] = {}

    def walk(unique_id: str) -> set[str]:
        if unique_id in memo:
            return memo[unique_id]
        node = manifest.nodes.get(unique_id)
        found: set[str] = set()
        if node is not None and node.resource_type == "source":
            owner = owners.get(node.identifier or node.name)
            if owner:
                found.add(owner)
        elif node is not None:
            for parent in node.depends_on:
                found |= walk(parent)
        memo[unique_id] = found
        return found

    return {node.name: walk(uid) for uid, node in manifest.nodes.items() if node.is_model}


def _read_contract(meta: dict[str, Any]) -> tuple[Contract, list[str]]:
    """Parse the two meta keys, returning shape problems instead of raising."""
    problems = []
    branches = meta.get("qa_branches")
    buildable = meta.get("qa_buildable")

    if branches is not None:
        if not isinstance(branches, list) or not all(isinstance(b, str) for b in branches):
            problems.append("qa_branches must be a list of `deployment/layer` strings")
            branches = None
        elif not branches:
            problems.append("qa_branches is empty; a model with no QA branches declares `qa_buildable: false`")
        else:
            problems.extend(
                f"qa_branches entry {b!r} is not a `deployment/layer` unit key"
                for b in branches
                if not _BRANCH.match(b)
            )
            if len(set(branches)) != len(branches):
                problems.append("qa_branches lists a branch more than once")

    if buildable is not None and buildable is not False:
        problems.append("qa_buildable only takes `false`; a buildable model says so by declaring qa_branches")

    return Contract(branches=branches, buildable=buildable), problems


def check_qa_contracts(manifest: ManifestRegistry, units: list[Unit], report: ValidationReport) -> None:
    """Report models whose QA contract is missing, malformed, or contradicts lineage.

    Every finding is an ERROR and none is baselineable: each is fixed by editing
    the model's YAML, and none can be caused by an upstream outage.
    """
    lineage = upstream_units(manifest, units)
    scope = {unit.key: unit.data.get("scope") for unit in units}

    for node in sorted(manifest.nodes.values(), key=lambda n: n.name):
        if not node.is_model:
            continue
        reads = lineage.get(node.name, set())
        contract, problems = _read_contract(node.meta)

        for problem in problems:
            report.add(QA_CONTRACT_CHECK, Severity.ERROR, node.name, problem)

        if contract.buildable is False and contract.branches is not None:
            report.add(
                QA_CONTRACT_CHECK,
                Severity.ERROR,
                node.name,
                "declares both qa_buildable: false and qa_branches",
                "The two contradict each other. Keep qa_branches if any QA form of the model "
                "exists, otherwise keep qa_buildable: false.",
            )

        for branch in sorted(set(contract.branches or []) - reads):
            if not _BRANCH.match(branch):
                continue
            report.add(
                QA_CONTRACT_CHECK,
                Severity.ERROR,
                node.name,
                f"qa_branches names {branch}, which is not upstream of this model",
                f"Lineage reaches: {', '.join(sorted(reads)) or 'no inventory unit'}. A declared "
                "branch the model cannot read asserts coverage nothing checks.",
            )

        is_union = len(reads) > 1 and classify_layer(node.original_file_path) not in UNCONTRACTED_LAYERS
        if is_union and contract.branches is None and contract.buildable is None:
            scoped = sorted(key for key in reads if scope.get(key) == "scoped")
            suggestion = f"qa_branches: [{', '.join(scoped)}]" if scoped else "qa_buildable: false"
            report.add(
                QA_CONTRACT_CHECK,
                Severity.ERROR,
                node.name,
                f"unions {len(reads)} ingestion units but declares no QA contract",
                f"Reads {', '.join(sorted(reads))}. Add to config.meta the branches this model "
                f"must have in QA — every scoped unit would be `{suggestion}` — or "
                "`qa_buildable: false` if it has no QA form. See "
                "docs/specs/QA_DATA_TOPOLOGY_SPEC.md §2.",
            )

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

The check has two halves with different severities (spec §2). A declaration
that is missing, malformed, contradicts lineage, or names a unit the inventory
omits from QA is fixed by editing text, so it is an ERROR nothing can baseline.
A declared branch whose tables QA does not hold, or a mirror past its
``mirror_max_age_days``, is an operational lapse upstream: an ERROR when new,
INFO when listed in ``qa_branch_baseline.txt``. That half reads a committed
observation of the QA lake (``lib.qa_observation``), since CI has no AWS access.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from ol_dbt_cli.lib.dimensional_layering import classify_layer
from ol_dbt_cli.lib.inventory import tables_by_raw_name
from ol_dbt_cli.lib.qa_observation import QA_GLUE_DATABASE, QA_STRATEGIES, qa_strategy
from ol_dbt_cli.lib.validation import Severity, ValidationReport

if TYPE_CHECKING:
    from pathlib import Path

    from ol_dbt_cli.lib.inventory import Unit
    from ol_dbt_cli.lib.manifest import ManifestRegistry
    from ol_dbt_cli.lib.qa_observation import Observation, TableState

QA_CONTRACT_CHECK = "qa_branch_contract"
BASELINE_FILENAME = "qa_branch_baseline.txt"
OBSERVATION_MAX_AGE_DAYS = 30
"""Past this the observation, not QA, is what the gap findings describe."""

UNCONTRACTED_LAYERS = frozenset({"staging"})
"""Staging models read exactly one source table, so they are never a union."""

_BRANCH = re.compile(r"^[a-z][a-z0-9_]*/[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class Contract:
    branches: list[str] | None
    buildable: bool | None


def upstream_tables(manifest: ManifestRegistry, units: list[Unit]) -> dict[str, set[str]]:
    """Map each model name to every inventory-declared raw table its lineage reads.

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
            table = node.identifier or node.name
            if table in owners:
                found.add(table)
        elif node is not None:
            for parent in node.depends_on:
                found |= walk(parent)
        memo[unique_id] = found
        return found

    return {node.name: walk(uid) for uid, node in manifest.nodes.items() if node.is_model}


def upstream_units(manifest: ManifestRegistry, units: list[Unit]) -> dict[str, set[str]]:
    """Map each model name to every inventory unit its lineage reads from."""
    owners = tables_by_raw_name(units)
    return {model: {owners[t] for t in tables} for model, tables in upstream_tables(manifest, units).items()}


def _read_contract(meta: dict[str, Any]) -> tuple[Contract, list[str]]:
    """Parse the two meta keys, returning shape problems instead of raising."""
    problems = []
    branches = meta.get("qa_branches")
    buildable = meta.get("qa_buildable")

    # Key presence, not a non-null value: dbt keeps `qa_branches:` with nothing
    # under it in config.meta as None, and reading that as an absent key lets a
    # half-written declaration pass on a model that unions nothing.
    if "qa_branches" in meta:
        if not isinstance(branches, list) or not all(isinstance(b, str) for b in branches):
            problems.append("qa_branches must be a list of `deployment/layer` strings")
            branches = None
        elif not branches:
            problems.append("qa_branches is empty; a model with no QA branches declares `qa_buildable: false`")
            branches = None
        else:
            problems.extend(
                f"qa_branches entry {b!r} is not a `deployment/layer` unit key"
                for b in branches
                if not _BRANCH.match(b)
            )
            if len(set(branches)) != len(branches):
                problems.append("qa_branches lists a branch more than once")

    if "qa_buildable" in meta and buildable is not False:
        problems.append("qa_buildable only takes `false`; a buildable model says so by declaring qa_branches")

    return Contract(branches=branches, buildable=buildable), problems


def check_qa_contracts(manifest: ManifestRegistry, units: list[Unit], report: ValidationReport) -> None:
    """Report models whose QA contract is missing, malformed, or contradicts lineage or the inventory.

    Every finding is an ERROR and none is baselineable: each is fixed by editing
    the model's YAML or the inventory, and none can be caused by an upstream outage.
    """
    lineage = upstream_units(manifest, units)
    scope = {unit.key: unit.data.get("scope") for unit in units}
    strategies = {unit.key: qa_strategy(unit) for unit in units}

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

        for branch in sorted(set(contract.branches or [])):
            if not _BRANCH.match(branch):
                continue
            if branch not in strategies:
                report.add(
                    QA_CONTRACT_CHECK,
                    Severity.ERROR,
                    node.name,
                    f"qa_branches names {branch}, which is not an inventory unit",
                    "Branches are `deployment/layer` keys of ingestion/inventory/units. A branch "
                    "no unit declares can never be ingested or mirrored into QA.",
                )
            elif branch not in reads:
                report.add(
                    QA_CONTRACT_CHECK,
                    Severity.ERROR,
                    node.name,
                    f"qa_branches names {branch}, which is not upstream of this model",
                    f"Lineage reaches: {', '.join(sorted(reads)) or 'no inventory unit'}. A declared "
                    "branch the model cannot read asserts coverage nothing checks.",
                )
            elif strategies[branch] not in QA_STRATEGIES:
                report.add(
                    QA_CONTRACT_CHECK,
                    Severity.ERROR,
                    node.name,
                    f"qa_branches names {branch}, whose strategies.qa is {strategies[branch]}",
                    "The model expects the branch in QA and the inventory says QA never gets it. "
                    "Set the unit's strategies.qa to ingest (scoped) or mirror (singleton), or drop "
                    "the branch here so this model's QA form is partial by declaration. Not "
                    "baselineable: see docs/specs/QA_DATA_TOPOLOGY_SPEC.md §2.",
                )

        is_union = len(reads) > 1 and classify_layer(node.original_file_path) not in UNCONTRACTED_LAYERS
        # Presence again: a malformed value is already reported above, and _read_contract
        # nulls it out, so reading the parsed contract would report it as missing too.
        if is_union and "qa_branches" not in node.meta and "qa_buildable" not in node.meta:
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


@dataclass(frozen=True)
class Gap:
    """One table of a declared branch that QA does not hold in usable form."""

    branch: str
    table: str
    condition: str
    reason: str
    models: tuple[str, ...]

    @property
    def key(self) -> str:
        """Baseline identity.

        Per table, so a model newly reading an empty table of a baselined branch
        is still a new finding.
        """
        return f"{self.branch} {self.table}: {self.condition}"


def _condition(state: TableState, mirror_max_age: timedelta | None, observed_at: datetime) -> tuple[str, str] | None:
    if not state.present:
        return "empty", "absent from QA raw"
    if not state.iceberg:
        return "empty", "not Iceberg (legacy JSON destination)"
    if state.rows is None:
        return "empty", "no current snapshot"
    if state.rows == 0:
        return "empty", "no rows"
    if mirror_max_age is not None and state.snapshot_at and observed_at - state.snapshot_at > mirror_max_age:
        return "stale", f"copied {state.snapshot_at:%Y-%m-%d}, past mirror_max_age_days"
    return None


def qa_gaps(manifest: ManifestRegistry, units: list[Unit], observation: Observation) -> list[Gap]:
    """Return the declared-branch tables QA lacks or the observation never saw.

    Only tables a declaring model actually reads count: an empty table in a unit
    no declaring model touches cannot make a QA build partial. Branches the
    inventory check already rejects (unknown, not upstream, `omit`) are left to it.

    A table missing from the observation is a gap too, not a warning: it is what
    a newly declared or newly ingested branch looks like before anyone has shown
    QA holds it, which is exactly when QA is least likely to.
    """
    owners = tables_by_raw_name(units)
    by_key = {unit.key: unit for unit in units}
    lineage = upstream_tables(manifest, units)

    readers: dict[tuple[str, str], set[str]] = defaultdict(set)
    for node in manifest.nodes.values():
        if not node.is_model:
            continue
        contract, _ = _read_contract(node.meta)
        declared = {
            branch
            for branch in contract.branches or []
            if branch in by_key and qa_strategy(by_key[branch]) in QA_STRATEGIES
        }
        for table in lineage[node.name]:
            if owners[table] in declared:
                readers[(owners[table], table)].add(node.name)

    gaps: list[Gap] = []
    for (branch, table), models in sorted(readers.items()):
        state = observation.tables.get(table)
        if state is None:
            found: tuple[str, str] | None = (
                "unobserved",
                "not in the QA observation; refresh it with `ol-dbt inventory observe`",
            )
        else:
            unit = by_key[branch]
            max_age = timedelta(days=unit.data["mirror_max_age_days"]) if qa_strategy(unit) == "mirror" else None
            found = _condition(state, max_age, observation.observed_at)
        if found:
            gaps.append(Gap(branch, table, *found, models=tuple(sorted(models))))
    return gaps


def check_qa_gaps(  # noqa: PLR0913
    manifest: ManifestRegistry,
    units: list[Unit],
    observation: Observation,
    baseline: set[str],
    now: datetime,
    report: ValidationReport,
) -> None:
    """Ratchet the operational half: new gaps ERROR, baselined ones collapse to INFO."""
    age = now - observation.observed_at
    if age > timedelta(days=OBSERVATION_MAX_AGE_DAYS):
        report.add(
            QA_CONTRACT_CHECK,
            Severity.WARNING,
            "(qa observation)",
            f"The QA observation is {age.days} days old",
            "Gap findings describe QA as of "
            f"{observation.observed_at:%Y-%m-%d}. Refresh with `ol-dbt inventory observe` and commit it.",
        )

    if observation.glue_database != QA_GLUE_DATABASE:
        report.add(
            QA_CONTRACT_CHECK,
            Severity.ERROR,
            "(qa observation)",
            f"The QA observation was taken from {observation.glue_database}, not {QA_GLUE_DATABASE}",
            "An observation of another environment hides every QA gap. Re-run "
            "`ol-dbt inventory observe` against the QA database.",
        )

    gaps = qa_gaps(manifest, units, observation)

    by_group: dict[tuple[str, str], list[Gap]] = defaultdict(list)
    for gap in gaps:
        if gap.key not in baseline:
            by_group[(gap.branch, gap.condition)].append(gap)
    for (branch, condition), group in sorted(by_group.items()):
        models = sorted({model for gap in group for model in gap.models})
        report.add(
            QA_CONTRACT_CHECK,
            Severity.ERROR,
            branch,
            f"{len(group)} table(s) that declaring models read are {condition} in QA",
            f"{'; '.join(f'{gap.table} ({gap.reason})' for gap in group)}. Declared by "
            f"{len(models)} model(s): {', '.join(models)}. Restore the branch in QA, drop it "
            "from those models' qa_branches, or acknowledge the lapse with "
            "`ol-dbt validate --update-qa-baseline`.",
        )

    current = {gap.key for gap in gaps}
    known = current & baseline
    if known:
        report.add(
            QA_CONTRACT_CHECK,
            Severity.INFO,
            "(qa baseline)",
            f"{len(known)} known QA gap(s) tolerated by baseline",
            f"See {BASELINE_FILENAME}. Each line is a declared-branch table QA does not hold yet.",
        )
    for resolved in sorted(baseline - current):
        report.add(
            QA_CONTRACT_CHECK,
            Severity.INFO,
            "(qa baseline)",
            f"Resolved baseline entry: {resolved}",
            "QA holds this table now, or no declaring model reads it. Run "
            "`ol-dbt validate --update-qa-baseline` to shrink the baseline.",
        )


def render_qa_baseline(gaps: list[Gap]) -> str:
    header = [
        "# QA branch-contract baseline (RFC 12711 step 5).",
        "#",
        "# Each line is a table of a declared QA branch that the committed QA",
        "# observation (qa_observation.json) shows empty, a mirror past its",
        "# mirror_max_age_days, or a table the observation does not cover yet",
        "# (unobserved). These are operational lapses, tolerated so QA builds",
        "# keep running while they are repaired. A gap NOT listed here fails",
        "# `ol-dbt validate`. Contradictions between qa_branches and the inventory",
        "# are never baselined. See docs/specs/QA_DATA_TOPOLOGY_SPEC.md §2.",
        "#",
        "# Regenerate with: ol-dbt validate --update-qa-baseline",
        "",
    ]
    return "\n".join([*header, *sorted({gap.key for gap in gaps}), ""])


def write_qa_baseline(path: Path, gaps: list[Gap]) -> None:
    path.write_text(render_qa_baseline(gaps))

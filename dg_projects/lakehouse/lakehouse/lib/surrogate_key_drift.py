"""Detect surrogate-key drift between builds and escalate to --full-refresh.

A dimension materialized ``table`` re-derives every ``generate_surrogate_key``
value on every build. Its incremental descendants stored the previous value as
a foreign key and only revisit rows inside their watermark, so editing the
hashed column list orphans every historical FK — silently, because no column
changed for ``on_schema_change`` to react to. It has happened twice in
production (``dim_discount.discount_pk``, #2411; the ``dim_user`` re-key,
#2618), both times needing a hand-run ``dbt build --full-refresh`` that only
memory and a doc comment were prompting anyone to do.

The comparison is against a small state artifact this module writes to S3 after
each successful build, not against the warehouse: the hashed column list only
exists in the model's source, so there is nothing live to read it back from.
That mirrors ``lakehouse.lib.starrocks_dbt``'s drift check, which compares the
manifest against StarRocks for the same reason in the other direction, and it
means the check fires on a re-key however it reached main.

Pure functions only — nothing here imports dagster or opens a connection, so it
is unit-testable without a parsed manifest on disk (importing the asset module
that calls it is not).
"""

from collections.abc import Mapping
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field
from typing import Any

from ol_dbt_cli.lib.manifest import registry_from_manifest
from ol_dbt_cli.lib.surrogate_keys import (
    KeyRegenFinding,
    SurrogateKeyState,
    changed_keys_from_state,
    detect_key_regen,
    surrogate_key_state,
)

SURROGATE_KEY_STATE_ARTIFACT = "surrogate-key-state.json"
"""S3 object (under DbtS3ArtifactsResource's prefix) holding the last build's
surrogate-key hash inputs. Written only after the repair build succeeds, so a
failed run leaves the drift pending and the next run retries it."""


@dataclass
class SurrogateKeyDrift:
    """What this build must full-refresh, and the state to record afterwards."""

    current_state: SurrogateKeyState
    findings: list[KeyRegenFinding] = field(default_factory=list)

    @property
    def models(self) -> list[str]:
        """Every incremental model holding an FK that the new hash invalidates.

        The full set, ignoring what any one run rebuilt — see
        :meth:`resolved_against` for what a subset build can actually repair.
        """
        return sorted({name for f in self.findings for name in f.affected_model_names})

    def describe(self) -> str:
        """One line per re-keyed column, for the run log."""
        return "; ".join(
            f"{f.ancestor}.{f.changed_key_column} re-keyed "
            f"({_join(f.base_inputs)} -> {_join(f.current_inputs)}), "
            f"stale in: {', '.join(f.affected_model_names)}"
            for f in self.findings
        )

    def resolved_against(self, built: AbstractSet[str]) -> tuple[list[str], bool]:
        """``(models to full-refresh now, whether the drift is fully handled)``.

        The one thing a run must have done before its repair can be trusted is
        rebuild the re-keyed dimension: the fact tables have to read its *new*
        keys, and refreshing them against a dimension this run never touched
        would copy the old ones straight back in. It is materialized ``table``,
        so a single build re-keys it whole and being in *built* is the entire
        precondition.

        The affected fact tables do NOT have to be in *built*. The repair names
        them in its own ``--select``, so it rebuilds them whether or not
        Dagster's automation conditions put them in this run — and requiring
        them here would deadlock the repair outright.
        ``upstream_or_code_changes`` selects the dimension on the tick its code
        version changes, and gates its facts behind ``any_deps_updated``, which
        cannot be true until that build has finished; ``~any_deps_in_progress``
        keeps them out of the same run besides. The dimension and its facts
        therefore land in *different* runs by construction: the first would
        find no facts to refresh and the second no dimension, and the drift
        would sit unrepaired forever while both runs reported success.
        """
        ready = [f for f in self.findings if f.ancestor in built]
        models = sorted({m for f in ready for m in f.affected_model_names})
        return models, len(ready) == len(self.findings)


def _join(calls: list[list[str]]) -> str:
    return " + ".join(", ".join(call) for call in calls)


def detect_drift(
    manifest: Mapping[str, Any], previous_state: Mapping[str, Any] | None
) -> SurrogateKeyDrift:
    """Compare *manifest*'s surrogate keys against *previous_state*.

    *previous_state* of None is the first run after this check ships (or after
    the bucket was emptied): the returned drift has no findings, so the build
    proceeds normally and the current state becomes the baseline. Escalating
    instead would full-refresh every fact table in the warehouse on the
    strength of having nothing to compare against.
    """
    current_state = surrogate_key_state(manifest)
    if previous_state is None:
        return SurrogateKeyDrift(current_state=current_state)
    changed = changed_keys_from_state(previous_state, current_state)
    if not changed:
        return SurrogateKeyDrift(current_state=current_state)
    return SurrogateKeyDrift(
        current_state=current_state,
        findings=detect_key_regen(changed, registry_from_manifest(dict(manifest))),
    )


def full_refresh_build_args(
    models: list[str], build_vars: list[str] | None = None
) -> list[str]:
    """``dbt build`` args that rebuild exactly *models* from scratch.

    The selector is one space-separated string rather than one argv entry per
    model: dbt accepts both, but only this spelling is stable across the two
    ways dbt's CLI has parsed multi-value options.

    *build_vars* carries through whatever ``--vars`` the surrounding build uses
    (the dev schema suffix), since a repair run against a different schema than
    the build it is repairing would rebuild the wrong relations.
    """
    return [
        "build",
        "--full-refresh",
        "--select",
        " ".join(models),
        *(build_vars or []),
    ]

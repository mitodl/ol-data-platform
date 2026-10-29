"""Report where QA raw has drifted from the committed qa_observation.json.

`ol-dbt validate`'s qa_branch_contract check reads which QA raw tables hold
rows from ingestion/inventory/qa_observation.json, because CI has no AWS
credentials to look. The file is refreshed by hand (`ol-dbt inventory
observe`), so a QA load or a QA mirror that lands a table leaves CI checking
against a file that no longer describes QA. #2897's observation was stale
within three hours.

This compares the file baked into the image with QA's Glue and Iceberg
metadata now, and fails the run when a table's emptiness differs or the table
sets do. The failed step reaches Sentry through the failure hook every
lakehouse asset carries (with_failure_hooks), which is the prompt to
refresh. Every failure here is raised permanent: the committed file is baked
into the image, so a rerun compares the same file and only repeats the Sentry
event. It runs in QA, where the Dagster role already reads
ol_warehouse_qa_raw (iceberg_raw_orphan_files lists the same tables there).
The mirrors run in production, so a check attached to them would not.

It also fails when a mirrored table's copy is older than its unit's
mirror_max_age_days, measured from now. The qa_branch_contract check measures
that age at the committed observation's observed_at, and ageing changes no
table's emptiness, so without this nothing would ever prompt the refresh that
lets the contract see it.

The comparison lives in `ol_dbt_cli.lib.qa_observation`, the same code
`ol-dbt inventory observe` and `ol-dbt validate` use.
"""

from dagster import AssetExecutionContext, MetadataValue, Output, asset
from ol_dbt_cli.lib.inventory import load_units
from ol_dbt_cli.lib.qa_mirror import stale_copies
from ol_dbt_cli.lib.qa_observation import (
    OBSERVATION_FILENAME,
    holds_rows,
    load_observation,
    observation_drift,
    observe_glue,
    observed_tables,
)
from ol_orchestrate.lib.failures import permanent_failure

from lakehouse.lib.inventory import INVENTORY_DIR


@asset(
    name="qa_observation_drift",
    group_name="ingestion_inventory",
    description=(
        "Diff QA raw against ingestion/inventory/qa_observation.json. Fails the "
        "run when the committed observation is wrong about which tables hold rows."
    ),
    compute_kind="glue",
)
def qa_observation_drift(context: AssetExecutionContext) -> Output[dict[str, int]]:
    """Fail when QA raw no longer matches the committed observation."""
    units = load_units(INVENTORY_DIR)
    if not units:
        msg = f"No inventory units found under {INVENTORY_DIR}."
        raise permanent_failure(msg)
    committed = load_observation(INVENTORY_DIR / OBSERVATION_FILENAME)
    if committed is None:
        msg = f"No {OBSERVATION_FILENAME} under {INVENTORY_DIR}."
        raise permanent_failure(msg)

    live = observe_glue(observed_tables(units), committed.glue_database)
    findings = observation_drift(committed, live)
    stale = stale_copies(units, live)
    for line in [*findings, *stale]:
        context.log.warning(line)

    counts = {
        "tables_observed": len(live.tables),
        "tables_holding_rows": sum(map(holds_rows, live.tables.values())),
        "drifted_tables": len(findings),
        "stale_mirror_tables": len(stale),
    }
    metadata = {
        **{key: MetadataValue.int(value) for key, value in counts.items()},
        "committed_observed_at": MetadataValue.text(committed.observed_at.isoformat()),
        "findings": MetadataValue.md(
            "\n".join(f"- {line}" for line in findings) or "No drift."
        ),
        "stale_mirrors": MetadataValue.md(
            "\n".join(f"- {line}" for line in stale) or "No stale mirror copies."
        ),
    }
    problems = []
    if findings:
        problems.append(
            f"{len(findings)} QA raw table(s) differ from {OBSERVATION_FILENAME} "
            f"(observed {committed.observed_at:%Y-%m-%d %H:%MZ}). Run `ol-dbt "
            "inventory observe`, then `ol-dbt validate --update-qa-baseline`, and "
            "commit both files."
        )
    if stale:
        problems.append(
            f"{len(stale)} mirrored QA table(s) are older than their unit's "
            "mirror_max_age_days. Production's qa_mirror_monthly_job should have "
            "refreshed them: check its runs there."
        )
    if problems:
        raise permanent_failure(" ".join(problems), metadata=metadata)
    return Output(counts, metadata=metadata)

"""Validate command - validate Superset asset files."""

import sys
from pathlib import Path
from typing import Annotated

import yaml
from cyclopts import Parameter

from ol_superset.lib.asset_index import AssetIndex, build_asset_index
from ol_superset.lib.dbt_registry import (
    build_dbt_registry,
    extract_sql_table_refs,
)
from ol_superset.lib.role_management import (
    find_governance_roles_json,
    get_local_datasets,
    load_governance_roles,
)
from ol_superset.lib.utils import count_assets, get_assets_dir


def validate(
    assets_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--assets-dir", "-d"],
            help="Assets directory to validate (default: assets/)",
        ),
    ] = None,
    dbt_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--dbt-dir", "-b"],
            help=(
                "Path to dbt project root (contains dbt_project.yml). "
                "When provided, validates the full Dashboard → Chart → "
                "Dataset → dbt model dependency chain including column "
                "names and schema correctness."
            ),
        ),
    ] = None,
) -> None:
    """
    Validate Superset asset definitions.

    Checks YAML syntax, counts assets, and looks for common issues
    like embedded passwords in database configs.

    When --dbt-dir is supplied, also validates the full dependency chain:
    Dashboard chart UUID references, Chart dataset UUID references,
    Dataset table names and schemas against dbt models, and column name
    consistency across the entire chain.

    Examples:
        Validate default assets directory:
            ol-superset validate

        Validate custom directory:
            ol-superset validate --assets-dir /tmp/qa-backup

        Validate with full dbt dependency chain checks:
            ol-superset validate --dbt-dir ../../ol_dbt
    """
    assets_dir = get_assets_dir(assets_dir_path)

    print("=" * 50)
    print("Validating Superset Assets")
    print("=" * 50)
    print()

    if not assets_dir.exists():
        print(f"Error: Assets directory not found: {assets_dir}", file=sys.stderr)
        sys.exit(1)

    # Count assets
    counts = count_assets(assets_dir)
    published = counts["published_dashboards"]
    total_dashboards = counts["dashboards"]
    drafts = total_dashboards - published

    print("Asset inventory:")
    print(f"  Dashboards: {total_dashboards} ({published} published, {drafts} drafts)")
    print(f"  Charts:     {counts['charts']}")
    print(f"  Datasets:   {counts['datasets']}")
    print(f"  Databases:  {counts['databases']}")
    print()

    # Validate YAML syntax
    print("Checking YAML syntax...")
    errors = []
    checked = 0

    for yaml_file in sorted(assets_dir.rglob("*.yaml")):
        try:
            with yaml_file.open() as f:
                yaml.safe_load(f)
            checked += 1
        except Exception as e:
            errors.append((yaml_file, str(e)))

    if errors:
        print(
            f"  ❌ Found {len(errors)} invalid YAML file(s) "
            f"out of {checked + len(errors)} total:"
        )
        for file, error in errors:
            print(f"     {file.relative_to(assets_dir)}: {error}")
        sys.exit(1)

    print(f"  ✅ All {checked} YAML files are syntactically valid")
    print()

    # Check for common issues
    print("Checking for common issues...")
    warnings = 0

    # Check for database passwords
    db_dir = assets_dir / "databases"
    if db_dir.exists():
        for db_file in db_dir.glob("*.yaml"):
            with db_file.open() as f:
                content = f.read()
                if "password" in content and not content.startswith("#"):
                    print(
                        f"  ⚠️  Warning: Password found in {db_file.name}",
                        file=sys.stderr,
                    )
                    warnings += 1

    if warnings == 0:
        print("  ✅ No security issues detected")

    # Check governance role schema coverage
    print()
    print("Checking governance role schema coverage...")
    gov_json = find_governance_roles_json(assets_dir)
    if gov_json is None:
        print("  ⚠️  ol_governance_roles.json not found; skipping schema coverage check")
    else:
        governance_roles = load_governance_roles(gov_json)
        local_datasets = get_local_datasets(assets_dir)

        # Only check warehouse (Trino) datasets - other DBs (e.g. Superset
        # Metadata DB) are not subject to warehouse schema governance.
        trino_datasets = [ds for ds in local_datasets if ds.get("database") == "Trino"]

        covered_schemas: set[str] = set()
        for role in governance_roles:
            covered_schemas.update(role.get("allowed_schemas", []))

        dataset_schemas: set[str] = {
            ds["schema"] for ds in trino_datasets if ds.get("schema")
        }
        uncovered = dataset_schemas - covered_schemas

        if uncovered:
            warnings += len(uncovered)
            for schema in sorted(uncovered):
                count = sum(1 for ds in trino_datasets if ds.get("schema") == schema)
                print(
                    f"  ⚠️  Schema not covered by any governance role: "
                    f"{schema} ({count} dataset(s))"
                )
            print(
                "      Add the missing schema(s) to ol_governance_roles.json "
                "and run 'ol-superset roles sync'."
            )
        else:
            print(
                f"  ✅ All {len(dataset_schemas)} dataset schema(s) covered "
                "by governance roles"
            )

    # Optionally validate the full dbt dependency chain
    errors = 0
    if dbt_dir_path is not None:
        print()
        errors, warnings = _validate_dbt_chain(
            assets_dir=assets_dir,
            dbt_dir=Path(dbt_dir_path).resolve(),
            existing_warnings=warnings,
        )
    else:
        print()
        print("Skipping dbt dependency chain checks (use --dbt-dir to enable).")

    print()
    print("=" * 50)
    print("Validation Complete!")
    print("=" * 50)
    print()
    print("Summary:")
    print(
        f"  ✅ Exported: {total_dashboards} dashboards, "
        f"{counts['charts']} charts, {counts['datasets']} datasets"
    )
    print("  ✅ All YAML files are valid")
    if errors > 0:
        print(f"  ❌ {errors} error(s) found — fix before deploying")
    if warnings == 0 and errors == 0:
        print("  ✅ Ready for version control")
    elif warnings > 0:
        print(f"  ⚠️  {warnings} warning(s) found — review above before deploying")
    print()

    if errors > 0:
        sys.exit(1)


def _validate_dbt_chain(
    assets_dir: Path,
    dbt_dir: Path,
    existing_warnings: int,
) -> tuple[int, int]:
    """
    Validate the full Dashboard → Chart → Dataset → dbt model dependency chain.

    Args:
        assets_dir: Path to the Superset assets directory.
        dbt_dir: Path to the dbt project root.
        existing_warnings: Warning count accumulated by the caller so far.

    Returns:
        Tuple of (error_count, warning_count) where warning_count includes
        existing_warnings plus any new ones found here.
    """
    print("Checking dbt dependency chain...")

    if not dbt_dir.exists():
        print(
            f"  ❌ dbt directory not found: {dbt_dir}",
            file=sys.stderr,
        )
        return 1, existing_warnings

    # Load dbt registry
    try:
        dbt_registry = build_dbt_registry(dbt_dir)
    except Exception as exc:  # noqa: BLE001
        print(f"  ❌ Failed to load dbt registry: {exc}", file=sys.stderr)
        return 1, existing_warnings

    model_count = len(dbt_registry.models)
    print(f"  Loaded {model_count} dbt models from {dbt_dir.name}/")

    # Build asset index
    index: AssetIndex = build_asset_index(assets_dir)

    errors = 0
    warnings = existing_warnings

    # ------------------------------------------------------------------
    # 1. Dashboard → Chart: all referenced chart UUIDs must exist locally
    # ------------------------------------------------------------------
    print()
    print("  [1/4] Dashboard → Chart UUID references...")
    dashboard_errors = 0
    for dash in index.dashboards.values():
        for chart_uuid in dash.chart_uuids:
            if chart_uuid not in index.charts:
                print(
                    f"    ❌ Dashboard '{dash.title}' references chart UUID "
                    f"{chart_uuid} which does not exist in assets/charts/"
                )
                errors += 1
                dashboard_errors += 1
    if dashboard_errors == 0:
        dash_count = len(index.dashboards)
        print(f"    ✅ All {dash_count} dashboard(s) have valid chart references")

    # ------------------------------------------------------------------
    # 2. Chart → Dataset: dataset_uuid must reference an existing dataset
    # ------------------------------------------------------------------
    print()
    print("  [2/4] Chart → Dataset UUID references...")
    chart_uuid_errors = 0
    for chart in index.charts.values():
        if chart.dataset_uuid is None:
            continue
        if chart.dataset_uuid not in index.datasets:
            print(
                f"    ❌ Chart '{chart.name}' references dataset UUID "
                f"{chart.dataset_uuid} which does not exist in assets/datasets/"
            )
            errors += 1
            chart_uuid_errors += 1
    if chart_uuid_errors == 0:
        print(f"    ✅ All {len(index.charts)} chart(s) have valid dataset references")

    # ------------------------------------------------------------------
    # 3. Dataset → dbt model: table_name must match a known dbt model
    # ------------------------------------------------------------------
    print()
    print("  [3/4] Dataset → dbt model (table name + schema)...")
    dataset_model_errors = 0
    dataset_schema_warnings = 0
    for dataset in index.datasets.values():
        if not dataset.table_name:
            continue
        # Skip non-Trino datasets (Superset Metadata DB etc.)
        if dataset.database != "Trino":
            continue
        # Skip virtual datasets — their table_name is a Superset-internal
        # identifier, not a dbt model name. SQL table refs are validated in [4/4].
        if dataset.sql:
            continue

        model = dbt_registry.get_model(dataset.table_name)
        if model is None:
            # If the dataset's schema is not a known dbt-managed schema, it may
            # be an Airbyte-sourced raw table or other external source — warn
            # rather than error since dbt doesn't own it.
            if dataset.schema not in dbt_registry.schema_to_layer:
                print(
                    f"    ⚠️  Dataset '{dataset.table_name}': no matching dbt model "
                    f"(schema '{dataset.schema}' is not a known dbt layer — "
                    f"may be a raw/external table)"
                )
                warnings += 1
                dataset_schema_warnings += 1
            else:
                print(
                    f"    ❌ Dataset '{dataset.table_name}' ({dataset.path.name}): "
                    f"no matching dbt model found in registry"
                )
                errors += 1
                dataset_model_errors += 1
            continue

        # Check schema alignment
        if dataset.schema and model.expected_schema:
            if dataset.schema != model.expected_schema:
                print(
                    f"    ⚠️  Dataset '{dataset.table_name}': schema mismatch — "
                    f"Superset has '{dataset.schema}', "
                    f"expected '{model.expected_schema}' for dbt layer '{model.layer}'"
                )
                warnings += 1
                dataset_schema_warnings += 1

    if dataset_model_errors == 0 and dataset_schema_warnings == 0:
        trino_count = sum(
            1 for d in index.datasets.values() if d.database == "Trino" and d.table_name
        )
        print(f"    ✅ All {trino_count} Trino dataset(s) map to known dbt models")

    # ------------------------------------------------------------------
    # 4. Column-level validation
    # ------------------------------------------------------------------
    print()
    print("  [4/4] Column-level consistency...")
    col_warnings = 0

    for dataset in index.datasets.values():
        if dataset.database != "Trino":
            continue

        model = (
            dbt_registry.get_model(dataset.table_name) if dataset.table_name else None
        )

        # 4a. Dataset columns vs dbt model columns (simple columns only)
        if model and model.columns and not dataset.sql:
            for col_name in sorted(dataset.columns):
                if col_name not in model.columns:
                    print(
                        f"    ⚠️  Dataset '{dataset.table_name}': column "
                        f"'{col_name}' not documented in dbt model YAML "
                        f"(may still exist in the warehouse)"
                    )
                    warnings += 1
                    col_warnings += 1

        # 4b. Virtual dataset: validate referenced table names exist in dbt
        if dataset.sql:
            table_refs = extract_sql_table_refs(dataset.sql)
            for ref_table in sorted(table_refs):
                if dbt_registry.get_model(ref_table) is None:
                    print(
                        f"    ⚠️  Virtual dataset '{dataset.table_name}': SQL "
                        f"references table '{ref_table}' not found in dbt registry"
                    )
                    warnings += 1
                    col_warnings += 1

        # 4c. Chart column refs vs dataset columns
        if not dataset.columns:
            continue
        for chart in index.charts.values():
            if chart.dataset_uuid != dataset.uuid:
                continue
            for col_ref in sorted(chart.column_refs):
                if col_ref not in dataset.columns:
                    print(
                        f"    ⚠️  Chart '{chart.name}': column '{col_ref}' "
                        f"not found in dataset '{dataset.table_name}' "
                        f"column list"
                    )
                    warnings += 1
                    col_warnings += 1

    if col_warnings == 0:
        print("    ✅ All column references are consistent")

    return errors, warnings

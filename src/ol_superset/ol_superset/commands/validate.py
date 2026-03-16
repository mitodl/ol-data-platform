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

    Always validates the asset dependency chain:
    Dashboard chart UUID references, and Chart dataset UUID references.

    When --dbt-dir is supplied, also validates Dataset table names and schemas
    against dbt models, and column name consistency across the entire chain.

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
    yaml_errors = []
    checked = 0

    for yaml_file in sorted(assets_dir.rglob("*.yaml")):
        try:
            with yaml_file.open() as f:
                yaml.safe_load(f)
            checked += 1
        except Exception as e:
            yaml_errors.append((yaml_file, str(e)))

    if yaml_errors:
        print(
            f"  ❌ Found {len(yaml_errors)} invalid YAML file(s) "
            f"out of {checked + len(yaml_errors)} total:"
        )
        for file, error in yaml_errors:
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

    # Validate asset dependency chain (always runs — no dbt dir required)
    print()
    total_steps = 5 if dbt_dir_path is not None else 3
    errors, warnings, asset_index = _validate_asset_references(
        assets_dir=assets_dir,
        existing_warnings=warnings,
        total_steps=total_steps,
    )

    if dbt_dir_path is not None:
        dbt_errors, warnings = _validate_dbt_chain(
            index=asset_index,
            dbt_dir=Path(dbt_dir_path).resolve(),
            existing_warnings=warnings,
            total_steps=total_steps,
        )
        errors += dbt_errors
    else:
        print()
        print("  ℹ️  Dataset → dbt model checks skipped (use --dbt-dir to enable).")

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


def _validate_asset_references(
    assets_dir: Path,
    existing_warnings: int,
    *,
    total_steps: int = 3,
) -> tuple[int, int, "AssetIndex"]:
    """
    Validate Dashboard → Chart, Chart → Dataset UUID cross-references, and
    Chart → Dataset column name case consistency.

    Always runs regardless of whether a dbt directory is provided.

    Args:
        assets_dir: Path to the Superset assets directory.
        existing_warnings: Warning count accumulated by the caller so far.
        total_steps: Total number of validation steps (used for labelling).

    Returns:
        Tuple of (error_count, warning_count, asset_index).
    """
    print("Checking asset dependency chain...")

    index: AssetIndex = build_asset_index(assets_dir)
    errors = 0
    warnings = existing_warnings

    # ------------------------------------------------------------------
    # 1. Dashboard → Chart: all referenced chart UUIDs must exist locally
    # ------------------------------------------------------------------
    print()
    print(f"  [1/{total_steps}] Dashboard → Chart UUID references...")
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
    print(f"  [2/{total_steps}] Chart → Dataset UUID references...")
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
    # 3. Chart → Dataset: column name case consistency
    #
    # Superset does exact-string matching when resolving chart column
    # references against the dataset column list.  Trino normalises all
    # column names to lowercase, so any mixed-case reference (e.g.
    # ``DEDP_Course_Cert_Count``) will fail at query time even if the
    # column exists in lowercase.  This check runs without a dbt registry
    # because it only needs the chart and dataset YAML files.
    # ------------------------------------------------------------------
    print()
    print(f"  [3/{total_steps}] Chart → Dataset column case consistency...")
    col_case_issues = 0

    for dataset in index.datasets.values():
        if dataset.database != "Trino":
            continue

        if dataset.sql:
            # Virtual dataset: compare against sqlglot-parsed SQL output
            # columns (already lowercase). Skip when columns are
            # indeterminate (wildcard SELECT or unparseable SQL).
            if dataset.virtual_columns is None:
                continue
            virtual_cols = dataset.virtual_columns  # already lowercase
            for chart in index.charts.values():
                if chart.dataset_uuid != dataset.uuid:
                    continue
                for col_ref in sorted(chart.column_refs):
                    col_ref_lower = col_ref.lower()
                    if col_ref in virtual_cols:
                        pass  # exact match — correct
                    elif col_ref_lower in virtual_cols:
                        print(
                            f"    ❌ Chart '{chart.name}': column '{col_ref}' "
                            f"matches virtual dataset '{dataset.table_name}' "
                            f"SQL output as '{col_ref_lower}' — "
                            f"Trino normalises column names to lowercase; "
                            f"rename to '{col_ref_lower}'"
                        )
                        errors += 1
                        col_case_issues += 1
                    else:
                        print(
                            f"    ⚠️  Chart '{chart.name}': column '{col_ref}' "
                            f"not found in virtual dataset "
                            f"'{dataset.table_name}' SQL output"
                        )
                        warnings += 1
                        col_case_issues += 1
            continue

        # Simple (dbt-backed) dataset: compare against declared column list.
        all_cols = dataset.columns | dataset.calculated_columns
        if not all_cols:
            continue
        all_cols_lower: dict[str, str] = {c.lower(): c for c in all_cols}
        for chart in index.charts.values():
            if chart.dataset_uuid != dataset.uuid:
                continue
            for col_ref in sorted(chart.column_refs):
                if col_ref not in all_cols:
                    if col_ref.lower() in all_cols_lower:
                        canonical = all_cols_lower[col_ref.lower()]
                        print(
                            f"    ❌ Chart '{chart.name}': column '{col_ref}' "
                            f"exists in dataset '{dataset.table_name}' as "
                            f"'{canonical}' — rename to '{canonical}'"
                        )
                        errors += 1
                        col_case_issues += 1
                    else:
                        print(
                            f"    ⚠️  Chart '{chart.name}': column '{col_ref}' "
                            f"not found in dataset '{dataset.table_name}' "
                            f"column list"
                        )
                        warnings += 1
                        col_case_issues += 1

    if col_case_issues == 0:
        print("    ✅ All chart column references match dataset column names")

    return errors, warnings, index


def _validate_dbt_chain(
    index: AssetIndex,
    dbt_dir: Path,
    existing_warnings: int,
    total_steps: int = 5,
) -> tuple[int, int]:
    """
    Validate the Dataset → dbt model and column-level dependency chain (steps 4–5).

    Requires a pre-built AssetIndex (from _validate_asset_references) so that
    the index is not reconstructed unnecessarily.

    Chart → Dataset column consistency (step 3) is handled by
    _validate_asset_references and always runs; this function only validates
    the dataset → dbt model link and dataset column alignment with dbt docs.

    Args:
        index: Pre-built AssetIndex for the assets directory.
        dbt_dir: Path to the dbt project root.
        existing_warnings: Warning count accumulated by the caller so far.
        total_steps: Total validation step count (used for step labels).

    Returns:
        Tuple of (error_count, warning_count) for steps 4–5 only.
    """

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

    errors = 0
    warnings = existing_warnings

    # ------------------------------------------------------------------
    # 4. Dataset → dbt model: table_name must match a known dbt model
    # ------------------------------------------------------------------
    print()
    print(f"  [4/{total_steps}] Dataset → dbt model (table name + schema)...")
    dataset_model_errors = 0
    dataset_schema_warnings = 0
    virtual_count = sum(
        1
        for d in index.datasets.values()
        if d.database == "Trino" and d.table_name and d.sql
    )
    if virtual_count:
        print(
            f"    ℹ️  {virtual_count} virtual dataset(s) skipped "
            f"(SQL-defined — table refs validated in [5/{total_steps}])"
        )

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
                    f"expected '{model.expected_schema}' for dbt layer "
                    f"'{model.layer}'"
                )
                warnings += 1
                dataset_schema_warnings += 1

    if dataset_model_errors == 0 and dataset_schema_warnings == 0:
        simple_count = sum(
            1
            for d in index.datasets.values()
            if d.database == "Trino" and d.table_name and not d.sql
        )
        print(
            f"    ✅ All {simple_count} simple Trino dataset(s) map to known dbt models"
        )

    # ------------------------------------------------------------------
    # 5. Column-level validation (dataset columns vs dbt model docs,
    #    and virtual dataset SQL table references).
    #
    #    Chart → Dataset column case consistency is step 3 and always
    #    runs; this step only covers the dbt-registry-dependent checks.
    # ------------------------------------------------------------------
    print()
    print(f"  [5/{total_steps}] Column-level consistency (dataset vs dbt model)...")
    col_warnings = 0

    for dataset in index.datasets.values():
        if dataset.database != "Trino":
            continue

        model = (
            dbt_registry.get_model(dataset.table_name) if dataset.table_name else None
        )

        # 5a. Dataset columns vs dbt model columns (plain columns only).
        # Calculated columns (those with a Superset ``expression``) are derived
        # at query time by Superset and don't correspond to warehouse columns, so
        # they are excluded from this check.
        # Virtual datasets are defined by SQL — their YAML column list is a
        # cached metadata snapshot and may be incomplete.
        if model and model.columns and not dataset.sql:
            model_cols_lower: dict[str, str] = {c.lower(): c for c in model.columns}
            for col_name in sorted(dataset.columns):
                if col_name not in model.columns:
                    if col_name.lower() in model_cols_lower:
                        canonical = model_cols_lower[col_name.lower()]
                        print(
                            f"    ❌ Dataset '{dataset.table_name}': column "
                            f"'{col_name}' has wrong case — dbt model documents "
                            f"it as '{canonical}' "
                            f"(Trino normalises column names to lowercase; "
                            f"update the dataset YAML)"
                        )
                        errors += 1
                        col_warnings += 1
                    else:
                        print(
                            f"    ⚠️  Dataset '{dataset.table_name}': column "
                            f"'{col_name}' not documented in dbt model YAML "
                            f"(may still exist in the warehouse)"
                        )
                        warnings += 1
                        col_warnings += 1

        # 5b. Virtual dataset: validate referenced table names exist in dbt,
        # and warn about SELECT * / table.* wildcards that prevent static
        # column analysis.
        if dataset.sql:
            if dataset.sql_has_wildcard:
                print(
                    f"    ⚠️  Virtual dataset '{dataset.table_name}': SQL uses "
                    f"SELECT * or table.* — output columns cannot be statically "
                    f"determined; replace with explicit column list"
                )
                warnings += 1
                col_warnings += 1

            table_refs = extract_sql_table_refs(dataset.sql)
            for ref_table in sorted(table_refs):
                if dbt_registry.get_model(ref_table) is None:
                    print(
                        f"    ⚠️  Virtual dataset '{dataset.table_name}': SQL "
                        f"references table '{ref_table}' not found in dbt registry"
                    )
                    warnings += 1
                    col_warnings += 1

    if col_warnings == 0:
        print("    ✅ All column references are consistent")

    return errors, warnings

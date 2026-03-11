"""Validate command - validate Superset asset files."""

import sys
from typing import Annotated

import yaml
from cyclopts import Parameter

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
) -> None:
    """
    Validate Superset asset definitions.

    Checks YAML syntax, counts assets, and looks for common issues
    like embedded passwords in database configs.

    Examples:
        Validate default assets directory:
            ol-superset validate

        Validate custom directory:
            ol-superset validate --assets-dir /tmp/qa-backup
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
        print(
            "  ⚠️  ol_governance_roles.json not found; skipping schema coverage check"
        )
    else:
        governance_roles = load_governance_roles(gov_json)
        local_datasets = get_local_datasets(assets_dir)

        # Only check warehouse (Trino) datasets - other DBs (e.g. Superset
        # Metadata DB) are not subject to warehouse schema governance.
        trino_datasets = [
            ds for ds in local_datasets if ds.get("database") == "Trino"
        ]

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
    if warnings == 0:
        print("  ✅ Ready for version control")
    else:
        print(f"  ⚠️  {warnings} warning(s) found — review above before deploying")
    print()

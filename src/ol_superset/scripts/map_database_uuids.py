#!/usr/bin/env python3
"""
Map database UUIDs from source to target environment.

Fetches database configurations from target Superset instance and rewrites
all database_uuid references in exported assets to match the target environment.
"""

import json
import sys
from pathlib import Path

import yaml


def get_target_databases(instance_name: str) -> dict[str, str]:
    """
    Fetch databases from target Superset instance using sup CLI.

    Returns:
        Dict mapping database_name to uuid
    """
    import re
    import subprocess

    # Get all databases with JSON output in one call
    result = subprocess.run(
        ["sup", "database", "list", "--instance", instance_name, "--json"],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(f"❌ Failed to fetch databases: {result.stderr}", file=sys.stderr)
        sys.exit(1)

    # Find the start of JSON array (skip spinner output)
    start_idx = result.stdout.find("[")
    if start_idx == -1:
        print("❌ No JSON found in database list output", file=sys.stderr)
        sys.exit(1)

    # Find the end of JSON array
    end_idx = result.stdout.rfind("]")
    if end_idx == -1:
        end_idx = len(result.stdout)
    else:
        end_idx += 1

    json_text = result.stdout[start_idx:end_idx]

    # The JSON output may have newlines inserted in string values due to line wrapping
    # This makes the JSON invalid. We'll use regex to extract just what we need.
    try:
        db_list = json.loads(json_text)
        return {db["database_name"]: db["uuid"] for db in db_list}
    except json.JSONDecodeError:
        # Fall back: extract UUID/name pairs using regex
        # (CLI wraps long lines which breaks JSON string values)
        databases = {}

        # Extract database_name and uuid pairs using regex
        # Look for patterns across potential line breaks
        name_pattern = r'"database_name":\s*"([^"]+)"'
        uuid_pattern = r'"uuid":\s*"([0-9a-f-]{36})"'

        names = re.findall(name_pattern, json_text)
        uuids = re.findall(uuid_pattern, json_text)

        # Match them up (assuming they appear in order)
        if len(names) == len(uuids):
            databases = dict(zip(names, uuids))
            return databases

        print(
            f"❌ Could not extract database information (found {len(names)} names, {len(uuids)} UUIDs)",
            file=sys.stderr,
        )
        debug_file = Path("/tmp/database_list_debug.json")
        debug_file.write_text(json_text)
        print(f"📝 JSON output written to {debug_file} for debugging", file=sys.stderr)
        sys.exit(1)


def load_source_databases(assets_dir: Path) -> dict[str, str]:
    """
    Load source database configurations from exported assets.

    Returns:
        Dict mapping database_name to uuid
    """
    db_dir = assets_dir / "databases"
    if not db_dir.exists():
        print(f"❌ No databases directory found in {assets_dir}", file=sys.stderr)
        sys.exit(1)

    databases = {}
    for db_file in db_dir.glob("*.yaml"):
        with open(db_file) as f:
            config = yaml.safe_load(f)
            databases[config["database_name"]] = config["uuid"]

    return databases


def build_uuid_mapping(
    source_dbs: dict[str, str], target_dbs: dict[str, str]
) -> dict[str, str]:
    """
    Build mapping from source UUIDs to target UUIDs based on database names.

    Returns:
        Dict mapping source_uuid to target_uuid
    """
    mapping = {}

    print(f"\nSource databases: {source_dbs}")
    print(f"Target databases: {target_dbs}\n")

    for db_name, source_uuid in source_dbs.items():
        if db_name not in target_dbs:
            print(
                f"⚠️  Warning: Database '{db_name}' not found in target environment",
                file=sys.stderr,
            )
            continue

        target_uuid = target_dbs[db_name]
        if source_uuid != target_uuid:
            mapping[source_uuid] = target_uuid
            print(f"📋 {db_name}: {source_uuid} → {target_uuid}")
        else:
            print(f"✓  {db_name}: UUID matches ({source_uuid})")

    return mapping


def rewrite_uuids_in_file(file_path: Path, uuid_mapping: dict[str, str]) -> int:
    """
    Rewrite database UUIDs in a YAML file.

    Returns:
        Number of replacements made
    """
    with open(file_path) as f:
        content = f.read()

    replacements = 0
    for source_uuid, target_uuid in uuid_mapping.items():
        if source_uuid in content:
            content = content.replace(source_uuid, target_uuid)
            replacements += 1

    if replacements > 0:
        with open(file_path, "w") as f:
            f.write(content)

    return replacements


def rewrite_all_assets(assets_dir: Path, uuid_mapping: dict[str, str]) -> None:
    """
    Rewrite database UUIDs in all asset files including database configs.
    """
    total_files = 0
    total_replacements = 0

    # Update database configs first
    db_dir = assets_dir / "databases"
    if db_dir.exists():
        for yaml_file in db_dir.glob("*.yaml"):
            replacements = rewrite_uuids_in_file(yaml_file, uuid_mapping)
            if replacements > 0:
                total_files += 1
                total_replacements += replacements

    # Update datasets, charts, dashboards
    for asset_type in ["datasets", "charts", "dashboards"]:
        asset_dir = assets_dir / asset_type
        if not asset_dir.exists():
            continue

        for yaml_file in asset_dir.rglob("*.yaml"):
            replacements = rewrite_uuids_in_file(yaml_file, uuid_mapping)
            if replacements > 0:
                total_files += 1
                total_replacements += replacements

    print(f"\n✅ Updated {total_replacements} UUID references in {total_files} files")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Map database UUIDs from exported assets to target environment"
    )
    parser.add_argument(
        "target_instance", help="Target Superset instance name (e.g., 'superset-qa')"
    )
    parser.add_argument(
        "--assets-dir",
        default="assets",
        help="Path to assets directory (default: assets)",
    )

    args = parser.parse_args()
    assets_dir = Path(args.assets_dir)

    if not assets_dir.exists():
        print("❌ Assets directory not found", file=sys.stderr)
        sys.exit(1)

    print("================================================")
    print("Mapping Database UUIDs for Target Environment")
    print("================================================\n")

    print("📥 Loading source databases from assets...")
    source_dbs = load_source_databases(assets_dir)
    print(f"   Found {len(source_dbs)} source databases\n")

    print(f"📥 Fetching target databases from {args.target_instance}...")
    target_dbs = get_target_databases(args.target_instance)
    print(f"   Found {len(target_dbs)} target databases\n")

    print("🔄 Building UUID mapping...")
    uuid_mapping = build_uuid_mapping(source_dbs, target_dbs)

    if not uuid_mapping:
        print("\n✅ No UUID changes needed - all databases match!")
        return

    print("\n📝 Rewriting UUIDs in asset files...")
    rewrite_all_assets(assets_dir, uuid_mapping)

    print("\n================================================")
    print("UUID Mapping Complete!")
    print("================================================")


if __name__ == "__main__":
    main()

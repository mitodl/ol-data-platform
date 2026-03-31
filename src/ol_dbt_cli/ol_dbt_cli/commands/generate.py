"""Generate commands for scaffolding dbt sources and staging models.

Provides commands for:
- Generating dbt source YAML definitions from database schemas
- Generating staging model SQL + YAML from source tables
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path

import yaml
from cyclopts import App

generate_app = App(
    name="generate",
    help="Scaffold dbt sources and staging models.",
)


def _extract_domain(prefix: str) -> str:
    """Extract the domain segment (second part) from a table prefix like raw__mitlearn__..."""
    parts = prefix.split("__")
    return parts[1] if len(parts) >= 2 else ""  # noqa: PLR2004


def _run_dbt_command(
    dbt_project_dir: str,
    command: list[str],
    target: str | None,
) -> subprocess.CompletedProcess[str]:
    """Run a dbt command or run-operation, returning the CompletedProcess result."""
    if len(command) == 1 and command[0] in {"parse", "compile", "run", "test", "snapshot"}:
        dbt_cmd = ["dbt", "--quiet", "--no-write-json", *command]
    else:
        dbt_cmd = ["dbt", "--quiet", "--no-write-json", "run-operation"]
        dbt_cmd.extend(command)

    dbt_cmd.extend(
        [
            "--project-dir",
            dbt_project_dir,
            "--profiles-dir",
            dbt_project_dir,
            "--vars",
            "{ 'schema_suffix': '' }",
        ]
    )
    if target:
        dbt_cmd.extend(["--target", target])

    try:
        result = subprocess.run(dbt_cmd, capture_output=True, text=True, check=True)  # noqa: S603
        result.stderr = "\n".join(line for line in result.stderr.splitlines() if "keyring module not found" not in line)
        return result  # noqa: TRY300
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt command: {' '.join(dbt_cmd)}")
        print(f"Return code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise


def _adjust_source_schema_pattern(content: str, original_schema: str | None = None) -> str:
    """Replace a hardcoded schema name with the standard dynamic pattern."""
    schema_to_replace = original_schema or "ol_warehouse_production_raw"
    adjusted = content.replace(f"name: {schema_to_replace}", "name: ol_warehouse_raw_data")

    lines = adjusted.split("\n")
    new_lines: list[str] = []
    in_source = False
    source_indent = 0

    for line in lines:
        if "- name: ol_warehouse_raw_data" in line:
            new_lines.append(line)
            source_indent = len(line) - len(line.lstrip())
            indent = " " * (source_indent + 2)
            new_lines.append(f"{indent}loader: airbyte")
            new_lines.append(f"{indent}database: '{{{{ target.database }}}}'")
            new_lines.append(
                f'{indent}schema: \'{{{{ target.schema.replace(var("schema_suffix", ""), "").rstrip("_") }}}}_raw\''
            )
            in_source = True
        elif line.strip().startswith("description:") and in_source:
            indent = " " * (source_indent + 2)
            new_lines.append(f'{indent}description: ""')
        elif line.strip().startswith("tables:") and in_source:
            indent = " " * (source_indent + 2)
            new_lines.append(f"{indent}tables:")
        elif line.strip().startswith("- name:") and "ol_warehouse_raw_data" not in line:
            new_lines.append(line)
            in_source = False
        elif not (
            line.strip().startswith("loader:")
            or line.strip().startswith("database:")
            or line.strip().startswith("schema:")
            or (line.strip().startswith("description:") and in_source)
        ):
            new_lines.append(line)
            if line.strip() and not line.startswith(" "):
                in_source = False

    return "\n".join(new_lines)


def _merge_sources_content(  # noqa: C901, PLR0912, PLR0915
    existing_content: str,
    new_content: str,
    original_schema: str | None = None,
) -> str:
    """Merge new source table definitions into an existing sources YAML file."""
    try:
        adjusted_new = _adjust_source_schema_pattern(new_content, original_schema)
        existing_yaml = yaml.safe_load(existing_content)
        new_yaml = yaml.safe_load(adjusted_new)

        if not existing_yaml or "sources" not in existing_yaml:
            print("Warning: Existing sources file has invalid structure, using new content")
            return adjusted_new

        if not new_yaml or "sources" not in new_yaml:
            print("Warning: New content has invalid structure, keeping existing content")
            return existing_content

        existing_sources = existing_yaml.get("sources", [])
        new_sources = new_yaml.get("sources", [])

        if not new_sources:
            return existing_content

        new_source = new_sources[0]
        source_name = new_source.get("name", "ol_warehouse_raw_data")

        existing_source = next((s for s in existing_sources if s.get("name") == source_name), None)

        if existing_source:
            existing_tables = {t["name"]: t for t in existing_source.get("tables", [])}

            for new_table in new_source.get("tables", []):
                tname = new_table["name"]

                if tname in existing_tables:
                    existing_table = existing_tables[tname]
                    existing_cols = {c["name"]: c for c in existing_table.get("columns", [])}

                    for new_col in new_table.get("columns", []):
                        col_name = new_col["name"]
                        if col_name in existing_cols:
                            if not existing_cols[col_name].get("description", "").strip():
                                existing_cols[col_name]["data_type"] = new_col.get("data_type", "")
                        else:
                            existing_cols[col_name] = new_col

                    existing_table["columns"] = sorted(existing_cols.values(), key=lambda x: x["name"])

                    if not existing_table.get("description", "").strip():
                        existing_table["description"] = new_table.get("description", "")
                else:
                    existing_tables[tname] = new_table

            existing_source["tables"] = sorted(existing_tables.values(), key=lambda x: x["name"])
        else:
            existing_sources.append(new_source)

        return yaml.dump(existing_yaml, default_flow_style=False, sort_keys=False, width=1000, indent=2)  # noqa: TRY300

    except yaml.YAMLError as e:
        print(f"YAML parsing error during merge: {e}")
        print("Falling back to appending new content")
        adjusted_fallback = _adjust_source_schema_pattern(new_content, original_schema)
        return existing_content + "\n\n# --- NEWLY ADDED TABLES ---\n" + adjusted_fallback
    except Exception as e:  # noqa: BLE001
        print(f"Unexpected error merging sources: {e}")
        adjusted_fallback = _adjust_source_schema_pattern(new_content, original_schema)
        return existing_content + "\n\n# --- NEWLY ADDED TABLES ---\n" + adjusted_fallback


# ============================================================================
# CLI Commands
# ============================================================================


@generate_app.command
def sources(  # noqa: C901, PLR0915
    schema: str,
    prefix: str,
    output_directory: str = ".",
    database: str | None = None,
    target: str | None = None,
) -> list[str] | None:
    """Generate a dbt sources YAML file for tables matching a schema and prefix.

    Discovers tables via dbt-codegen's generate_source macro and writes (or merges into)
    a _<domain>__sources.yml file in the appropriate staging subdirectory.

    Args:
        schema: Database schema to discover tables from.
        prefix: Table name prefix to filter by (e.g., raw__mitlearn__app__postgres__).
        output_directory: Subdirectory within models/staging/<domain>/ to write to.
        database: Optional database name override.
        target: Optional dbt target to use.

    """
    dbt_project_dir = Path("src") / "ol_dbt"
    domain = _extract_domain(prefix)
    staging_dir = dbt_project_dir / "models" / "staging"
    if domain:
        staging_dir = staging_dir / domain

    output_path = Path(output_directory)
    if not output_path.is_absolute():
        output_path = staging_dir / output_directory
    output_path.mkdir(parents=True, exist_ok=True)

    sources_filename = f"_{domain}__sources.yml" if domain else "_sources.yml"
    sources_file_path = output_path / sources_filename

    source_args: dict[str, object] = {
        "schema_name": schema,
        "generate_columns": True,
        "include_descriptions": True,
        "table_pattern": f"{prefix}%",
    }
    if database:
        source_args["database_name"] = database

    result = _run_dbt_command(str(dbt_project_dir), ["generate_source", "--args", json.dumps(source_args)], target)

    source_content_match = re.search(r"(version:.*)", result.stdout.strip(), re.DOTALL)
    if not source_content_match:
        return None

    generated_content = source_content_match.group(1)

    if sources_file_path.exists():
        print(f"Merging with existing sources file: {sources_file_path}")
        existing_content = sources_file_path.read_text()
        try:
            yaml.safe_load(existing_content)
        except yaml.YAMLError as e:
            print(f"Warning: Existing sources file has invalid YAML: {e}")
            backup_path = sources_file_path.with_suffix(".yml.backup")
            backup_path.write_text(existing_content)
            sources_file_path.write_text(_adjust_source_schema_pattern(generated_content, schema))
        else:
            merged = _merge_sources_content(existing_content, generated_content, schema)
            try:
                yaml.safe_load(merged)
                sources_file_path.write_text(merged)
            except yaml.YAMLError as e:
                print(f"Error: Merged content has invalid YAML: {e}")
                backup_path = sources_file_path.with_suffix(".yml.backup")
                backup_path.write_text(existing_content)
                sources_file_path.write_text(_adjust_source_schema_pattern(generated_content, schema))
    else:
        sources_file_path.write_text(_adjust_source_schema_pattern(generated_content, schema))

    print(f"Generated sources file: {sources_file_path}")

    discovered_tables: list[str] = []
    for table_match in re.findall(r"- name: ([^\n]+)", generated_content):
        name = table_match.strip()
        if name.startswith(prefix.rstrip("_")):
            discovered_tables.append(name)

    return discovered_tables


@generate_app.command
def staging_models(  # noqa: C901, PLR0912, PLR0913, PLR0915
    schema: str,
    prefix: str,
    tables: list[str] | None = None,
    directory: str | None = None,
    target: str | None = None,
    apply_transformations: bool = True,  # noqa: FBT001, FBT002
    entity_type: str | None = None,
) -> None:
    """Generate dbt staging SQL models and YAML for tables matching a schema and prefix.

    For each discovered table, generates a staging SQL file using the
    generate_base_model_enhanced macro, then produces a consolidated models YAML
    via generate_model_yaml_enhanced.

    Args:
        schema: Database schema to discover tables from.
        prefix: Table name prefix to filter by.
        tables: Explicit list of table names (skips discovery if provided).
        directory: Subdirectory within models/staging/ for the generated files.
        target: Optional dbt target to use.
        apply_transformations: Apply semantic column transformations (default: True).
        entity_type: Override auto-detected entity type for transformations.

    """
    dbt_project_dir = Path("src") / "ol_dbt"
    staging_dir = dbt_project_dir / "models" / "staging"

    if not directory:
        domain = _extract_domain(prefix)
        if domain:
            directory = domain

    if directory:
        staging_dir = staging_dir / directory
    staging_dir.mkdir(parents=True, exist_ok=True)

    discovered_tables = tables
    if not discovered_tables:
        source_args = {
            "schema_name": schema,
            "generate_columns": True,
            "include_descriptions": True,
            "table_pattern": f"{prefix}%",
        }
        result = _run_dbt_command(str(dbt_project_dir), ["generate_source", "--args", json.dumps(source_args)], target)
        discovered_tables = []
        for table_match in re.findall(r"- name: ([^\n]+)", result.stdout.strip()):
            name = table_match.strip()
            if name.startswith(prefix.rstrip("_")):
                discovered_tables.append(name)

    if not discovered_tables:
        return

    generated_models: list[str] = []

    for table_name in discovered_tables:
        source_match = re.match(r"raw__([a-z]+)__", table_name)
        if not source_match:
            continue
        source = source_match.group(1)

        source_prefix = f"raw__{source}__"
        table_suffix = table_name[len(source_prefix) :] if table_name.startswith(source_prefix) else table_name

        model_name = f"stg__{source}__{table_suffix}"
        sql_file_path = staging_dir / f"{model_name}.sql"

        try:
            base_model_args = json.dumps(
                {
                    "source_name": "ol_warehouse_raw_data",
                    "table_name": table_name,
                    "apply_transformations": apply_transformations,
                    "entity_type": entity_type,
                }
            )
            result_sql = _run_dbt_command(
                str(dbt_project_dir),
                ["generate_base_model_enhanced", "--args", base_model_args],
                target,
            )
            sql_match = re.search(r"(with source as.*)", result_sql.stdout.strip(), re.DOTALL)
            if sql_match:
                sql_file_path.write_text(sql_match.group(1))
                print(f"Generated SQL for {table_name}")
                generated_models.append(model_name)
            else:
                print(f"Could not extract SQL content for {table_name}")
        except Exception as e:  # noqa: BLE001
            print(f"Error processing table {table_name}: {e}")

    if not generated_models:
        return

    domain = _extract_domain(prefix)
    consolidated_yaml_path = staging_dir / f"_stg_{domain}__models.yml"

    try:
        print("Parsing dbt project to register new models...")
        try:
            _run_dbt_command(str(dbt_project_dir), ["parse"], target)
            print("dbt parse completed successfully")
        except Exception as e:  # noqa: BLE001
            print(f"Warning: dbt parse failed: {e} — continuing anyway")

        model_yaml_args = json.dumps(
            {
                "model_names": generated_models,
                "upstream_descriptions": True,
                "include_data_types": True,
            }
        )
        result_yaml = _run_dbt_command(
            str(dbt_project_dir),
            ["generate_model_yaml_enhanced", "--args", model_yaml_args],
            target,
        )

        yaml_match = re.search(r"(version:.*)", result_yaml.stdout.strip(), re.DOTALL)
        if not yaml_match:
            msg = "Failed to extract YAML content from generate_model_yaml_enhanced output"
            raise ValueError(msg)

        generated_yaml_content = yaml_match.group(1)

        if consolidated_yaml_path.exists():
            print(f"Merging with existing models YAML: {consolidated_yaml_path}")
            existing_yaml = yaml.safe_load(consolidated_yaml_path.read_text())
            new_yaml = yaml.safe_load(generated_yaml_content)

            existing_models_map = {m["name"]: m for m in existing_yaml.get("models", [])}
            for new_model in new_yaml.get("models", []):
                mname = new_model["name"]
                if mname in existing_models_map:
                    existing_model = existing_models_map[mname]
                    if existing_model.get("description", "").strip():
                        new_model["description"] = existing_model["description"]
                    existing_cols = {c["name"]: c for c in existing_model.get("columns", [])}
                    for new_col in new_model.get("columns", []):
                        col_has_desc = (
                            new_col["name"] in existing_cols
                            and existing_cols[new_col["name"]].get("description", "").strip()
                        )
                        if col_has_desc:
                            new_col["description"] = existing_cols[new_col["name"]]["description"]
                existing_models_map[mname] = new_model

            existing_yaml["models"] = sorted(existing_models_map.values(), key=lambda x: x["name"])
            consolidated_content = yaml.dump(
                existing_yaml, default_flow_style=False, sort_keys=False, width=1000, indent=2
            )
        else:
            print(f"Creating new models YAML: {consolidated_yaml_path}")
            consolidated_content = generated_yaml_content

        consolidated_yaml_path.write_text(consolidated_content)
        print(f"Generated consolidated YAML with {len(generated_models)} models")

    except Exception as e:  # noqa: BLE001
        print(f"Error generating model YAML: {e}")
        print("Falling back to basic YAML structure")
        basic = {"version": 2, "models": [{"name": m, "description": "", "columns": []} for m in generated_models]}
        consolidated_yaml_path.write_text(
            yaml.dump(basic, default_flow_style=False, sort_keys=False, width=1000, indent=2)
        )
        print(f"Generated basic YAML with {len(generated_models)} models")


@generate_app.command
def all(  # noqa: A001
    schema: str,
    prefix: str,
    database: str | None = None,
    target: str | None = None,
    apply_transformations: bool = True,  # noqa: FBT001, FBT002
    entity_type: str | None = None,
) -> None:
    """Generate both dbt sources YAML and staging models for a schema/prefix.

    Convenience command combining `generate sources` and `generate staging-models`.

    Args:
        schema: Database schema to discover tables from.
        prefix: Table name prefix to filter by.
        database: Optional database name override.
        target: Optional dbt target to use.
        apply_transformations: Apply semantic column transformations (default: True).
        entity_type: Override auto-detected entity type for transformations.

    """
    discovered = sources(schema, prefix, ".", database, target)

    if discovered:
        staging_models(
            schema,
            prefix,
            tables=discovered,
            target=target,
            apply_transformations=apply_transformations,
            entity_type=entity_type,
        )
        print(f"Generated staging models for {len(discovered)} tables")
    else:
        print("No tables found matching the specified criteria")

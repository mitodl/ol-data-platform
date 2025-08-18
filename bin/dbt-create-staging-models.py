#!/usr/bin/env python3
# ruff: noqa: T201, BLE001, UP045
"""
This script provides commands to generate dbt sources and staging models.
It interacts with dbt to discover tables and generate the necessary YAML and SQL files.
"""

import json
import re
import subprocess
from pathlib import Path
from typing import Optional

import yaml
from cyclopts import App

app = App()


def extract_domain_from_prefix(prefix: str) -> str:
    """
    Extract the domain (second section) from a table prefix.

    Args:
        prefix: The table prefix (e.g., 'raw__mitlearn__app__postgres__')

    Returns:
        The domain name (e.g., 'mitlearn')
    """
    parts = prefix.split("__")
    if len(parts) >= 2:  # noqa: PLR2004
        return parts[1]
    return ""


def run_dbt_command(
    dbt_project_dir: str, command: list[str], target: Optional[str]
) -> subprocess.CompletedProcess[str]:
    """
    Run a dbt command and captures its output.

    Args:
        dbt_project_dir: The directory of the dbt project.
        command: A list of strings representing the dbt command and its arguments.
        target: The dbt target to use.

    Returns:
        A CompletedProcess object containing the result of the dbt command.
    """
    # Check if this is a regular dbt command or a run-operation command
    if len(command) == 1 and command[0] in [
        "parse",
        "compile",
        "run",
        "test",
        "snapshot",
    ]:
        # Regular dbt command
        dbt_cmd = ["dbt", "--quiet", "--no-write-json", *command]
    else:
        # run-operation command (legacy behavior)
        dbt_cmd = [
            "dbt",
            "--quiet",
            "--no-write-json",
            "run-operation",
        ]
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

    # The `dbt_cmd` is constructed from trusted inputs, so this is safe.
    try:
        result = subprocess.run(dbt_cmd, capture_output=True, text=True, check=True)  # noqa: S603
        # Filter out keyring module not found messages from stderr
        filtered_stderr = "\n".join(
            line
            for line in result.stderr.splitlines()
            if "keyring module not found" not in line
        )
        result.stderr = filtered_stderr
        return result  # noqa: TRY300
    except subprocess.CalledProcessError as e:
        print(f"Error running dbt command: {' '.join(dbt_cmd)}")
        print(f"Return code: {e.returncode}")
        print(f"Stdout: {e.stdout}")
        print(f"Stderr: {e.stderr}")
        raise


@app.command
def generate_sources(  # noqa: C901, PLR0915
    schema: str,
    prefix: str,
    output_directory: str = ".",
    database: Optional[str] = None,
    target: Optional[str] = None,
):
    """
    Generate dbt sources YAML file for a given schema and table prefix.

    Args:
        schema: The database schema to generate sources for.
        prefix: The table prefix to filter tables by.
        output_directory: The directory where the source YAML file will be written.
        database: The database name (optional).
        target: The dbt target to use (optional).
    """
    dbt_project_dir = Path("src") / "ol_dbt"

    # Extract domain from prefix for subdirectory organization
    domain = extract_domain_from_prefix(prefix)
    staging_dir = dbt_project_dir / "models" / "staging"
    if domain:
        staging_dir = staging_dir / domain

    # Use the staging directory if output_directory is relative
    output_path = Path(output_directory)
    if not output_path.is_absolute():
        output_path = staging_dir / output_directory

    output_path.mkdir(parents=True, exist_ok=True)

    # Use standard naming pattern and source configuration
    sources_filename = f"_{domain}__sources.yml" if domain else "_sources.yml"
    sources_file_path = output_path / sources_filename

    # Use generate_source directly with table_pattern instead of separate discovery step
    source_args = {
        "schema_name": schema,  # Use the actual schema for discovery
        "generate_columns": True,
        "include_descriptions": True,
        "table_pattern": f"{prefix}%",  # Use table_pattern instead of table_names
    }
    if database:
        source_args["database_name"] = database

    result = run_dbt_command(
        str(dbt_project_dir),
        ["generate_source", "--args", json.dumps(source_args)],
        target,
    )

    source_content_match = re.search(r"(version:.*)", result.stdout.strip(), re.DOTALL)
    if not source_content_match:
        return None

    generated_content = source_content_match.group(1)

    # Handle merging with existing sources file
    if sources_file_path.exists():
        print(f"Merging with existing sources file: {sources_file_path}")
        existing_content = sources_file_path.read_text()

        # Validate existing content
        try:
            yaml.safe_load(existing_content)
        except yaml.YAMLError as e:
            print(f"Warning: Existing sources file has invalid YAML: {e}")
            print("Creating backup and using new content")
            backup_path = sources_file_path.with_suffix(".yml.backup")
            backup_path.write_text(existing_content)
            adjusted_content = adjust_source_schema_pattern(generated_content, schema)
            sources_file_path.write_text(adjusted_content)
        else:
            merged_content = merge_sources_content(
                existing_content, generated_content, schema
            )

            # Validate merged content before writing
            try:
                yaml.safe_load(merged_content)
                sources_file_path.write_text(merged_content)
            except yaml.YAMLError as e:
                print(f"Error: Merged content has invalid YAML: {e}")
                print("Creating backup and using new content instead")
                backup_path = sources_file_path.with_suffix(".yml.backup")
                backup_path.write_text(existing_content)
                adjusted_content = adjust_source_schema_pattern(
                    generated_content, schema
                )
                sources_file_path.write_text(adjusted_content)
    else:
        # Adjust the generated content to use standard source schema pattern
        adjusted_content = adjust_source_schema_pattern(generated_content, schema)
        sources_file_path.write_text(adjusted_content)

    print(f"Generated sources file: {sources_file_path}")

    # Extract discovered tables from the generated sources for use in staging models
    # Parse the YAML to get table names that were actually found
    discovered_tables = []
    tables_section = re.findall(r"- name: ([^\n]+)", generated_content)
    for table_match in tables_section:
        table_name = table_match.strip()
        if table_name.startswith(
            prefix.rstrip("_")
        ):  # Only include tables matching our prefix
            discovered_tables.append(table_name)

    return discovered_tables


def merge_sources_content(  # noqa: C901, PLR0912, PLR0915
    existing_content: str,
    new_content: str,
    original_schema: Optional[str] = None,
) -> str:
    """
    Merge new source table definitions with existing sources file.

    Args:
        existing_content: Content of existing sources YAML file
        new_content: New source content to merge in
        original_schema: The original schema name (unused but kept for compatibility)

    Returns:
        Merged YAML content
    """
    try:
        # First adjust the new content to use standard schema pattern
        adjusted_new_content = adjust_source_schema_pattern(
            new_content, original_schema
        )

        existing_yaml = yaml.safe_load(existing_content)
        new_yaml = yaml.safe_load(adjusted_new_content)

        # Ensure both have the correct structure
        if not existing_yaml or "sources" not in existing_yaml:
            print(
                "Warning: Existing sources file has invalid structure, using new content"  # noqa: E501
            )
            return adjusted_new_content

        if not new_yaml or "sources" not in new_yaml:
            print(
                "Warning: New content has invalid structure, keeping existing content"
            )
            return existing_content

        # Find the source section in both
        existing_sources = existing_yaml.get("sources", [])
        new_sources = new_yaml.get("sources", [])

        if not new_sources:
            return existing_content

        new_source = new_sources[0]  # Should only be one source from generate_source
        source_name = new_source.get("name", "ol_warehouse_raw_data")

        # Find matching source in existing content
        existing_source = None
        for source in existing_sources:
            if source.get("name") == source_name:
                existing_source = source
                break

        if existing_source:
            # Merge tables, preserving existing table data and only adding new columns
            existing_tables = {
                table["name"]: table for table in existing_source.get("tables", [])
            }
            new_tables = new_source.get("tables", [])

            # Add new tables or merge with existing ones
            for new_table in new_tables:
                table_name = new_table["name"]

                if table_name in existing_tables:
                    # Table already exists - merge columns while preserving existing
                    # data
                    existing_table = existing_tables[table_name]

                    # Preserve existing table-level properties (description, meta, etc.)
                    # Only update columns by merging new columns with existing ones
                    existing_columns = {}
                    for col in existing_table.get("columns", []):
                        existing_columns[col["name"]] = col

                    new_columns = new_table.get("columns", [])

                    # Add or update columns, preserving existing column data
                    for new_col in new_columns:
                        col_name = new_col["name"]
                        if col_name in existing_columns:
                            # Column exists - preserve existing description, meta, etc.
                            # Only update data_type if it wasn't manually set
                            existing_col = existing_columns[col_name]

                            # Update data_type only if existing description is empty or
                            # default (indicating it wasn't manually edited)
                            if (
                                not existing_col.get("description")
                                or existing_col.get("description").strip() == ""
                            ):
                                existing_col["data_type"] = new_col.get("data_type", "")

                            # Keep the existing column with its preserved data
                        else:
                            # New column - add it
                            existing_columns[col_name] = new_col

                    # Update the existing table's columns
                    existing_table["columns"] = sorted(
                        existing_columns.values(), key=lambda x: x["name"]
                    )

                    # Update table-level description only if it's empty
                    if (
                        not existing_table.get("description")
                        or existing_table.get("description").strip() == ""
                    ):
                        existing_table["description"] = new_table.get("description", "")

                else:
                    # New table - add it completely
                    existing_tables[table_name] = new_table

            # Sort tables by name for consistency
            existing_source["tables"] = sorted(
                existing_tables.values(), key=lambda x: x["name"]
            )
        else:
            # Add new source to existing sources
            existing_sources.append(new_source)

        # Convert back to YAML with proper formatting
        yaml_output = yaml.dump(
            existing_yaml,
            default_flow_style=False,
            sort_keys=False,
            width=1000,  # Prevent line wrapping
            indent=2,
        )

        return yaml_output  # noqa: RET504, TRY300

    except yaml.YAMLError as e:
        print(f"YAML parsing error during merge: {e}")
        print("Falling back to appending new content")
        adjusted_fallback = adjust_source_schema_pattern(new_content, original_schema)
        return (
            existing_content + "\n\n# --- NEWLY ADDED TABLES ---\n" + adjusted_fallback
        )
    except Exception as e:
        print(f"Unexpected error merging sources: {e}")
        print("Falling back to appending new content")
        adjusted_fallback = adjust_source_schema_pattern(new_content, original_schema)
        return (
            existing_content + "\n\n# --- NEWLY ADDED TABLES ---\n" + adjusted_fallback
        )


def adjust_source_schema_pattern(
    content: str, original_schema: Optional[str] = None
) -> str:
    """
    Adjust the generated source content to use the standard schema pattern.

    Args:
        content: Generated source YAML content
        original_schema: The original schema name to replace

    Returns:
        Adjusted content with standard schema pattern
    """
    # Replace the hardcoded schema with the dynamic pattern
    schema_to_replace = (
        original_schema if original_schema else "ol_warehouse_production_raw"
    )
    adjusted = content.replace(
        f"name: {schema_to_replace}", "name: ol_warehouse_raw_data"
    )

    # Fix the YAML structure to properly indent loader, database, schema under the
    # source
    lines = adjusted.split("\n")
    new_lines = []
    in_source = False
    source_indent = 0

    for line in lines:
        if "- name: ol_warehouse_raw_data" in line:
            new_lines.append(line)
            source_indent = len(line) - len(line.lstrip())
            # Add the standard configuration with proper indentation
            indent = " " * (source_indent + 2)
            new_lines.append(f"{indent}loader: airbyte")
            new_lines.append(f"{indent}database: '{{{{ target.database }}}}'")
            new_lines.append(
                f'{indent}schema: \'{{{{ target.schema.replace(var("schema_suffix", ""), "").rstrip("_") }}}}_raw\''  # noqa: E501
            )
            in_source = True
        elif line.strip().startswith("description:") and in_source:
            # Keep the description with proper indentation
            indent = " " * (source_indent + 2)
            new_lines.append(f'{indent}description: ""')
        elif line.strip().startswith("tables:") and in_source:
            # Keep tables with proper indentation
            indent = " " * (source_indent + 2)
            new_lines.append(f"{indent}tables:")
        elif line.strip().startswith("- name:") and "ol_warehouse_raw_data" not in line:
            # This is a table entry, keep it as is
            new_lines.append(line)
            in_source = False
        elif not (
            line.strip().startswith("loader:")
            or line.strip().startswith("database:")
            or line.strip().startswith("schema:")
            or (line.strip().startswith("description:") and in_source)
        ):
            # Keep other lines as is, but stop being in source section if we hit
            # something else
            new_lines.append(line)
            if line.strip() and not line.startswith(" "):
                in_source = False

    return "\n".join(new_lines)


@app.command
def generate_staging_models(  # noqa: C901, PLR0912, PLR0913, PLR0915
    schema: str,
    prefix: str,
    tables: Optional[list[str]] = None,
    directory: Optional[str] = None,
    target: Optional[str] = None,
    apply_transformations: bool = True,  # noqa: FBT001, FBT002
    entity_type: Optional[str] = None,
):
    """
    Generate dbt staging models and YAML files for a given schema and table prefix,
    optionally for a specific list of tables.

    Args:
        schema: The database schema to generate staging models for.
        prefix: The table prefix to filter tables by.
        tables: An optional list of table names to generate models for.
        directory: An optional subdirectory within 'models/staging' to place the models.
        target: The dbt target to use (optional).
        apply_transformations: Whether to apply semantic transformations (default: True).
        entity_type: Override auto-detection of entity type (optional).
    """  # noqa: E501
    dbt_project_dir = Path("src") / "ol_dbt"
    staging_dir = dbt_project_dir / "models" / "staging"

    # Use domain from prefix if directory not specified
    if not directory:
        domain = extract_domain_from_prefix(prefix)
        if domain:
            directory = domain

    if directory:
        staging_dir = staging_dir / directory

    staging_dir.mkdir(parents=True, exist_ok=True)

    discovered_tables = tables
    if not discovered_tables:
        # Use generate_source to discover tables automatically
        source_args = {
            "schema_name": schema,  # Use actual schema for discovery
            "generate_columns": True,
            "include_descriptions": True,
            "table_pattern": f"{prefix}%",
        }

        result = run_dbt_command(
            str(dbt_project_dir),
            ["generate_source", "--args", json.dumps(source_args)],
            target,
        )

        # Extract table names from the generated source YAML
        source_content = result.stdout.strip()
        discovered_tables = []
        tables_section = re.findall(r"- name: ([^\n]+)", source_content)
        for table_match in tables_section:
            table_name = table_match.strip()
            if table_name.startswith(
                prefix.rstrip("_")
            ):  # Only include tables matching our prefix
                discovered_tables.append(table_name)

    if not discovered_tables:
        return

    # Collect model names for YAML generation
    generated_models = []

    for table_name in discovered_tables:
        # Extract the source (second section) from table name like
        # raw__mitlearn__app__postgres__table
        source_match = re.match(r"raw__([a-z]+)__", table_name)
        if not source_match:
            continue
        source = source_match.group(1)

        # Extract the meaningful part of the table name after the source prefix
        # e.g., raw__mitlearn__app__postgres__auth_user -> app__postgres__auth_user
        source_prefix = f"raw__{source}__"
        if table_name.startswith(source_prefix):
            table_suffix = table_name[len(source_prefix) :]
        else:
            table_suffix = table_name  # fallback to full name

        # Create file paths within the staging directory
        model_name = f"stg__{source}__{table_suffix}"
        sql_file_path = staging_dir / f"{model_name}.sql"

        try:
            # Generate SQL file first using enhanced macro
            base_model_args = json.dumps(
                {
                    "source_name": "ol_warehouse_raw_data",  # Use standard source name
                    "table_name": table_name,
                    "apply_transformations": apply_transformations,
                    "entity_type": entity_type,
                }
            )
            result_sql = run_dbt_command(
                str(dbt_project_dir),
                ["generate_base_model_enhanced", "--args", base_model_args],
                target,
            )
            sql_content_match = re.search(
                r"(with source as.*)", result_sql.stdout.strip(), re.DOTALL
            )
            if sql_content_match:
                sql_content = sql_content_match.group(1)
                sql_file_path.write_text(sql_content)
                print(f"Generated SQL for {table_name}")

                # Track the model name for YAML generation
                generated_models.append(model_name)
            else:
                print(f"Could not extract SQL content for {table_name}")
                continue

        except Exception as e:
            print(f"Error processing table {table_name}: {e}")
            continue

    # Generate consolidated YAML file using dbt-codegen generate_model_yaml
    if generated_models:
        domain = extract_domain_from_prefix(prefix)
        consolidated_yaml_path = staging_dir / f"_stg_{domain}__models.yml"

        try:
            # First, parse the project to make dbt aware of the new models
            print("Parsing dbt project to register new models...")
            try:
                run_dbt_command(
                    str(dbt_project_dir),
                    ["parse"],
                    target,
                )
                print("dbt parse completed successfully")
            except Exception as parse_error:
                print(f"Warning: dbt parse failed: {parse_error}")
                print("Proceeding with YAML generation anyway...")

            # Use our enhanced generate_model_yaml macro
            model_yaml_args = json.dumps(
                {
                    "model_names": generated_models,
                    "upstream_descriptions": True,
                    "include_data_types": True,
                }
            )

            result_yaml = run_dbt_command(
                str(dbt_project_dir),
                ["generate_model_yaml_enhanced", "--args", model_yaml_args],
                target,
            )

            # Extract the YAML content from the output
            yaml_content_match = re.search(
                r"(version:.*)", result_yaml.stdout.strip(), re.DOTALL
            )

            if yaml_content_match:
                generated_yaml_content = yaml_content_match.group(1)

                # Check if consolidated YAML already exists and merge
                if consolidated_yaml_path.exists():
                    print(
                        "Merging with existing consolidated YAML: "
                        f"{consolidated_yaml_path}"
                    )
                    existing_content = consolidated_yaml_path.read_text()
                    try:
                        existing_yaml = yaml.safe_load(existing_content)
                        new_yaml = yaml.safe_load(generated_yaml_content)

                        # Merge models, preserving existing model data where possible
                        existing_models = {
                            model["name"]: model
                            for model in existing_yaml.get("models", [])
                        }
                        new_models = new_yaml.get("models", [])

                        # Add or update models, preserving manual descriptions
                        for new_model in new_models:
                            model_name = new_model["name"]
                            if model_name in existing_models:
                                existing_model = existing_models[model_name]
                                # Preserve existing model description if it's not empty
                                if (
                                    existing_model.get("description")
                                    and existing_model.get("description").strip()
                                ):
                                    new_model["description"] = existing_model[
                                        "description"
                                    ]

                                # Preserve existing column descriptions
                                existing_columns = {
                                    col["name"]: col
                                    for col in existing_model.get("columns", [])
                                }
                                new_columns = new_model.get("columns", [])

                                for new_col in new_columns:
                                    col_name = new_col["name"]
                                    if (
                                        col_name in existing_columns
                                        and existing_columns[col_name].get(
                                            "description"
                                        )
                                        and existing_columns[col_name]
                                        .get("description")
                                        .strip()
                                    ):
                                        new_col["description"] = existing_columns[
                                            col_name
                                        ]["description"]

                            existing_models[model_name] = new_model

                        existing_yaml["models"] = sorted(
                            existing_models.values(), key=lambda x: x["name"]
                        )

                        consolidated_content = yaml.dump(
                            existing_yaml,
                            default_flow_style=False,
                            sort_keys=False,
                            width=1000,
                            indent=2,
                        )
                    except yaml.YAMLError as e:
                        print(f"Error parsing existing YAML: {e}, using new content")
                        consolidated_content = generated_yaml_content
                else:
                    print(f"Creating new consolidated YAML: {consolidated_yaml_path}")
                    consolidated_content = generated_yaml_content

                consolidated_yaml_path.write_text(consolidated_content)
                print(
                    f"Generated consolidated YAML with {len(generated_models)} "
                    "models using dbt-codegen"
                )
            else:
                print("Could not extract YAML content from generate_model_yaml output")
                msg = "Failed to extract YAML content"
                raise Exception(msg)  # noqa: TRY002, TRY301

        except Exception as e:
            print(f"Error generating model YAML with dbt-codegen: {e}")
            print("Falling back to basic YAML structure")
            # Fallback to basic structure if generate_model_yaml fails
            basic_models = [
                {"name": model_name, "description": "", "columns": []}
                for model_name in generated_models
            ]
            consolidated_content = yaml.dump(
                {"version": 2, "models": basic_models},
                default_flow_style=False,
                sort_keys=False,
                width=1000,
                indent=2,
            )
            consolidated_yaml_path.write_text(consolidated_content)
            print(
                f"Generated basic consolidated YAML with {len(generated_models)} models"
            )


@app.command
def generate_all(  # noqa: PLR0913
    schema: str,
    prefix: str,
    database: Optional[str] = None,
    target: Optional[str] = None,
    apply_transformations: bool = True,  # noqa: FBT001, FBT002
    entity_type: Optional[str] = None,
):
    """Generate both dbt sources and staging models for a given schema and table prefix.

    This is a convenience command that combines generate_sources and
    generate_staging_models.

    Args:
        schema: The database schema to generate sources and models for.
        prefix: The table prefix to filter tables by.
        database: The database name (optional).
        target: The dbt target to use (optional).
        apply_transformations: Whether to apply semantic transformations (default: True).
        entity_type: Override auto-detection of entity type (optional).

    """  # noqa: E501
    # First generate sources and get the discovered tables
    discovered_tables = generate_sources(schema, prefix, ".", database, target)

    # Then generate staging models using the discovered tables
    if discovered_tables:
        generate_staging_models(
            schema,
            prefix,
            tables=discovered_tables,
            target=target,
            apply_transformations=apply_transformations,
            entity_type=entity_type,
        )
        print(f"Generated staging models for {len(discovered_tables)} tables")
    else:
        print("No tables found matching the specified criteria")


if __name__ == "__main__":
    app()

#!/usr/bin/env python3
# ruff: noqa: T201, BLE001
"""
This script provides commands to generate dbt sources and staging models.
It interacts with dbt to discover tables and generate the necessary YAML and SQL files.
"""

import json
import re
import subprocess
from pathlib import Path

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
    dbt_project_dir: str, command: list[str], target: str | None
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
def generate_sources(
    schema: str,
    prefix: str,
    output_directory: str = ".",
    database: str | None = None,
    target: str | None = None,
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
        merged_content = merge_sources_content(
            existing_content, generated_content, schema
        )
        sources_file_path.write_text(merged_content)
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


def merge_sources_content(
    existing_content: str,
    new_content: str,
    original_schema: str | None,  # noqa: ARG001
) -> str:
    """
    Merge new source table definitions with existing sources file.

    Args:
        existing_content: Content of existing sources YAML file
        new_content: New source content to merge in

    Returns:
        Merged YAML content
    """

    try:
        existing_yaml = yaml.safe_load(existing_content)
        new_yaml = yaml.safe_load(new_content)

        # Find the source section in both
        existing_sources = existing_yaml.get("sources", [])
        new_sources = new_yaml.get("sources", [])

        if not new_sources:
            return existing_content

        new_source = new_sources[0]  # Should only be one source from generate_source
        source_name = new_source.get("name")

        # Find matching source in existing content
        existing_source = None
        for source in existing_sources:
            if source.get("name") == source_name:
                existing_source = source
                break

        if existing_source:
            # Merge tables, avoiding duplicates
            existing_tables = {
                table["name"]: table for table in existing_source.get("tables", [])
            }
            new_tables = new_source.get("tables", [])

            for table in new_tables:
                table_name = table["name"]
                existing_tables[table_name] = table  # This will overwrite if duplicate

            existing_source["tables"] = list(existing_tables.values())
        else:
            # Add new source
            existing_sources.append(new_source)

        # Convert back to YAML
        return yaml.dump(existing_yaml, default_flow_style=False, sort_keys=False)

    except Exception as e:
        print(f"Error merging sources: {e}")
        # Fall back to appending
        return existing_content + "\n\n# Newly added tables:\n" + new_content


def adjust_source_schema_pattern(
    content: str, original_schema: str | None = None
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
def generate_staging_models(  # noqa: C901
    schema: str,
    prefix: str,
    tables: list[str] | None = None,
    directory: str | None = None,
    target: str | None = None,
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
    """
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

    for table_name in discovered_tables:
        # Extract the source (second section) from table name like
        # raw__mitlearn__app__postgres__table
        source_match = re.match(r"raw__([a-z]+)__", table_name)
        if not source_match:
            continue
        source = source_match.group(1)

        # Create file paths within the staging directory
        sql_file_path = staging_dir / f"stg_{source}__{table_name}.sql"
        yaml_file_path = staging_dir / f"stg_{source}__{table_name}.yml"

        try:
            # Generate SQL file first
            base_model_args = json.dumps(
                {
                    "source_name": "ol_warehouse_raw_data",  # Use standard source name
                    "table_name": table_name,
                }
            )
            result_sql = run_dbt_command(
                str(dbt_project_dir),
                ["generate_base_model", "--args", base_model_args],
                target,
            )
            sql_content_match = re.search(
                r"(with source as.*)", result_sql.stdout.strip(), re.DOTALL
            )
            if sql_content_match:
                sql_content = sql_content_match.group(1)
                sql_file_path.write_text(sql_content)
                print(f"Generated SQL for {table_name}")
            else:
                print(f"Could not extract SQL content for {table_name}")
                continue

            basic_yaml = f"""version: 2

models:
  - name: stg_{source}__{table_name}
    description: ""
    columns: []
"""
            yaml_file_path.write_text(basic_yaml)
            print(f"Generated basic YAML structure for {table_name}")

        except Exception as e:
            print(f"Error processing table {table_name}: {e}")
            continue


@app.command
def generate_all(
    schema: str,
    prefix: str,
    database: str | None = None,
    target: str | None = None,
):
    """Generate both dbt sources and staging models for a given schema and table prefix.

    This is a convenience command that combines generate_sources and
    generate_staging_models.

    Args:
        schema: The database schema to generate sources and models for.
        prefix: The table prefix to filter tables by.
        database: The database name (optional).
        target: The dbt target to use (optional).

    """
    # First generate sources and get the discovered tables
    discovered_tables = generate_sources(schema, prefix, ".", database, target)

    # Then generate staging models using the discovered tables
    if discovered_tables:
        generate_staging_models(schema, prefix, tables=discovered_tables, target=target)
        print(f"Generated staging models for {len(discovered_tables)} tables")
    else:
        print("No tables found matching the specified criteria")


if __name__ == "__main__":
    app()

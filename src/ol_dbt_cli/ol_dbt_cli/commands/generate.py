"""Generate commands for scaffolding dbt sources and staging models.

Provides commands for:
- Generating dbt source YAML definitions from database schemas
- Generating staging model SQL + YAML from source tables

Tables can be discovered either via dbt-codegen macros (requires Trino credentials)
or directly from the local DuckDB database populated by `ol-dbt local register`
(requires only AWS credentials).
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Annotated

import duckdb
import yaml
from cyclopts import App, Parameter

generate_app = App(
    name="generate",
    help="Scaffold dbt sources and staging models.",
)

# Default path for the local DuckDB database (matches local_dev.py)
_DEFAULT_DUCKDB_PATH = Path.home() / ".ol-dbt" / "local.duckdb"


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
# Local DuckDB helpers
# ============================================================================


def _discover_tables_from_local_db(
    duckdb_path: Path,
    glue_database: str,
    prefix: str,
) -> list[str]:
    """Return table names from the local DuckDB registry matching a prefix.

    Reads the _glue_source_registry table populated by `ol-dbt local register`.
    Does not require Trino credentials — only the local DuckDB file.
    """
    if not duckdb_path.exists():
        msg = f"Local DuckDB database not found: {duckdb_path}. Run `ol-dbt local register` first."
        raise FileNotFoundError(msg)

    clean_prefix = prefix.rstrip("_%")
    with duckdb.connect(str(duckdb_path), read_only=True) as conn:
        rows = conn.execute(
            "SELECT glue_table FROM _glue_source_registry WHERE glue_database = ? AND glue_table LIKE ?",
            (glue_database, f"{clean_prefix}%"),
        ).fetchall()
    return [row[0] for row in rows]


def _describe_local_view(
    duckdb_path: Path,
    glue_database: str,
    table_name: str,
) -> list[dict[str, str]]:
    """Return column name/type pairs for a registered Iceberg view.

    Opens the local DuckDB file, loads the required extensions, and runs DESCRIBE
    on the registered view to fetch column metadata from Iceberg table metadata on S3.
    Requires AWS credentials (same as `ol-dbt local register`).
    """
    view_name = f"glue__{glue_database}__{table_name}"
    with duckdb.connect(str(duckdb_path)) as conn:
        for ext in ["httpfs", "aws", "iceberg"]:
            conn.execute(f"LOAD {ext}")
        conn.execute("CALL load_aws_credentials()")
        quoted_view_name = '"' + view_name.replace('"', '""') + '"'
        rows = conn.execute(f"DESCRIBE {quoted_view_name}").fetchall()  # noqa: S608
    # DESCRIBE returns: column_name, column_type, null, key, default, extra
    return [{"name": row[0], "data_type": row[1]} for row in rows]


def _build_sources_content_from_local(
    glue_database: str,
    tables_with_columns: list[tuple[str, list[dict[str, str]]]],
) -> str:
    """Build a sources YAML string from locally-discovered table schemas.

    The returned YAML uses the Glue database name as the source name; callers
    should pass it through _adjust_source_schema_pattern to normalise it.
    """
    tables = [
        {
            "name": table_name,
            "description": "",
            "columns": [{"name": col["name"], "data_type": col["data_type"], "description": ""} for col in columns],
        }
        for table_name, columns in tables_with_columns
    ]
    source_yaml: dict[str, object] = {
        "version": 2,
        "sources": [
            {
                "name": glue_database,
                "tables": tables,
            }
        ],
    }
    return yaml.dump(source_yaml, default_flow_style=False, sort_keys=False, width=1000, indent=2)


def _build_staging_sql_from_columns(
    source_name: str,
    table_name: str,
    columns: list[dict[str, str]],
) -> str:
    """Build a minimal staging SQL model from a list of columns.

    Produces the same ``with source as / renamed as / select`` structure that
    dbt-codegen's generate_base_model macro emits, so the output is a drop-in
    replacement when Trino is unavailable.
    """
    source_ref = f"{{{{ source('{source_name}', '{table_name}') }}}}"  # noqa: S608
    if not columns:
        return (  # noqa: S608
            f"with source as (\n    select * from {source_ref}\n)\n\nselect * from source\n"  # noqa: S608
        )
    col_lines = ",\n        ".join(col["name"] for col in columns)
    return (
        f"with source as (\n"  # noqa: S608
        f"    select * from {source_ref}\n"
        f"),\nrenamed as (\n"
        f"    select\n"
        f"        {col_lines}\n"
        f"    from source\n"
        f")\n\nselect * from renamed\n"
    )


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
    use_local_db: Annotated[
        bool,
        Parameter(help="Discover tables from local DuckDB (populated by `ol-dbt local register`) instead of Trino."),
    ] = False,
    duckdb_path: Annotated[
        Path,
        Parameter(help="Path to local DuckDB database file."),
    ] = _DEFAULT_DUCKDB_PATH,
) -> list[str] | None:
    """Generate a dbt sources YAML file for tables matching a schema and prefix.

    Discovers tables either via dbt-codegen's generate_source macro (default,
    requires Trino credentials) or from the local DuckDB database populated by
    `ol-dbt local register` (--use-local-db, requires only AWS credentials).

    Writes (or merges into) a _<domain>__sources.yml file in the appropriate
    staging subdirectory.

    Args:
        schema: Glue database / Trino schema to discover tables from.
        prefix: Table name prefix to filter by (e.g., raw__mitlearn__app__postgres__).
        output_directory: Subdirectory within models/staging/<domain>/ to write to.
        database: Optional database name override (Trino path only).
        target: Optional dbt target to use (Trino path only).
        use_local_db: Use local DuckDB instead of Trino.
        duckdb_path: Path to the local DuckDB file.

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

    if use_local_db:
        print(f"Discovering tables from local DuckDB: {duckdb_path}")
        try:
            discovered_tables = _discover_tables_from_local_db(duckdb_path, schema, prefix)
        except FileNotFoundError as e:
            print(f"✗ {e}")
            return None

        if not discovered_tables:
            print(f"No tables found in local DB for database={schema!r}, prefix={prefix!r}")
            return None

        print(f"Found {len(discovered_tables)} tables. Fetching column schemas from Iceberg metadata...")
        tables_with_columns: list[tuple[str, list[dict[str, str]]]] = []
        for table_name in discovered_tables:
            try:
                columns = _describe_local_view(duckdb_path, schema, table_name)
                tables_with_columns.append((table_name, columns))
                print(f"  ✓ {table_name} ({len(columns)} columns)")
            except Exception as e:  # noqa: BLE001
                print(f"  ✗ {table_name}: {e}")

        if not tables_with_columns:
            print("No tables could be described. Check AWS credentials.")
            return None

        generated_content = _build_sources_content_from_local(schema, tables_with_columns)
    else:
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

    discovered_tables_list: list[str] = []
    for table_match in re.findall(r"- name: ([^\n]+)", generated_content):
        name = table_match.strip()
        if name.startswith(prefix.rstrip("_")):
            discovered_tables_list.append(name)

    return discovered_tables_list


@generate_app.command
def staging_models(  # noqa: C901, PLR0912, PLR0913, PLR0915
    schema: str,
    prefix: str,
    tables: list[str] | None = None,
    directory: str | None = None,
    target: str | None = None,
    apply_transformations: bool = True,  # noqa: FBT001, FBT002
    entity_type: str | None = None,
    use_local_db: Annotated[
        bool,
        Parameter(help="Build staging SQL from local DuckDB column schemas instead of dbt-codegen macros."),
    ] = False,
    duckdb_path: Annotated[
        Path,
        Parameter(help="Path to local DuckDB database file."),
    ] = _DEFAULT_DUCKDB_PATH,
) -> None:
    """Generate dbt staging SQL models and YAML for tables matching a schema and prefix.

    For each discovered table, generates a staging SQL file either using the
    generate_base_model_enhanced macro (default, requires Trino) or by reading
    column schemas directly from the local DuckDB Iceberg views (--use-local-db).

    Args:
        schema: Glue database / Trino schema to discover tables from.
        prefix: Table name prefix to filter by.
        tables: Explicit list of table names (skips discovery if provided).
        directory: Subdirectory within models/staging/ for the generated files.
        target: Optional dbt target (Trino path only).
        apply_transformations: Apply semantic column transformations (Trino path only).
        entity_type: Override auto-detected entity type (Trino path only).
        use_local_db: Use local DuckDB instead of Trino macros.
        duckdb_path: Path to the local DuckDB file.

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
        if use_local_db:
            try:
                discovered_tables = _discover_tables_from_local_db(duckdb_path, schema, prefix)
            except FileNotFoundError as e:
                print(f"✗ {e}")
                return
        else:
            source_args = {
                "schema_name": schema,
                "generate_columns": True,
                "include_descriptions": True,
                "table_pattern": f"{prefix}%",
            }
            result = _run_dbt_command(
                str(dbt_project_dir), ["generate_source", "--args", json.dumps(source_args)], target
            )
            discovered_tables = []
            for table_match in re.findall(r"- name: ([^\n]+)", result.stdout.strip()):
                name = table_match.strip()
                if name.startswith(prefix.rstrip("_")):
                    discovered_tables.append(name)

    if not discovered_tables:
        return

    generated_models: list[str] = []

    for table_name in discovered_tables:
        source_match = re.match(r"raw__([a-z][a-z0-9_]*)__", table_name)
        if not source_match:
            continue
        source = source_match.group(1)

        source_prefix = f"raw__{source}__"
        table_suffix = table_name[len(source_prefix) :] if table_name.startswith(source_prefix) else table_name

        model_name = f"stg__{source}__{table_suffix}"
        sql_file_path = staging_dir / f"{model_name}.sql"

        if use_local_db:
            try:
                columns = _describe_local_view(duckdb_path, schema, table_name)
                sql_content = _build_staging_sql_from_columns("ol_warehouse_raw_data", table_name, columns)
                sql_file_path.write_text(sql_content)
                print(f"Generated SQL for {table_name} ({len(columns)} columns)")
                generated_models.append(model_name)
            except Exception as e:  # noqa: BLE001
                print(f"Error processing table {table_name}: {e}")
        else:
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

    if use_local_db:
        # dbt macros unavailable — generate a basic models YAML skeleton
        if consolidated_yaml_path.exists():
            existing_yaml = yaml.safe_load(consolidated_yaml_path.read_text()) or {}
            existing_models_map = {m["name"]: m for m in existing_yaml.get("models", [])}
            for model_name in generated_models:
                if model_name not in existing_models_map:
                    existing_models_map[model_name] = {"name": model_name, "description": "", "columns": []}
            existing_yaml["models"] = sorted(existing_models_map.values(), key=lambda x: x["name"])
            consolidated_yaml_path.write_text(
                yaml.dump(existing_yaml, default_flow_style=False, sort_keys=False, width=1000, indent=2)
            )
        else:
            basic = {
                "version": 2,
                "models": [{"name": m, "description": "", "columns": []} for m in sorted(generated_models)],
            }
            consolidated_yaml_path.write_text(
                yaml.dump(basic, default_flow_style=False, sort_keys=False, width=1000, indent=2)
            )
        print(f"Generated models YAML skeleton with {len(generated_models)} models")
    else:
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
            basic = {
                "version": 2,
                "models": [{"name": m, "description": "", "columns": []} for m in generated_models],
            }
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
    use_local_db: Annotated[
        bool,
        Parameter(help="Use local DuckDB for table discovery and column schemas instead of Trino."),
    ] = False,
    duckdb_path: Annotated[
        Path,
        Parameter(help="Path to local DuckDB database file."),
    ] = _DEFAULT_DUCKDB_PATH,
) -> None:
    """Generate both dbt sources YAML and staging models for a schema/prefix.

    Convenience command combining `generate sources` and `generate staging-models`.
    Pass --use-local-db to discover tables and column schemas from the local DuckDB
    database (populated by `ol-dbt local register`) instead of Trino.

    Args:
        schema: Glue database / Trino schema to discover tables from.
        prefix: Table name prefix to filter by.
        database: Optional database name override (Trino path only).
        target: Optional dbt target (Trino path only).
        apply_transformations: Apply semantic column transformations (Trino path only).
        entity_type: Override auto-detected entity type (Trino path only).
        use_local_db: Use local DuckDB instead of Trino.
        duckdb_path: Path to the local DuckDB file.

    """
    discovered = sources(schema, prefix, ".", database, target, use_local_db=use_local_db, duckdb_path=duckdb_path)

    if discovered:
        staging_models(
            schema,
            prefix,
            tables=discovered,
            target=target,
            apply_transformations=apply_transformations,
            entity_type=entity_type,
            use_local_db=use_local_db,
            duckdb_path=duckdb_path,
        )
        print(f"Generated staging models for {len(discovered)} tables")
    else:
        print("No tables found matching the specified criteria")

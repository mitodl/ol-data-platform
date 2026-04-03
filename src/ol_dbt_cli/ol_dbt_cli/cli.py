"""Main CLI entry point for ol-dbt tooling."""

import sys

import cyclopts

from ol_dbt_cli.commands.generate import generate_app
from ol_dbt_cli.commands.impact import impact
from ol_dbt_cli.commands.local_dev import local_app
from ol_dbt_cli.commands.run import run_app
from ol_dbt_cli.commands.validate import validate

app = cyclopts.App(
    name="ol-dbt",
    help="""
    ol-dbt: dbt project tooling CLI

    Tools for the engineering team to develop, validate, and analyse dbt changes.

    Common workflows:

      1. Set up local DuckDB + Iceberg development environment:
         $ ol-dbt local setup

      2. Register Iceberg tables as DuckDB views:
         $ ol-dbt local register --all-layers

      3. Scaffold sources and staging models for a new data source:
         $ ol-dbt generate all --schema ol_warehouse_production_raw --prefix raw__myapp__

      4. Incrementally run only changed or errored models (fast iteration):
         $ ol-dbt run

      5. Full rebuild (re-initialises state for next incremental run):
         $ ol-dbt run --full-refresh

      6. Check what columns break downstream before opening a PR:
         $ ol-dbt impact

      7. Validate model SQL/YAML consistency:
         $ ol-dbt validate --model staging

      8. JSON output for CI pipelines:
         $ ol-dbt impact --format json
         $ ol-dbt validate --format json

    Use --help on any subcommand for detailed usage.
    """,
    version="0.1.0",
)

app.command(local_app)
app.command(generate_app)
app.command(run_app)
app.command(impact, name="impact")
app.command(validate, name="validate")


def main() -> None:
    """CLI entry point."""
    try:
        app()
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

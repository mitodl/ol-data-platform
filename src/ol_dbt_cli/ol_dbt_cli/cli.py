"""Main CLI entry point for ol-dbt tooling."""

import sys

import cyclopts

from ol_dbt_cli.commands.impact import impact
from ol_dbt_cli.commands.validate import validate

app = cyclopts.App(
    name="ol-dbt",
    help="""
    ol-dbt: dbt project analysis and validation CLI

    Tools for the engineering team to reason about in-progress dbt changes,
    validate model quality, and understand column-level impact.

    Common workflows:

      1. Check what breaks before opening a PR:
         $ ol-dbt impact --changed-only

      2. Validate model SQL/YAML consistency:
         $ ol-dbt validate --changed-only

      3. Full validation of all models:
         $ ol-dbt validate

      4. JSON output for CI pipelines:
         $ ol-dbt impact --format json
         $ ol-dbt validate --format json

    Use --help on any command for detailed usage.
    """,
    version="0.1.0",
)

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

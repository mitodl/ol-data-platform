"""Main CLI entry point for Superset asset management."""

import sys

import cyclopts

from ol_superset.commands.export import export
from ol_superset.commands.promote import promote
from ol_superset.commands.sync import sync
from ol_superset.commands.validate import validate

app = cyclopts.App(
    name="ol-superset",
    help="""
    Superset Asset Management CLI

    Manage Apache Superset dashboards, charts, and datasets across QA and
    production environments with built-in safety guardrails.

    Common workflows:

      1. Export production assets for backup:
         $ ol-superset export

      2. Test changes in QA, then promote to production:
         $ ol-superset export --from superset-qa
         $ ol-superset promote

      3. Sync production to QA:
         $ ol-superset export --from superset-production
         $ ol-superset sync superset-production superset-qa

    Use --help on any command for detailed usage.
    """,
    version="0.1.0",
)

# Register commands
app.command(export, name="export")
app.command(validate, name="validate")
app.command(sync, name="sync")
app.command(promote, name="promote")


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

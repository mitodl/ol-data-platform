#!/usr/bin/env python3
"""Helper script to manage dlt destination environment switching.

Usage:
    python scripts/dlt_env.py local     # Switch to local development
    python scripts/dlt_env.py production # Switch to production
    python scripts/dlt_env.py status    # Show current environment
"""

import os
import sys
from pathlib import Path


def get_project_root() -> Path:
    """Get the lakehouse project root directory."""
    return Path(__file__).parent.parent


def set_env(env: str):
    """Set DLT_DESTINATION_ENV environment variable."""
    if env not in ["local", "production"]:
        print(  # noqa: T201
            f"Error: Invalid environment '{env}'. Must be 'local' or 'production'."
        )
        sys.exit(1)

    # For the current shell session
    os.environ["DLT_DESTINATION_ENV"] = env

    # Print export command for user to run
    print("To set environment for your shell, run:")  # noqa: T201
    print(f"  export DLT_DESTINATION_ENV={env}")  # noqa: T201
    print()  # noqa: T201
    print("Or prefix your command with the environment variable:")  # noqa: T201
    print(  # noqa: T201
        f"  DLT_DESTINATION_ENV={env} python -m lakehouse.defs.your_pipeline.loads"
    )


def show_status():
    """Show current dlt environment configuration."""
    env = os.getenv("DLT_DESTINATION_ENV", "local")
    project_root = get_project_root()

    print("=" * 60)  # noqa: T201
    print("dlt Environment Status")  # noqa: T201
    print("=" * 60)  # noqa: T201
    print(f"Current Environment: {env}")  # noqa: T201
    print(f"Project Root: {project_root}")  # noqa: T201
    print()  # noqa: T201

    if env == "local":
        data_dir = project_root / ".dlt" / "data"
        print("Destination: Local filesystem")  # noqa: T201
        print(f"Data Directory: {data_dir}")  # noqa: T201
        print("Query Engine: DuckDB")  # noqa: T201
        print()  # noqa: T201
        print("To query local data:")  # noqa: T201
        parquet_pattern = f"{data_dir}/*/*.parquet"
        # This is just example output showing the user how to query, not actual SQL
        print(  # noqa: T201
            f'  duckdb -c "INSTALL iceberg; LOAD iceberg; '  # noqa: S608
            f"SELECT * FROM iceberg_scan('{parquet_pattern}') LIMIT 10;\""
        )
    else:
        print("Destination: AWS S3")  # noqa: T201
        print("Check .dlt/config.toml for bucket configuration")  # noqa: T201
        print("Catalog: AWS Glue")  # noqa: T201

    print()  # noqa: T201
    print("Available commands:")  # noqa: T201
    print(  # noqa: T201
        "  python scripts/dlt_env.py local       - Switch to local development"
    )
    print(  # noqa: T201
        "  python scripts/dlt_env.py production  - Switch to production"
    )
    print("  python scripts/dlt_env.py status      - Show this status")  # noqa: T201


def main():
    min_args = 2  # Require at least command name
    if len(sys.argv) < min_args:
        show_status()
        return

    command = sys.argv[1].lower()

    if command == "status":
        show_status()
    elif command in ["local", "production"]:
        set_env(command)
    else:
        print(f"Error: Unknown command '{command}'")  # noqa: T201
        print()  # noqa: T201
        show_status()
        sys.exit(1)


if __name__ == "__main__":
    main()

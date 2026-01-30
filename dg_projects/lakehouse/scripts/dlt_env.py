#!/usr/bin/env python3
"""
Helper script to manage dlt destination environment switching.

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
        print(f"Error: Invalid environment '{env}'. Must be 'local' or 'production'.")
        sys.exit(1)
    
    # For the current shell session
    os.environ["DLT_DESTINATION_ENV"] = env
    
    # Print export command for user to run
    print(f"To set environment for your shell, run:")
    print(f"  export DLT_DESTINATION_ENV={env}")
    print()
    print(f"Or prefix your command with the environment variable:")
    print(f"  DLT_DESTINATION_ENV={env} python -m lakehouse.defs.your_pipeline.loads")


def show_status():
    """Show current dlt environment configuration."""
    env = os.getenv("DLT_DESTINATION_ENV", "local")
    project_root = get_project_root()
    
    print("=" * 60)
    print("dlt Environment Status")
    print("=" * 60)
    print(f"Current Environment: {env}")
    print(f"Project Root: {project_root}")
    print()
    
    if env == "local":
        data_dir = project_root / ".dlt" / "data"
        print(f"Destination: Local filesystem")
        print(f"Data Directory: {data_dir}")
        print(f"Query Engine: DuckDB")
        print()
        print("To query local data:")
        print(f'  duckdb -c "INSTALL iceberg; LOAD iceberg; SELECT * FROM iceberg_scan(\\'{data_dir}/*/*.parquet\\') LIMIT 10;"')
    else:
        print(f"Destination: AWS S3")
        print(f"Check .dlt/config.toml for bucket configuration")
        print(f"Catalog: AWS Glue")
    
    print()
    print("Available commands:")
    print("  python scripts/dlt_env.py local       - Switch to local development")
    print("  python scripts/dlt_env.py production  - Switch to production")
    print("  python scripts/dlt_env.py status      - Show this status")


def main():
    if len(sys.argv) < 2:
        show_status()
        return
    
    command = sys.argv[1].lower()
    
    if command == "status":
        show_status()
    elif command in ["local", "production"]:
        set_env(command)
    else:
        print(f"Error: Unknown command '{command}'")
        print()
        show_status()
        sys.exit(1)


if __name__ == "__main__":
    main()

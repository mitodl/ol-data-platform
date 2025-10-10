#!/usr/bin/env python3
# ruff: noqa: T201
"""
Script to run uv commands across all code locations in the dg_projects directory.

This script discovers all directories containing a pyproject.toml file and executes
the specified uv command on each one.
"""

import subprocess
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter

app = App(
    help="Run uv commands across all code locations",
    default_parameter=Parameter(negative=""),
)


class Colors:
    """ANSI color codes for terminal output."""

    GREEN = "\033[0;32m"
    BLUE = "\033[0;34m"
    RED = "\033[0;31m"
    YELLOW = "\033[1;33m"
    NC = "\033[0m"  # No Color


def find_code_locations(base_dir: Path) -> list[Path]:
    """
    Find all directories containing a pyproject.toml file.

    Args:
        base_dir: The base directory to search for code locations.

    Returns:
        A sorted list of Path objects for each code location.
    """
    locations = []
    for item in base_dir.iterdir():
        if item.is_dir() and (item / "pyproject.toml").exists():
            locations.append(item)  # noqa: PERF401
    return sorted(locations)


def run_uv_command(location: Path, uv_args: list[str], verbose: bool = False) -> bool:  # noqa: FBT001, FBT002
    """
    Run uv command in the specified location.

    Args:
        location: Path to the code location directory.
        uv_args: List of arguments to pass to the uv command.
        verbose: Whether to print verbose output.

    Returns:
        True if the command succeeded, False otherwise.
    """
    try:
        if verbose:
            print(f"Running: uv {' '.join(uv_args)} in {location}")

        subprocess.run(["uv", *uv_args], cwd=location, check=True, capture_output=False)  # noqa: S603, S607
        return True  # noqa: TRY300
    except subprocess.CalledProcessError:
        return False
    except FileNotFoundError:
        print(
            f"{Colors.RED}Error: 'uv' command not found. Please install uv first.{Colors.NC}"  # noqa: E501
        )
        sys.exit(1)


@app.default
def run_uv_operations(  # noqa: C901
    *uv_command: Annotated[
        str,
        Parameter(
            show=False,
            help="The uv command and arguments to run (e.g., sync, lock --upgrade)",
        ),
    ],
    code_locations_dir: Annotated[
        Path,
        Parameter(
            help="Base directory containing code locations",
        ),
    ] = Path(__file__).parent.parent / "dg_projects",
    continue_on_error: Annotated[
        bool,
        Parameter(
            help="Continue running even if some locations fail",
        ),
    ] = False,
    verbose: Annotated[
        bool,
        Parameter(
            help="Print verbose output",
        ),
    ] = False,
) -> None:
    """
    Run uv commands across all code locations.

    Examples:
        uv-operations sync
        uv-operations lock --upgrade
        uv-operations build
        uv-operations pip list
    """
    if not uv_command:
        print(f"{Colors.RED}Error: No uv command specified{Colors.NC}")
        print("\nUsage examples:")
        print("  uv-operations sync")
        print("  uv-operations lock --upgrade")
        print("  uv-operations build")
        sys.exit(1)

    base_dir = code_locations_dir.resolve()

    if not base_dir.exists():
        print(f"{Colors.RED}Error: Directory does not exist: {base_dir}{Colors.NC}")
        sys.exit(1)

    # Find all code locations
    print(f"{Colors.BLUE}Discovering code locations in: {base_dir}{Colors.NC}\n")

    locations = find_code_locations(base_dir)

    if not locations:
        print(f"{Colors.RED}No code locations with pyproject.toml found!{Colors.NC}")
        sys.exit(1)

    print(f"{Colors.GREEN}Found {len(locations)} code locations:{Colors.NC}")
    for loc in locations:
        print(f"  - {loc.name}")
    print()

    # Track results
    successful: list[str] = []
    failed: list[str] = []

    # Run command on each location
    for location in locations:
        print(f"{Colors.BLUE}{'=' * 40}{Colors.NC}")
        print(f"{Colors.BLUE}Processing: {location.name}{Colors.NC}")
        print(f"{Colors.BLUE}{'=' * 40}{Colors.NC}")

        success = run_uv_command(location, list(uv_command), verbose=verbose)

        if success:
            print(f"{Colors.GREEN}✓ Success: {location.name}{Colors.NC}")
            successful.append(location.name)
        else:
            print(f"{Colors.RED}✗ Failed: {location.name}{Colors.NC}")
            failed.append(location.name)
            if not continue_on_error:
                print(
                    f"\n{Colors.RED}Stopping due to failure. "
                    f"Use --continue-on-error to continue.{Colors.NC}"
                )
                sys.exit(1)

        print()

    # Print summary
    print(f"{Colors.BLUE}{'=' * 40}{Colors.NC}")
    print(f"{Colors.BLUE}SUMMARY{Colors.NC}")
    print(f"{Colors.BLUE}{'=' * 40}{Colors.NC}")
    print(f"{Colors.GREEN}Successful ({len(successful)}):{Colors.NC}")
    for loc in successful:  # type: ignore[assignment]
        print(f"  {Colors.GREEN}✓{Colors.NC} {loc}")

    if failed:
        print(f"\n{Colors.RED}Failed ({len(failed)}):{Colors.NC}")
        for loc in failed:  # type: ignore[assignment]
            print(f"  {Colors.RED}✗{Colors.NC} {loc}")
        print(
            f"\n{Colors.YELLOW}Some operations failed. "
            f"Check the output above for details.{Colors.NC}"
        )
        sys.exit(1)
    else:
        print(f"\n{Colors.GREEN}All operations completed successfully!{Colors.NC}")


if __name__ == "__main__":
    app()

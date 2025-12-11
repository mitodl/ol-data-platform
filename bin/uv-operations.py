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

app = App(help="Run uv commands across all code locations")


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


def run_across_locations(
    uv_args: list[str],
    code_locations_dir: Path,
    continue_on_error: bool,  # noqa: FBT001
    verbose: bool,  # noqa: FBT001
) -> None:
    """
    Run a uv command across all code locations.

    Args:
        uv_args: Arguments to pass to uv.
        code_locations_dir: Base directory containing code locations.
        continue_on_error: Whether to continue if a location fails.
        verbose: Whether to print verbose output.
    """
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

        success = run_uv_command(location, uv_args, verbose=verbose)

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


@app.command
def sync(
    code_locations_dir: Annotated[
        Path,
        Parameter(
            help="Base directory containing code locations",
        ),
    ] = Path(__file__).parent.parent / "dg_projects",
    continue_on_error: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Continue running even if some locations fail",
        ),
    ] = False,
    verbose: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Print verbose output",
        ),
    ] = False,
) -> None:
    """Sync dependencies in all code locations."""
    run_across_locations(["sync"], code_locations_dir, continue_on_error, verbose)


@app.command
def lock(
    upgrade: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Upgrade all dependencies to latest versions",
        ),
    ] = False,
    code_locations_dir: Annotated[
        Path,
        Parameter(
            help="Base directory containing code locations",
        ),
    ] = Path(__file__).parent.parent / "dg_projects",
    continue_on_error: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Continue running even if some locations fail",
        ),
    ] = False,
    verbose: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Print verbose output",
        ),
    ] = False,
) -> None:
    """Lock dependencies in all code locations."""
    uv_args = ["lock"]
    if upgrade:
        uv_args.append("--upgrade")
    run_across_locations(uv_args, code_locations_dir, continue_on_error, verbose)


@app.command
def build(
    code_locations_dir: Annotated[
        Path,
        Parameter(
            help="Base directory containing code locations",
        ),
    ] = Path(__file__).parent.parent / "dg_projects",
    continue_on_error: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Continue running even if some locations fail",
        ),
    ] = False,
    verbose: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Print verbose output",
        ),
    ] = False,
) -> None:
    """Build packages in all code locations."""
    run_across_locations(["build"], code_locations_dir, continue_on_error, verbose)


@app.command
def pip(
    pip_command: Annotated[
        str,
        Parameter(
            help="The pip subcommand to run (e.g., list, freeze)",
        ),
    ],
    code_locations_dir: Annotated[
        Path,
        Parameter(
            help="Base directory containing code locations",
        ),
    ] = Path(__file__).parent.parent / "dg_projects",
    continue_on_error: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Continue running even if some locations fail",
        ),
    ] = False,
    verbose: Annotated[  # noqa: FBT002
        bool,
        Parameter(
            help="Print verbose output",
        ),
    ] = False,
) -> None:
    """Run pip commands in all code locations."""
    run_across_locations(
        ["pip", pip_command], code_locations_dir, continue_on_error, verbose
    )


if __name__ == "__main__":
    app()

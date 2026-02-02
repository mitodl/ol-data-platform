"""Shared utilities and helpers for Superset CLI."""

import subprocess
import sys
from pathlib import Path


def get_repo_root() -> Path:
    """Get the repository root directory."""
    return Path(__file__).parent.parent.parent.resolve()


def get_assets_dir(custom_dir: str | None = None) -> Path:
    """Get the assets directory path."""
    if custom_dir:
        return Path(custom_dir).resolve()
    return get_repo_root() / "assets"


def count_assets(assets_dir: Path) -> dict[str, int]:
    """
    Count assets by type.

    Returns dict with counts for dashboards, charts, datasets, databases.
    """
    counts = {
        "dashboards": 0,
        "published_dashboards": 0,
        "charts": 0,
        "datasets": 0,
        "databases": 0,
    }

    if not assets_dir.exists():
        return counts

    # Count dashboards
    dashboard_dir = assets_dir / "dashboards"
    if dashboard_dir.exists():
        all_dashboards = list(dashboard_dir.glob("*.yaml"))
        counts["dashboards"] = len(all_dashboards)
        counts["published_dashboards"] = len(
            [d for d in all_dashboards if not d.name.startswith("untitled_")]
        )

    # Count other asset types
    for asset_type in ["charts", "datasets", "databases"]:
        asset_dir = assets_dir / asset_type
        if asset_dir.exists():
            counts[asset_type] = len(list(asset_dir.rglob("*.yaml")))

    return counts


def run_sup_command(
    args: list[str], *, check: bool = True, capture_output: bool = False
) -> subprocess.CompletedProcess[str]:
    """
    Run a sup CLI command.

    Args:
        args: Command arguments (e.g., ['instance', 'list'])
        check: Raise exception on non-zero exit code
        capture_output: Capture stdout/stderr

    Returns:
        CompletedProcess result
    """
    cmd = ["sup", *args]
    result = subprocess.run(
        cmd,
        capture_output=capture_output,
        text=True,
        check=False,
    )

    if check and result.returncode != 0:
        print(f"Error: sup command failed: {' '.join(cmd)}", file=sys.stderr)
        if capture_output and result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(result.returncode)

    return result


def confirm_action(
    prompt: str, *, require_exact: str | None = None, default: bool = False
) -> bool:
    """
    Prompt user for confirmation.

    Args:
        prompt: Prompt message
        require_exact: If set, user must type this exact string
        default: Default value if user just presses Enter

    Returns:
        True if confirmed, False otherwise
    """
    if require_exact:
        print(f"\n{prompt}")
        response = input(f"Type '{require_exact}' to continue: ").strip()
        return response == require_exact

    suffix = " (yes/no): " if not default else " [Y/n]: "
    response = input(f"\n{prompt}{suffix}").strip().lower()

    if not response:
        return default

    return response in ("y", "yes")


def check_git_status(assets_dir: Path) -> tuple[bool, list[str]]:
    """
    Check if assets directory has uncommitted changes.

    Returns:
        Tuple of (has_changes, list of changed files)
    """
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", "HEAD", str(assets_dir)],
            capture_output=True,
            text=True,
            check=True,
            cwd=assets_dir.parent,
        )
        changed_files = [line for line in result.stdout.strip().split("\n") if line]
        return len(changed_files) > 0, changed_files
    except subprocess.CalledProcessError:
        return False, []


def get_available_instances() -> list[str]:
    """Get list of configured Superset instances from sup CLI."""
    try:
        result = run_sup_command(["instance", "list"], check=False, capture_output=True)
        # Parse instance names from output
        instances = []
        for line in result.stdout.split("\n"):
            line = line.strip()
            # Look for lines that start with instance names (indented)
            if line and not line.startswith("Instances:"):
                # Extract instance name (first word)
                parts = line.split()
                if parts:
                    instances.append(parts[0])
        return instances
    except Exception:
        return []

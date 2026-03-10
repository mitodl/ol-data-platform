"""Apply governance RLS policies to a Superset instance."""

import json
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter
from rich.console import Console
from rich.table import Table

from ol_superset.lib.superset_api import (
    apply_rls_filter,
    create_authenticated_session,
    get_instance_config,
    list_datasets_name_to_id,
    list_rls_filters,
    list_roles,
)
from ol_superset.lib.utils import confirm_action

console = Console()

# Default policy file bundled with the package (policies/ next to src/).
_DEFAULT_POLICY_FILE = (
    Path(__file__).parent.parent.parent / "policies" / "ol_rls_policies.json"
)


def apply_rls(
    instance: Annotated[
        str,
        Parameter(help="Target instance name (e.g., superset-production, superset-qa)"),
    ],
    policy_file: Annotated[
        str | None,
        Parameter(
            name=["--policy-file", "-p"],
            help=(
                "Path to RLS policy JSON file "
                f"(default: {_DEFAULT_POLICY_FILE.name} bundled with ol-superset)"
            ),
        ),
    ] = None,
    dry_run: Annotated[
        bool,
        Parameter(
            name=["--dry-run", "-n"],
            help="Show what would be applied without making any changes",
        ),
    ] = False,
    skip_confirmation: Annotated[
        bool,
        Parameter(name=["--yes", "-y"], help="Skip confirmation prompt"),
    ] = False,
) -> None:
    """
    Apply OL governance row-level security (RLS) policies to a Superset instance.

    Reads RLS filter definitions from the bundled ol_rls_policies.json (or a
    custom file), resolves role and dataset names to their Superset IDs via the
    REST API, then creates or updates each filter idempotently.

    Authenticates using your personal Keycloak credentials via OAuth — no shared
    admin password is required.

    Examples:
        Apply policies to production (with confirmation):
            ol-superset apply-rls superset-production

        Apply policies to QA without prompting:
            ol-superset apply-rls superset-qa --yes

        Preview what would be applied:
            ol-superset apply-rls superset-production --dry-run

        Use a custom policy file:
            ol-superset apply-rls superset-qa --policy-file /path/to/policies.json
    """
    resolved_policy_file = Path(policy_file) if policy_file else _DEFAULT_POLICY_FILE

    console.print()
    console.rule("[bold]Applying OL Governance RLS Policies[/bold]")
    console.print()
    console.print(f"Instance:    [cyan]{instance}[/cyan]")
    console.print(f"Policy file: [cyan]{resolved_policy_file}[/cyan]")
    if dry_run:
        console.print("[yellow]DRY RUN MODE — no changes will be made[/yellow]")
    console.print()

    # Load policy definitions.
    if not resolved_policy_file.exists():
        console.print(f"[red]❌ Policy file not found: {resolved_policy_file}[/red]")
        sys.exit(1)

    try:
        policy_data = json.loads(resolved_policy_file.read_text())
        policies: list[dict[str, object]] = policy_data["rls_filters"]
    except (json.JSONDecodeError, KeyError) as exc:
        console.print(f"[red]❌ Failed to parse policy file: {exc}[/red]")
        sys.exit(1)

    console.print(f"Loaded [bold]{len(policies)}[/bold] RLS filter definition(s):")
    for p in policies:
        console.print(
            f"  • [white]{p['name']}[/white] "
            f"([dim]{p['filter_type']}[/dim], "
            f"roles: {', '.join(p['roles'])}, "  # type: ignore[arg-type]
            f"tables: {len(p['tables'])})"  # type: ignore[arg-type]
        )
    console.print()

    # Get instance config and authenticate.
    config = get_instance_config(instance)
    if not config:
        console.print("[red]❌ Could not get instance configuration[/red]")
        sys.exit(1)

    base_url = config["url"]
    is_production = "production" in instance.lower()

    if is_production and not dry_run:
        console.print(
            "[yellow]⚠️  WARNING: Target is the PRODUCTION environment[/yellow]"
        )
        console.print()

    if not dry_run and not skip_confirmation:
        prompt = (
            "⚠️  Apply RLS policies to PRODUCTION?"
            if is_production
            else "Apply RLS policies?"
        )
        require = "APPLY TO PRODUCTION" if is_production else None
        confirmed = confirm_action(prompt, require_exact=require)
        if not confirmed:
            console.print("Cancelled.")
            sys.exit(0)
        console.print()

    console.print("🔐 Authenticating with Superset API...")
    session = create_authenticated_session(instance)
    if not session:
        console.print("[red]❌ Could not create authenticated session[/red]")
        sys.exit(1)
    console.print(f"✅ Authenticated to {base_url}")
    console.print()

    # Build name→id lookup maps.
    console.print("📋 Fetching roles, datasets, and existing RLS filters...")
    try:
        role_map = list_roles(session, base_url)
        dataset_map = list_datasets_name_to_id(session, base_url)
        existing = list_rls_filters(session, base_url)
    except Exception as exc:
        console.print(f"[red]❌ Failed to fetch Superset data: {exc}[/red]")
        sys.exit(1)

    console.print(
        f"  Found [bold]{len(role_map)}[/bold] roles, "
        f"[bold]{len(dataset_map)}[/bold] dataset entries, "
        f"[bold]{len(existing)}[/bold] existing RLS filters"
    )
    console.print()

    # Show dry-run plan as a table.
    if dry_run:
        table = Table(title="RLS filters that would be applied")
        table.add_column("Filter name", style="cyan")
        table.add_column("Action", style="yellow")
        table.add_column("Type", style="white")
        table.add_column("Roles", style="magenta")
        table.add_column("Tables", style="dim")

        for p in policies:
            name = str(p["name"])
            action = "UPDATE" if name in existing else "CREATE"
            roles_str = ", ".join(p["roles"])  # type: ignore[arg-type]
            tables_str = str(len(p["tables"])) + " table(s)"  # type: ignore[arg-type]
            table.add_row(name, action, str(p["filter_type"]), roles_str, tables_str)

        console.print(table)
        console.print()

    # Apply each filter.
    console.print(
        f"{'🔍 Previewing' if dry_run else '🔧 Applying'} "
        f"{len(policies)} RLS filter(s)..."
    )
    console.print()

    success_count = 0
    failed: list[str] = []

    for policy in policies:
        try:
            ok = apply_rls_filter(
                session,
                base_url,
                policy,
                role_map,
                dataset_map,
                existing,
                dry_run=dry_run,
            )
            if ok:
                success_count += 1
            else:
                failed.append(str(policy["name"]))
        except ValueError as exc:
            console.print(f"  [red]❌ {exc}[/red]")
            failed.append(str(policy["name"]))
        except Exception as exc:
            name_str = str(policy["name"])
            console.print(f"  [red]❌ Unexpected error on '{name_str}': {exc}[/red]")
            failed.append(str(policy["name"]))

    console.print()
    console.rule(
        "[bold green]Complete[/bold green]"
        if not failed
        else "[bold red]Completed with errors[/bold red]"
    )
    console.print()

    if dry_run:
        msg = (
            f"[green]✅ Dry run complete — "
            f"{success_count} filter(s) would be applied[/green]"
        )
        console.print(msg)
        console.print("[dim]Remove --dry-run to apply changes.[/dim]")
    else:
        console.print(
            f"[green]✅ Successfully applied {success_count} RLS filter(s)[/green]"
        )
        if failed:
            console.print(
                f"[red]❌ Failed to apply {len(failed)} filter(s): "
                f"{', '.join(failed)}[/red]"
            )
            sys.exit(1)

    console.print()

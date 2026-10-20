"""`ol-dbt exposures` — write the column contract of each application exposure to ``exposures/``.

The check that the committed files are current is ``ol-dbt validate --only
exposure_contract``; see ``ol_dbt_cli.lib.exposure_contracts``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from rich.console import Console

from ol_dbt_cli.lib.exposure_contracts import DEFAULT_EXPOSURES_DIR, write_exposure_contracts
from ol_dbt_cli.lib.git_utils import get_repo_root

console = Console()
err_console = Console(stderr=True)

exposures_app = App(
    name="exposures",
    help="Write the column contract of each application exposure to exposures/.",
)


@exposures_app.command(name="export")
def export(
    dbt_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--dbt-dir", "-d"],
            help="Path to the dbt project root. Defaults to src/ol_dbt under the repo root.",
        ),
    ] = None,
    exposures_dir_path: Annotated[
        str | None,
        Parameter(
            name=["--exposures-dir"],
            help="Where the contract files go. Defaults to <repo>/exposures, two levels above --dbt-dir.",
        ),
    ] = None,
) -> None:
    """Regenerate exposures/<name>.json from the model YAML and remove files of exposures that are gone."""
    dbt_dir = Path(dbt_dir_path).resolve() if dbt_dir_path else get_repo_root() / "src" / "ol_dbt"
    exposures_dir = (
        Path(exposures_dir_path).resolve() if exposures_dir_path else dbt_dir.parents[1] / DEFAULT_EXPOSURES_DIR
    )
    contracts = write_exposure_contracts(dbt_dir, exposures_dir)
    if contracts.problems:
        for name, problems in contracts.problems.items():
            for problem in problems:
                err_console.print(f"[bold red]{name}:[/] {problem}")
        err_console.print("Nothing was written.")
        raise SystemExit(1)
    for name in contracts.files:
        console.print(f"wrote {exposures_dir / name}.json")

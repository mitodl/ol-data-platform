#!/usr/bin/env python3
# ruff: noqa: T201
"""Run a uv command in every uv project in the repository that has its own lockfile.

A project has its own lockfile when its pyproject.toml is tracked in git and it is
not a member of the root workspace. Each one resolves separately and embeds the
metadata of its path dependencies, so a change to e.g.
packages/ol-orchestrate-lib/pyproject.toml makes every lockfile that pulls it in
stale, not only the root uv.lock.
"""

import os
import subprocess
import sys
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter

REPO_ROOT = Path(__file__).resolve().parent.parent

app = App(help="Run uv commands in every uv project that has its own lockfile.")


def lock_roots() -> list[Path]:
    """
    Find the projects that own a uv.lock.

    :returns: Project directories, sorted, the repository root first.
    :rtype: list[Path]
    """
    tracked = subprocess.run(
        ["git", "ls-files", "-z", "--", ":(glob)**/pyproject.toml"],  # noqa: S607
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    members = subprocess.run(
        ["uv", "workspace", "list", "--paths"],  # noqa: S607
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    projects = {(REPO_ROOT / path).parent for path in tracked.split("\0") if path}
    member_dirs = {Path(path).resolve() for path in members.splitlines()} - {REPO_ROOT}
    return sorted(projects - member_dirs)


def run_in_each(uv_args: list[str], *, fail_fast: bool) -> None:
    """
    Run ``uv <uv_args>`` in each lock root and exit non-zero if any of them failed.

    :param uv_args: Arguments passed to uv.
    :param fail_fast: Stop at the first project that fails.
    """
    # Under `uv run`, VIRTUAL_ENV points at the root venv, and uv warns in every
    # other project that it does not match the project environment.
    env = {key: value for key, value in os.environ.items() if key != "VIRTUAL_ENV"}
    failed: list[str] = []
    for project in lock_roots():
        name = project.relative_to(REPO_ROOT).as_posix()
        print(f"==> uv {' '.join(uv_args)}  [{name}]", flush=True)
        result = subprocess.run(["uv", *uv_args], cwd=project, env=env, check=False)  # noqa: S603, S607
        if result.returncode:
            failed.append(name)
            if fail_fast:
                break
    if failed:
        print(f"\nFailed in: {', '.join(failed)}", file=sys.stderr)
        sys.exit(1)


@app.command(name="list")
def list_projects() -> None:
    """Print the projects that have their own lockfile."""
    for project in lock_roots():
        print(project.relative_to(REPO_ROOT).as_posix())


@app.command
def lock(
    *,
    check: bool = False,
    upgrade: bool = False,
    upgrade_package: tuple[str, ...] = (),
    fail_fast: bool = False,
) -> None:
    """
    Lock every project.

    :param check: Fail if any lockfile is out of date instead of updating it.
    :param upgrade: Upgrade every package to its latest allowed version.
    :param upgrade_package: Upgrade only this package. Repeatable.
    :param fail_fast: Stop at the first project that fails.
    """
    uv_args = ["lock"]
    if check:
        uv_args.append("--check")
    if upgrade:
        uv_args.append("--upgrade")
    for package in upgrade_package:
        uv_args.extend(["--upgrade-package", package])
    run_in_each(uv_args, fail_fast=fail_fast)


@app.command
def sync(*, fail_fast: bool = False) -> None:
    """
    Sync every project's environment from its lockfile.

    :param fail_fast: Stop at the first project that fails.
    """
    run_in_each(["sync", "--locked"], fail_fast=fail_fast)


@app.command(name="exec")
def exec_uv(
    *uv_args: Annotated[str, Parameter(allow_leading_hyphen=True)],
    fail_fast: bool = False,
) -> None:
    """
    Run any uv command in every project, e.g. ``exec pip list`` or ``exec tree``.

    :param uv_args: Arguments passed to uv.
    :param fail_fast: Stop at the first project that fails.
    """
    run_in_each(list(uv_args), fail_fast=fail_fast)


if __name__ == "__main__":
    app()

"""Output and JSON Patch writes shared by the `ol-dbt ... sync` commands that patch OpenMetadata entities."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from rich.console import Console
from rich.markup import escape

from ol_dbt_cli.lib.openmetadata import JSON_PATCH_CONTENT_TYPE

if TYPE_CHECKING:
    from ol_dbt_cli.lib.openmetadata import OpenMetadataClient

console = Console()

# Sync output labels are padded to this many columns.
LABEL_WIDTH = 10


def say(style: str, label: str, subject: str, message: str) -> None:
    """Print one sync result line, e.g. ``patched   glossary Learning: description``."""
    console.print(f"[{style}]{label.ljust(LABEL_WIDTH)}[/]{escape(subject)}: {escape(message)}")


def apply_patch(  # noqa: PLR0913
    client: OpenMetadataClient,
    subject: str,
    path: str,
    ops: list[dict[str, Any]],
    *,
    dry_run: bool,
    unchanged: str = "matches the file",
) -> None:
    """Send *ops* to *path* as a JSON Patch, or report that there are none or what a dry run would send.

    :param unchanged: the message printed when *ops* is empty.
    """
    if not ops:
        say("green", "unchanged", subject, unchanged)
        return
    fields = ", ".join(op["path"].lstrip("/") for op in ops)
    if dry_run:
        say("green", "would", subject, f"PATCH {fields}")
        return
    client.request("PATCH", path, ops, content_type=JSON_PATCH_CONTENT_TYPE)
    say("green", "patched", subject, fields)

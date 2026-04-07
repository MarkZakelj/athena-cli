"""Compare local table definitions against remote Athena tables."""

from __future__ import annotations

from dataclasses import dataclass

from rich import print as rprint

from athena_cli.schema import TableDefinition
from athena_cli.types import normalize_type


@dataclass
class TableDiff:
    """A single difference between local and remote table state."""

    kind: str  # column_added, column_removed, column_type_changed, partition_changed, location_changed, format_changed
    column: str | None  # column name, if applicable
    local_type: str | None  # local type, if applicable
    remote_type: str | None  # remote type, if applicable
    description: str  # human-readable description


def diff_table(local: TableDefinition, remote: dict) -> list[TableDiff]:
    """Compare a local TableDefinition against a remote table dict from Glue.

    Returns a list of differences.
    """
    diffs: list[TableDiff] = []

    local_cols = {k: normalize_type(v) for k, v in local.columns.items()}
    remote_cols = remote.get("columns", {})

    # Columns added locally (not in remote)
    for col in local_cols:
        if col not in remote_cols:
            diffs.append(TableDiff(
                kind="column_added",
                column=col,
                local_type=local_cols[col],
                remote_type=None,
                description=f"Column '{col}' ({local_cols[col]}) exists locally but not in Athena",
            ))

    # Columns removed locally (in remote but not local)
    for col in remote_cols:
        if col not in local_cols:
            diffs.append(TableDiff(
                kind="column_removed",
                column=col,
                local_type=None,
                remote_type=remote_cols[col],
                description=f"Column '{col}' ({remote_cols[col]}) exists in Athena but not locally",
            ))

    # Column type changes
    for col in local_cols:
        if col in remote_cols:
            lt = local_cols[col]
            rt = remote_cols[col]
            if lt != rt:
                diffs.append(TableDiff(
                    kind="column_type_changed",
                    column=col,
                    local_type=lt,
                    remote_type=rt,
                    description=f"Column '{col}' type: local={lt}, remote={rt}",
                ))

    # Partition changes
    local_parts = {k: normalize_type(v) for k, v in (local.partitions or {}).items()}
    remote_parts = remote.get("partitions", {})
    if local_parts != remote_parts:
        diffs.append(TableDiff(
            kind="partition_changed",
            column=None,
            local_type=str(local_parts) if local_parts else None,
            remote_type=str(remote_parts) if remote_parts else None,
            description=f"Partition columns differ: local={local_parts}, remote={remote_parts}",
        ))

    # Location change
    local_loc = (local.location or "").rstrip("/")
    remote_loc = remote.get("location", "").rstrip("/")
    if local_loc and remote_loc and local_loc != remote_loc:
        diffs.append(TableDiff(
            kind="location_changed",
            column=None,
            local_type=local_loc,
            remote_type=remote_loc,
            description=f"Location differs: local={local_loc}, remote={remote_loc}",
        ))

    # Format change
    local_fmt = local.format.lower()
    remote_fmt = remote.get("format", "").lower()
    if remote_fmt and local_fmt != remote_fmt:
        diffs.append(TableDiff(
            kind="format_changed",
            column=None,
            local_type=local_fmt,
            remote_type=remote_fmt,
            description=f"Format differs: local={local_fmt}, remote={remote_fmt}",
        ))

    return diffs


def print_diff(diffs: list[TableDiff]) -> None:
    """Print a list of diffs to the console."""
    for d in diffs:
        if d.kind == "column_added":
            rprint(f"  [green]+ {d.column}[/green] ({d.local_type})")
        elif d.kind == "column_removed":
            rprint(f"  [red]- {d.column}[/red] ({d.remote_type})")
        elif d.kind == "column_type_changed":
            rprint(f"  [yellow]~ {d.column}[/yellow]: {d.remote_type} -> {d.local_type}")
        elif d.kind == "partition_changed":
            rprint(f"  [red]⚠ Partitions changed:[/red] {d.description}")
        elif d.kind == "location_changed":
            rprint(f"  [yellow]⚠ Location changed:[/yellow] {d.remote_type} -> {d.local_type}")
        elif d.kind == "format_changed":
            rprint(f"  [yellow]⚠ Format changed:[/yellow] {d.remote_type} -> {d.local_type}")

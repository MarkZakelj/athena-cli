"""CLI entrypoint — Typer app with all commands."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from athena_cli.ddl import (
    generate_alter_add_columns,
    generate_alter_change_column,
    generate_create_table,
    generate_drop_table,
    generate_msck_repair,
)
from athena_cli.discovery import find_schema_file
from athena_cli.schema import SchemaConfig, SchemaFile, TableDefinition, parse_schema_file

app = typer.Typer(
    name="athena-cli",
    help="Manage AWS Athena table schemas via table_definitions.yaml",
    no_args_is_help=True,
)
console = Console()


@app.callback()
def main(
    profile: Annotated[
        Optional[str],
        typer.Option("--profile", "-p", help="AWS profile to use"),
    ] = None,
) -> None:
    if profile:
        from athena_cli.athena_client import init_session

        init_session(profile)

# ---------------------------------------------------------------------------
# Completion helpers
# ---------------------------------------------------------------------------


def _load_table_names_silent() -> list[str]:
    """Load table names from the schema file, silently returning empty on failure."""
    try:
        path = find_schema_file(silent=True)
        if path is None:
            return []
        schema = parse_schema_file(path)
        return schema.table_names()
    except Exception:
        return []


def complete_table_name(incomplete: str) -> list[str]:
    """Tab-completion for table names."""
    return [t for t in _load_table_names_silent() if t.startswith(incomplete)]


# ---------------------------------------------------------------------------
# Common options
# ---------------------------------------------------------------------------

SchemaPathOption = Annotated[
    Optional[Path],
    typer.Option("--schema", "-s", help="Path to table_definitions.yaml (auto-discovered if omitted)"),
]


def _load_schema(schema_path: Path | None) -> tuple[Path, SchemaFile]:
    """Load and return the schema file, handling errors."""
    try:
        path = schema_path or find_schema_file()
    except (FileNotFoundError, ValueError) as e:
        rprint(f"[red]Error:[/red] {e}")
        raise typer.Exit(1)
    try:
        schema = parse_schema_file(path)
    except ValueError as e:
        rprint(f"[red]Schema error in {path}:[/red]\n{e}")
        raise typer.Exit(1)
    return path, schema


def _get_table(schema: SchemaFile, table_name: str) -> TableDefinition:
    """Get a table by name or exit with an error."""
    if table_name not in schema.tables:
        rprint(f"[red]Error:[/red] Table '{table_name}' not found in schema")
        rprint(f"Available tables: {', '.join(schema.table_names())}")
        raise typer.Exit(1)
    return schema.tables[table_name]


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


@app.command()
def init(
    path: Annotated[
        Path, typer.Argument(help="Where to create the file")
    ] = Path("table_definitions.yaml"),
) -> None:
    """Create a starter table_definitions.yaml file."""
    if path.exists():
        rprint(f"[yellow]Warning:[/yellow] {path} already exists")
        overwrite = typer.confirm("Overwrite?", default=False)
        if not overwrite:
            raise typer.Exit(0)

    content = """\
# Athena table definitions
# Docs: https://github.com/MarkZakelj/athena-cli

_config:
  database: my_database
  # catalog: AwsDataCatalog
  # workgroup: primary

example_table:
  location: s3://my-bucket/data/example_table/
  format: parquet
  description: "Example table"
  columns:
    id: bigint
    name: string
    created_at: timestamp
  partitions:
    dt: date
"""
    path.write_text(content)
    rprint(f"[green]Created[/green] {path}")


@app.command()
def validate(
    schema_path: SchemaPathOption = None,
) -> None:
    """Validate table_definitions.yaml for correctness."""
    path, schema = _load_schema(schema_path)

    table = Table(title=f"Validated {path}")
    table.add_column("Table", style="cyan")
    table.add_column("Database", style="green")
    table.add_column("Format", style="yellow")
    table.add_column("Columns", justify="right")
    table.add_column("Partition Cols", justify="right")
    table.add_column("Location")

    for name, tbl in schema.tables.items():
        try:
            db = tbl.resolved_database(schema.config)
        except ValueError:
            db = "[red]MISSING[/red]"
        table.add_row(
            name,
            db,
            tbl.format,
            str(len(tbl.columns)),
            str(len(tbl.partitions)) if tbl.partitions else "0",
            tbl.location or "[dim]not set[/dim]",
        )

    console.print(table)
    rprint(f"\n[green]✓[/green] {len(schema.tables)} table(s) valid")


@app.command()
def status(
    schema_path: SchemaPathOption = None,
    table_name: Annotated[
        Optional[str],
        typer.Argument(help="Table to check (all if omitted)", autocompletion=complete_table_name),
    ] = None,
) -> None:
    """Compare local schema against live Athena tables."""
    from athena_cli.athena_client import get_glue_table
    from athena_cli.diff import diff_table, print_diff

    path, schema = _load_schema(schema_path)
    tables_to_check = [_get_table(schema, table_name)] if table_name else list(schema.tables.values())

    for tbl in tables_to_check:
        db = tbl.resolved_database(schema.config)
        rprint(f"\n[bold]{db}.{tbl.name}[/bold]")
        try:
            remote = get_glue_table(db, tbl.name, schema.config.catalog)
        except Exception as e:
            rprint(f"  [yellow]Not found in Athena:[/yellow] {e}")
            continue
        diffs = diff_table(tbl, remote)
        if not diffs:
            rprint("  [green]✓ In sync[/green]")
        else:
            print_diff(diffs)


@app.command()
def push(
    schema_path: SchemaPathOption = None,
    table_name: Annotated[
        Optional[str],
        typer.Argument(help="Table to push (all if omitted)", autocompletion=complete_table_name),
    ] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Print DDL without executing")] = False,
    force: Annotated[bool, typer.Option("--force", help="Allow destructive changes (drop + recreate)")] = False,
) -> None:
    """Push local schema to Athena (CREATE or ALTER tables)."""
    from athena_cli.athena_client import execute_ddl, get_glue_table
    from athena_cli.diff import diff_table

    path, schema = _load_schema(schema_path)
    tables_to_push = [_get_table(schema, table_name)] if table_name else list(schema.tables.values())

    for tbl in tables_to_push:
        db = tbl.resolved_database(schema.config)
        rprint(f"\n[bold]{db}.{tbl.name}[/bold]")

        if tbl.location is None:
            rprint("  [red]Error:[/red] 'location' is required for push on new tables")
            continue

        # Check if table exists
        try:
            remote = get_glue_table(db, tbl.name, schema.config.catalog)
        except Exception:
            remote = None

        if remote is None:
            # CREATE
            ddl = generate_create_table(tbl, db)
            rprint(f"  [green]CREATE TABLE[/green] {db}.{tbl.name}")
            if dry_run:
                rprint(f"\n[dim]{ddl}[/dim]\n")
            else:
                execute_ddl(ddl, schema.config)
                rprint("  [green]✓ Created[/green]")
        else:
            # Check diff and ALTER
            diffs = diff_table(tbl, remote)
            if not diffs:
                rprint("  [green]✓ Already in sync[/green]")
                continue

            # Categorize changes
            new_columns = {d.column: d.local_type for d in diffs if d.kind == "column_added"}
            widenings = {d.column: d.local_type for d in diffs if d.kind == "column_type_widened"}
            destructive = [d for d in diffs if d.kind in ("column_removed", "column_type_changed", "partition_changed")]

            if destructive:
                for d in destructive:
                    rprint(f"  [red]⚠ {d.description}[/red]")
                if not force:
                    rprint("  [yellow]Use --force to drop and recreate the table[/yellow]")
                    continue
                # Force: drop + recreate
                drop_ddl = generate_drop_table(tbl.name, db)
                create_ddl = generate_create_table(tbl, db)
                rprint(f"  [red]DROP + CREATE[/red] {db}.{tbl.name}")
                if dry_run:
                    rprint(f"\n[dim]{drop_ddl}[/dim]\n[dim]{create_ddl}[/dim]\n")
                else:
                    if not typer.confirm(f"  Drop and recreate {db}.{tbl.name}?", default=False):
                        continue
                    execute_ddl(drop_ddl, schema.config)
                    execute_ddl(create_ddl, schema.config)
                    rprint("  [green]✓ Recreated[/green]")
            else:
                if widenings:
                    for col_name, new_type in widenings.items():
                        ddl = generate_alter_change_column(tbl.name, db, col_name, new_type)
                        rprint(f"  [cyan]ALTER CHANGE COLUMN[/cyan] {col_name} -> {new_type}")
                        if dry_run:
                            rprint(f"    [dim]{ddl}[/dim]")
                        else:
                            execute_ddl(ddl, schema.config)
                    if not dry_run:
                        rprint(f"  [green]✓ {len(widenings)} column(s) widened[/green]")

                if new_columns:
                    ddl = generate_alter_add_columns(tbl.name, db, new_columns)
                    rprint(f"  [cyan]ALTER TABLE ADD COLUMNS[/cyan]: {', '.join(new_columns.keys())}")
                    if dry_run:
                        rprint(f"\n[dim]{ddl}[/dim]\n")
                    else:
                        execute_ddl(ddl, schema.config)
                        rprint("  [green]✓ Columns added[/green]")


@app.command()
def pull(
    schema_path: SchemaPathOption = None,
    table_name: Annotated[
        Optional[str],
        typer.Option("--table", "-t", help="Pull a specific table"),
    ] = None,
    database: Annotated[
        Optional[str],
        typer.Option("--database", "-d", help="Database to pull from (overrides _config)"),
    ] = None,
    merge: Annotated[bool, typer.Option("--merge", help="Add new tables without touching existing")] = False,
) -> None:
    """Pull table definitions from Athena into table_definitions.yaml."""
    from athena_cli.athena_client import get_glue_table, list_glue_tables

    # Try to load existing schema, or start fresh
    try:
        path = schema_path or find_schema_file()
        schema = parse_schema_file(path)
    except FileNotFoundError:
        path = Path("table_definitions.yaml")
        schema = SchemaFile(config=SchemaConfig(), tables={})

    db = database or schema.config.database
    if not db:
        rprint("[red]Error:[/red] No database specified. Use --database or set it in _config")
        raise typer.Exit(1)

    # Get tables to pull
    if table_name:
        remote_names = [table_name]
    else:
        remote_names = list_glue_tables(db, schema.config.catalog)
        rprint(f"Found [cyan]{len(remote_names)}[/cyan] tables in {db}")

    pulled = 0
    for rname in remote_names:
        if merge and rname in schema.tables:
            rprint(f"  [dim]Skipping {rname} (already in schema)[/dim]")
            continue

        if not merge and rname in schema.tables:
            rprint(f"  [yellow]Warning:[/yellow] '{rname}' already exists in schema")
            if not typer.confirm(f"  Overwrite '{rname}'?", default=False):
                continue
            # Overwriting an existing table requires rewriting that section —
            # for now, warn that comments on that table block may be lost
            rprint(f"  [dim]Note: comments on '{rname}' block may be lost[/dim]")

        try:
            remote = get_glue_table(db, rname, schema.config.catalog)
            table_dict = _remote_to_dict(remote)
            if rname in schema.tables:
                _replace_table_in_yaml(path, rname, table_dict)
            else:
                _append_table_to_yaml(path, rname, table_dict)
            rprint(f"  [green]✓[/green] Pulled {rname}")
            pulled += 1
        except Exception as e:
            rprint(f"  [red]Error pulling {rname}:[/red] {e}")

    rprint(f"\n[green]Pulled {pulled} table(s)[/green] into {path}")


@app.command()
def repair(
    table_name: Annotated[
        str, typer.Argument(help="Table to repair", autocompletion=complete_table_name)
    ],
    schema_path: SchemaPathOption = None,
) -> None:
    """Run MSCK REPAIR TABLE to discover partitions."""
    from athena_cli.athena_client import execute_ddl, list_partitions

    _, schema = _load_schema(schema_path)
    tbl = _get_table(schema, table_name)
    db = tbl.resolved_database(schema.config)

    if not tbl.partitions:
        rprint(f"[yellow]Warning:[/yellow] Table '{table_name}' has no partitions defined")
        raise typer.Exit(0)

    rprint(f"Fetching current partitions for {db}.{tbl.name}...")
    before = set(list_partitions(db, tbl.name, schema.config))

    ddl = generate_msck_repair(tbl.name, db)
    rprint(f"Running: [dim]{ddl}[/dim]")
    execute_ddl(ddl, schema.config)

    after = set(list_partitions(db, tbl.name, schema.config))
    new_partitions = sorted(after - before)

    if new_partitions:
        rprint(f"[green]✓[/green] Repaired {db}.{tbl.name} — discovered {len(new_partitions)} new partition(s) (was {len(before)}, now {len(after)})")
        for p in new_partitions:
            rprint(f"  [green]+ {p}[/green]")
    else:
        rprint(f"[green]✓[/green] Repaired {db}.{tbl.name} — no new partitions ({len(after)} total)")


@app.command()
def drop(
    table_name: Annotated[
        str, typer.Argument(help="Table to drop", autocompletion=complete_table_name)
    ],
    schema_path: SchemaPathOption = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation")] = False,
) -> None:
    """Drop an Athena table."""
    from athena_cli.athena_client import execute_ddl

    _, schema = _load_schema(schema_path)
    tbl = _get_table(schema, table_name)
    db = tbl.resolved_database(schema.config)

    if not yes:
        if not typer.confirm(f"Drop table {db}.{tbl.name}?", default=False):
            raise typer.Exit(0)

    ddl = generate_drop_table(tbl.name, db)
    rprint(f"Running: [dim]{ddl}[/dim]")
    execute_ddl(ddl, schema.config)
    rprint(f"[green]✓[/green] Dropped {db}.{tbl.name}")


@app.command()
def infer(
    table_name: Annotated[str, typer.Argument(help="Name for the new table")],
    s3_url: Annotated[str, typer.Argument(help="S3 location (e.g. s3://bucket/path/)")],
    schema_path: SchemaPathOption = None,
) -> None:
    """Infer a table definition from an S3 location and add it to the schema."""
    from athena_cli.infer import read_orc_schema, read_parquet_schema, scan_s3_location

    # Load or create schema file
    try:
        path = schema_path or find_schema_file()
        schema = parse_schema_file(path)
    except FileNotFoundError:
        path = Path("table_definitions.yaml")
        schema = SchemaFile(config=SchemaConfig(), tables={})

    if table_name in schema.tables:
        rprint(f"[yellow]Warning:[/yellow] '{table_name}' already exists in schema")
        if not typer.confirm("Overwrite?", default=False):
            raise typer.Exit(0)

    rprint(f"Scanning [cyan]{s3_url}[/cyan]...")
    scan = scan_s3_location(s3_url)

    rprint(f"  Format: [green]{scan['format']}[/green] ({scan['file_count']} files)")

    if scan["partitions"]:
        parts_str = ", ".join(f"{k} ({v})" for k, v in scan["partitions"].items())
        rprint(f"  Partitions: [green]{parts_str}[/green]")
    else:
        rprint("  Partitions: [dim]none[/dim]")

    # Try to read column schema
    columns: dict[str, str] = {}
    if scan["sample_key"] and scan["format"] in ("parquet", "orc"):
        try:
            if scan["format"] == "parquet":
                columns = read_parquet_schema(scan["sample_bucket"], scan["sample_key"])
            else:
                columns = read_orc_schema(scan["sample_bucket"], scan["sample_key"])

            # Remove partition columns from regular columns
            for p in scan["partitions"]:
                columns.pop(p, None)

            rprint(f"  Columns: [green]{len(columns)}[/green] (from sample file)")
        except ImportError:
            rprint(
                "  [yellow]⚠ Install with infer support to auto-detect columns:[/yellow]\n"
                '    uv tool install "athena-cli[infer]"'
            )
    elif scan["format"] not in ("parquet", "orc"):
        rprint(f"  [dim]Column detection not supported for {scan['format']} — add columns manually[/dim]")

    # Build table entry
    table_dict: dict = {
        "location": s3_url.rstrip("/") + "/",
        "format": scan["format"],
        "columns": columns if columns else {"TODO_column_name": "string"},
    }
    if scan["partitions"]:
        table_dict["partitions"] = scan["partitions"]

    _append_table_to_yaml(path, table_name, table_dict)

    if columns:
        rprint(f"\n[green]✓[/green] Added '{table_name}' to {path}")
    else:
        rprint(f"\n[green]✓[/green] Added '{table_name}' (skeleton) to {path} — edit columns manually")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _table_to_dict(tbl: TableDefinition) -> dict:
    """Convert a TableDefinition to a plain dict for YAML output."""
    d: dict = {}
    if tbl.database:
        d["database"] = tbl.database
    if tbl.location:
        d["location"] = tbl.location
    d["format"] = tbl.format
    if tbl.description:
        d["description"] = tbl.description
    d["columns"] = dict(tbl.columns)
    if tbl.partitions:
        d["partitions"] = dict(tbl.partitions)
    if tbl.properties:
        d["properties"] = dict(tbl.properties)
    return d


def _remote_to_dict(remote: dict) -> dict:
    """Convert a remote table info dict to schema-compatible dict."""
    d: dict = {}
    if remote.get("location"):
        d["location"] = remote["location"]
    d["format"] = remote.get("format", "parquet")
    if remote.get("description"):
        d["description"] = remote["description"]
    d["columns"] = remote.get("columns", {})
    if remote.get("partitions"):
        d["partitions"] = remote["partitions"]
    return d


def _append_table_to_yaml(path: Path, table_name: str, table_dict: dict) -> None:
    """Append a table definition to the YAML file without rewriting existing content."""
    import yaml

    block = yaml.dump(
        {table_name: table_dict}, default_flow_style=False, sort_keys=False
    )

    if path.exists():
        existing = path.read_text()
        # Ensure there's an empty line separator
        if existing and not existing.endswith("\n\n"):
            existing = existing.rstrip("\n") + "\n\n"
        path.write_text(existing + block)
    else:
        path.write_text(block)


def _replace_table_in_yaml(path: Path, table_name: str, table_dict: dict) -> None:
    """Replace a table definition in the YAML file, preserving surrounding content."""
    import re

    import yaml

    content = path.read_text()
    lines = content.split("\n")

    # Find the table block: starts with "table_name:" at column 0,
    # ends before the next top-level key or EOF
    start_idx = None
    end_idx = None
    for i, line in enumerate(lines):
        if line.rstrip() == f"{table_name}:" or line.startswith(f"{table_name}:"):
            start_idx = i
        elif start_idx is not None and line and not line[0].isspace() and line[0] != "#":
            end_idx = i
            break

    if start_idx is None:
        # Table not found — just append
        _append_table_to_yaml(path, table_name, table_dict)
        return

    if end_idx is None:
        end_idx = len(lines)

    # Strip trailing blank lines from the block we're removing
    while end_idx > start_idx and not lines[end_idx - 1].strip():
        end_idx -= 1

    new_block = yaml.dump(
        {table_name: table_dict}, default_flow_style=False, sort_keys=False
    ).rstrip("\n")

    new_lines = lines[:start_idx] + [new_block] + lines[end_idx:]
    path.write_text("\n".join(new_lines))


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def app_entry() -> None:
    app()

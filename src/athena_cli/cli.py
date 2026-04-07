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
    table.add_column("Partitions", justify="right")
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
            elif new_columns:
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
    import yaml

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
    raw: dict = {}

    # Preserve _config
    raw["_config"] = schema.config.model_dump(exclude_defaults=True) or None
    if raw["_config"] is None:
        raw.pop("_config")

    # Preserve existing tables if merge mode
    if merge:
        for name, tbl in schema.tables.items():
            raw[name] = _table_to_dict(tbl)

    for rname in remote_names:
        if merge and rname in schema.tables:
            rprint(f"  [dim]Skipping {rname} (already in schema)[/dim]")
            continue

        if not merge and rname in schema.tables:
            rprint(f"  [yellow]Warning:[/yellow] '{rname}' already exists in schema")
            if not typer.confirm(f"  Overwrite '{rname}'?", default=False):
                raw[rname] = _table_to_dict(schema.tables[rname])
                continue

        try:
            remote = get_glue_table(db, rname, schema.config.catalog)
            raw[rname] = _remote_to_dict(remote)
            rprint(f"  [green]✓[/green] Pulled {rname}")
            pulled += 1
        except Exception as e:
            rprint(f"  [red]Error pulling {rname}:[/red] {e}")

    path.write_text(yaml.dump(raw, default_flow_style=False, sort_keys=False))
    rprint(f"\n[green]Pulled {pulled} table(s)[/green] into {path}")


@app.command()
def repair(
    table_name: Annotated[
        str, typer.Argument(help="Table to repair", autocompletion=complete_table_name)
    ],
    schema_path: SchemaPathOption = None,
) -> None:
    """Run MSCK REPAIR TABLE to discover partitions."""
    from athena_cli.athena_client import execute_ddl

    _, schema = _load_schema(schema_path)
    tbl = _get_table(schema, table_name)
    db = tbl.resolved_database(schema.config)

    if not tbl.partitions:
        rprint(f"[yellow]Warning:[/yellow] Table '{table_name}' has no partitions defined")
        raise typer.Exit(0)

    ddl = generate_msck_repair(tbl.name, db)
    rprint(f"Running: [dim]{ddl}[/dim]")
    execute_ddl(ddl, schema.config)
    rprint(f"[green]✓[/green] Repaired {db}.{tbl.name}")


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


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def app_entry() -> None:
    app()

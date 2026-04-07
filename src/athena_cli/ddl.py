"""DDL SQL generation for Athena tables."""

from __future__ import annotations

from athena_cli.schema import TableDefinition
from athena_cli.types import FORMAT_SERDE


def generate_create_table(table: TableDefinition, database: str) -> str:
    """Generate CREATE EXTERNAL TABLE DDL."""
    parts = [f"CREATE EXTERNAL TABLE `{database}`.`{table.name}` ("]

    # Columns
    col_lines = []
    for col_name, col_type in table.columns.items():
        col_lines.append(f"  `{col_name}` {col_type}")
    parts.append(",\n".join(col_lines))
    parts.append(")")

    # Comment
    if table.description:
        escaped = table.description.replace("'", "\\'")
        parts.append(f"COMMENT '{escaped}'")

    # Partitions
    if table.partitions:
        part_lines = []
        for p_name, p_type in table.partitions.items():
            part_lines.append(f"  `{p_name}` {p_type}")
        parts.append("PARTITIONED BY (")
        parts.append(",\n".join(part_lines))
        parts.append(")")

    # SerDe / format
    fmt = table.format.lower()
    serde_info = FORMAT_SERDE.get(fmt)
    if serde_info:
        parts.append(f"ROW FORMAT SERDE '{serde_info['serde']}'")
        parts.append(f"STORED AS INPUTFORMAT '{serde_info['input_format']}'")
        parts.append(f"OUTPUTFORMAT '{serde_info['output_format']}'")

    # Location
    if table.location:
        location = table.location.rstrip("/") + "/"
        parts.append(f"LOCATION '{location}'")

    # Table properties
    if table.properties:
        prop_lines = []
        for k, v in table.properties.items():
            prop_lines.append(f"  '{k}' = '{v}'")
        parts.append("TBLPROPERTIES (")
        parts.append(",\n".join(prop_lines))
        parts.append(")")

    return "\n".join(parts) + ";"


def generate_alter_add_columns(
    table_name: str, database: str, columns: dict[str, str]
) -> str:
    """Generate ALTER TABLE ADD COLUMNS DDL."""
    col_lines = []
    for col_name, col_type in columns.items():
        col_lines.append(f"  `{col_name}` {col_type}")

    return (
        f"ALTER TABLE `{database}`.`{table_name}` ADD COLUMNS (\n"
        + ",\n".join(col_lines)
        + "\n);"
    )


def generate_drop_table(table_name: str, database: str) -> str:
    """Generate DROP TABLE DDL."""
    return f"DROP TABLE IF EXISTS `{database}`.`{table_name}`;"


def generate_msck_repair(table_name: str, database: str) -> str:
    """Generate MSCK REPAIR TABLE DDL."""
    return f"MSCK REPAIR TABLE `{database}`.`{table_name}`;"

"""YAML schema parsing and pydantic models for table definitions."""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, field_validator, model_validator

from athena_cli.types import validate_athena_type


class SchemaConfig(BaseModel):
    """Top-level _config section."""

    database: str | None = None
    catalog: str = "AwsDataCatalog"
    workgroup: str = "primary"
    output_location: str | None = None


class TableDefinition(BaseModel):
    """A single Athena table definition."""

    name: str
    database: str | None = None
    location: str | None = None
    format: str = "parquet"
    description: str | None = None
    columns: dict[str, str]
    partitions: dict[str, str] | None = None
    properties: dict[str, str] | None = None

    @field_validator("format")
    @classmethod
    def validate_format(cls, v: str) -> str:
        allowed = {"parquet", "orc", "csv", "json", "avro"}
        if v.lower() not in allowed:
            raise ValueError(f"Unsupported format '{v}'. Must be one of: {', '.join(sorted(allowed))}")
        return v.lower()

    @field_validator("columns", "partitions")
    @classmethod
    def validate_column_types(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        if v is None:
            return v
        errors = []
        for col_name, col_type in v.items():
            if not validate_athena_type(col_type):
                errors.append(f"  Column '{col_name}' has invalid type '{col_type}'")
        if errors:
            raise ValueError("Invalid Athena types:\n" + "\n".join(errors))
        return v

    @model_validator(mode="after")
    def check_no_column_partition_overlap(self) -> TableDefinition:
        if self.partitions:
            overlap = set(self.columns.keys()) & set(self.partitions.keys())
            if overlap:
                raise ValueError(
                    f"Columns and partitions must not overlap. Shared names: {', '.join(sorted(overlap))}"
                )
        return self

    def resolved_database(self, config: SchemaConfig) -> str:
        """Get the database name, falling back to _config default."""
        db = self.database or config.database
        if not db:
            raise ValueError(f"Table '{self.name}' has no database set and no default in _config")
        return db


class SchemaFile(BaseModel):
    """Parsed table_definitions.yaml file."""

    config: SchemaConfig
    tables: dict[str, TableDefinition]

    def table_names(self) -> list[str]:
        return list(self.tables.keys())


def parse_schema_file(path: Path) -> SchemaFile:
    """Parse a table_definitions.yaml file into a SchemaFile."""
    with open(path) as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Expected a YAML mapping in {path}, got {type(raw).__name__}")

    config_raw = raw.pop("_config", {}) or {}
    config = SchemaConfig(**config_raw)

    tables: dict[str, TableDefinition] = {}
    errors: list[str] = []

    for table_name, table_data in raw.items():
        if not isinstance(table_data, dict):
            errors.append(f"Table '{table_name}': expected a mapping, got {type(table_data).__name__}")
            continue
        try:
            tables[table_name] = TableDefinition(name=table_name, **table_data)
        except Exception as e:
            errors.append(f"Table '{table_name}': {e}")

    if errors:
        raise ValueError("Schema validation errors:\n" + "\n".join(errors))

    return SchemaFile(config=config, tables=tables)

"""Tests for YAML schema parsing and validation."""

import textwrap

import pytest

from athena_cli.schema import SchemaConfig, TableDefinition, parse_schema_file

COMPLEX_MAP_STRUCT = "map<string,struct<value_double:double,value_string:string,contribution:double>>"


def test_table_with_complex_column_validates():
    table = TableDefinition(
        name="predictions",
        columns={"id": "bigint", "features": COMPLEX_MAP_STRUCT},
    )
    assert table.columns["features"] == COMPLEX_MAP_STRUCT


def test_invalid_column_type_rejected():
    with pytest.raises(ValueError, match="Invalid Athena types"):
        TableDefinition(name="t", columns={"bad": "notatype"})


def test_unsupported_format_rejected():
    with pytest.raises(ValueError, match="Unsupported format"):
        TableDefinition(name="t", columns={"a": "int"}, format="protobuf")


def test_column_partition_overlap_rejected():
    with pytest.raises(ValueError, match="must not overlap"):
        TableDefinition(
            name="t",
            columns={"a": "int", "dt": "string"},
            partitions={"dt": "string"},
        )


def test_resolved_database_falls_back_to_config():
    config = SchemaConfig(database="default_db")
    table = TableDefinition(name="t", columns={"a": "int"})
    assert table.resolved_database(config) == "default_db"


def test_resolved_database_prefers_table_level():
    config = SchemaConfig(database="default_db")
    table = TableDefinition(name="t", columns={"a": "int"}, database="own_db")
    assert table.resolved_database(config) == "own_db"


def test_resolved_database_missing_raises():
    config = SchemaConfig()
    table = TableDefinition(name="t", columns={"a": "int"})
    with pytest.raises(ValueError, match="no database set"):
        table.resolved_database(config)


def test_parse_schema_file_with_complex_type(tmp_path):
    yaml_content = textwrap.dedent(
        f"""
        _config:
          database: analytics
          workgroup: primary

        predictions:
          location: s3://bucket/predictions/
          format: parquet
          columns:
            id: bigint
            features: {COMPLEX_MAP_STRUCT}
          partitions:
            dt: string
        """
    )
    path = tmp_path / "table_definitions.yaml"
    path.write_text(yaml_content)

    schema = parse_schema_file(path)

    assert schema.config.database == "analytics"
    assert schema.table_names() == ["predictions"]
    table = schema.tables["predictions"]
    assert table.columns["features"] == COMPLEX_MAP_STRUCT
    assert table.resolved_database(schema.config) == "analytics"


def test_parse_schema_file_reports_errors(tmp_path):
    yaml_content = textwrap.dedent(
        """
        _config:
          database: analytics

        broken:
          columns:
            bad: notatype
        """
    )
    path = tmp_path / "table_definitions.yaml"
    path.write_text(yaml_content)

    with pytest.raises(ValueError, match="Schema validation errors"):
        parse_schema_file(path)

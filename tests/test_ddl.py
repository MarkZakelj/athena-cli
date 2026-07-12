"""Tests for DDL SQL generation."""

from athena_cli.ddl import (
    generate_alter_add_columns,
    generate_alter_change_column,
    generate_create_table,
    generate_drop_table,
    generate_msck_repair,
)
from athena_cli.schema import TableDefinition

COMPLEX_MAP_STRUCT = "map<string,struct<value_double:double,value_string:string,contribution:double>>"


def test_create_table_emits_complex_type_verbatim():
    table = TableDefinition(
        name="predictions",
        columns={"id": "bigint", "features": COMPLEX_MAP_STRUCT},
        partitions={"dt": "string"},
        location="s3://bucket/predictions/",
        format="parquet",
        description="model outputs",
    )
    ddl = generate_create_table(table, database="analytics")

    assert "CREATE EXTERNAL TABLE `analytics`.`predictions`" in ddl
    assert f"`features` {COMPLEX_MAP_STRUCT}" in ddl
    assert "`id` bigint" in ddl
    assert "PARTITIONED BY (" in ddl
    assert "`dt` string" in ddl
    assert "COMMENT 'model outputs'" in ddl
    assert "LOCATION 's3://bucket/predictions/'" in ddl
    assert "ParquetHiveSerDe" in ddl
    assert ddl.strip().endswith(";")


def test_create_table_trailing_slash_added_once():
    table = TableDefinition(
        name="t",
        columns={"a": "int"},
        location="s3://bucket/data",
    )
    ddl = generate_create_table(table, database="db")
    assert "LOCATION 's3://bucket/data/'" in ddl
    assert "data//" not in ddl


def test_create_table_escapes_comment_quotes():
    table = TableDefinition(
        name="t",
        columns={"a": "int"},
        description="it's a table",
    )
    ddl = generate_create_table(table, database="db")
    assert "COMMENT 'it\\'s a table'" in ddl


def test_alter_add_columns():
    ddl = generate_alter_add_columns("t", "db", {"features": COMPLEX_MAP_STRUCT})
    assert "ALTER TABLE `db`.`t` ADD COLUMNS (" in ddl
    assert f"`features` {COMPLEX_MAP_STRUCT}" in ddl


def test_alter_change_column():
    ddl = generate_alter_change_column("t", "db", "n", "bigint")
    assert ddl == "ALTER TABLE `db`.`t` CHANGE COLUMN `n` `n` bigint;"


def test_drop_table():
    assert generate_drop_table("t", "db") == "DROP TABLE IF EXISTS `db`.`t`;"


def test_msck_repair():
    assert generate_msck_repair("t", "db") == "MSCK REPAIR TABLE `db`.`t`;"

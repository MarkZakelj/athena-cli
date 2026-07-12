"""Tests for Athena type validation and mapping."""

import pytest

from athena_cli.types import (
    is_safe_widening,
    normalize_type,
    validate_athena_type,
)

COMPLEX_MAP_STRUCT = "map<string,struct<value_double:double,value_string:string,contribution:double>>"


@pytest.mark.parametrize(
    "type_str",
    [
        # primitives
        "int",
        "integer",
        "bigint",
        "double",
        "string",
        "boolean",
        "date",
        "timestamp",
        "binary",
        # parameterized
        "decimal(10,2)",
        "decimal(10, 2)",
        "varchar(255)",
        "char(3)",
        # complex
        "array<int>",
        "array<string>",
        "map<string,int>",
        "map<string, int>",
        "struct<a:int,b:string>",
        "struct<a:int, b:string>",
        # nested complex
        "array<struct<a:int,b:string>>",
        "map<string,array<int>>",
        "struct<x:map<string,int>,y:double>",
        "map<int,map<string,int>>",
        "array<map<string,struct<a:int,b:double>>>",
        # the regression case: map whose value is a comma-containing struct
        COMPLEX_MAP_STRUCT,
    ],
)
def test_valid_types(type_str):
    assert validate_athena_type(type_str) is True


@pytest.mark.parametrize(
    "type_str",
    [
        "notatype",
        "map<string>",  # map needs exactly key,value
        "map<string,int,double>",  # too many map args
        "struct<>",  # empty struct fields (no colon)
        "struct<a>",  # struct field without type
        "struct<a:notatype>",  # struct field with bad type
        "array<notatype>",
        "map<string,notatype>",
        "map<notatype,int>",
        "struct<1bad:int>",  # invalid field name
    ],
)
def test_invalid_types(type_str):
    assert validate_athena_type(type_str) is False


def test_case_insensitive():
    assert validate_athena_type("MAP<STRING,INT>") is True
    assert validate_athena_type("Array<Int>") is True


def test_whitespace_tolerant():
    assert validate_athena_type("  int  ") is True


class TestSafeWidening:
    @pytest.mark.parametrize(
        "from_type,to_type",
        [
            ("int", "bigint"),
            ("tinyint", "int"),
            ("tinyint", "bigint"),
            ("smallint", "bigint"),
            ("float", "double"),
            ("varchar(10)", "varchar(20)"),
            ("varchar(10)", "string"),
            ("char(5)", "string"),
            ("int", "int"),  # identity
        ],
    )
    def test_safe(self, from_type, to_type):
        assert is_safe_widening(from_type, to_type) is True

    @pytest.mark.parametrize(
        "from_type,to_type",
        [
            ("bigint", "int"),  # narrowing
            ("double", "float"),  # narrowing
            ("varchar(20)", "varchar(10)"),  # shrinking
            ("int", "string"),  # unrelated
            ("string", "int"),  # unrelated
            ("varchar(10)", "varchar(10)") if False else ("string", "varchar(10)"),
        ],
    )
    def test_unsafe(self, from_type, to_type):
        assert is_safe_widening(from_type, to_type) is False


class TestNormalizeType:
    def test_integer_alias(self):
        assert normalize_type("integer") == "int"

    def test_passthrough(self):
        assert normalize_type("bigint") == "bigint"

    def test_unknown_passthrough(self):
        assert normalize_type("map<string,int>") == "map<string,int>"

"""Athena type validation and mapping."""

import re

# Primitive types supported by Athena
PRIMITIVE_TYPES = frozenset(
    {
        "tinyint",
        "smallint",
        "int",
        "integer",
        "bigint",
        "float",
        "double",
        "decimal",
        "boolean",
        "string",
        "varchar",
        "char",
        "date",
        "timestamp",
        "binary",
    }
)

# Parameterized type patterns
_DECIMAL_RE = re.compile(r"^decimal\(\d+,\s*\d+\)$")
_VARCHAR_RE = re.compile(r"^varchar\(\d+\)$")
_CHAR_RE = re.compile(r"^char\(\d+\)$")

# Complex type patterns — recursive
_ARRAY_RE = re.compile(r"^array<(.+)>$")
_MAP_RE = re.compile(r"^map<(.+),\s*(.+)>$")
_STRUCT_RE = re.compile(r"^struct<(.+)>$")


def validate_athena_type(type_str: str) -> bool:
    """Validate that a type string is a valid Athena type.

    Supports primitives, parameterized types (decimal, varchar, char),
    and complex types (array, map, struct) with recursive validation.
    """
    type_str = type_str.strip().lower()

    if type_str in PRIMITIVE_TYPES:
        return True

    if _DECIMAL_RE.match(type_str) or _VARCHAR_RE.match(type_str) or _CHAR_RE.match(type_str):
        return True

    m = _ARRAY_RE.match(type_str)
    if m:
        return validate_athena_type(m.group(1))

    m = _MAP_RE.match(type_str)
    if m:
        return validate_athena_type(m.group(1)) and validate_athena_type(m.group(2))

    m = _STRUCT_RE.match(type_str)
    if m:
        return _validate_struct_fields(m.group(1))

    return False


def _validate_struct_fields(fields_str: str) -> bool:
    """Validate struct field definitions like 'name:string,age:int'."""
    fields = _split_top_level(fields_str, ",")
    for field in fields:
        field = field.strip()
        if ":" not in field:
            return False
        name, type_part = field.split(":", 1)
        name = name.strip()
        if not name or not re.match(r"^[a-z_][a-z0-9_]*$", name):
            return False
        if not validate_athena_type(type_part.strip()):
            return False
    return True


def _split_top_level(s: str, delimiter: str) -> list[str]:
    """Split string by delimiter, but only at the top level (not inside angle brackets)."""
    parts = []
    depth = 0
    current = []
    for char in s:
        if char == "<":
            depth += 1
        elif char == ">":
            depth -= 1
        if char == delimiter and depth == 0:
            parts.append("".join(current))
            current = []
        else:
            current.append(char)
    if current:
        parts.append("".join(current))
    return parts


# Safe type widenings — these can be done via ALTER TABLE CHANGE COLUMN
_INT_HIERARCHY = ["tinyint", "smallint", "int", "bigint"]
_FLOAT_HIERARCHY = ["float", "double"]

_WIDENING_RULES: dict[str, set[str]] = {}
for _hierarchy in [_INT_HIERARCHY, _FLOAT_HIERARCHY]:
    for i, _from in enumerate(_hierarchy):
        for _to in _hierarchy[i + 1 :]:
            _WIDENING_RULES.setdefault(_from, set()).add(_to)

# varchar(n) -> varchar(m) where m > n, varchar(n) -> string, char(n) -> string
_PARAM_VARCHAR_RE = re.compile(r"^varchar\((\d+)\)$")


def is_safe_widening(from_type: str, to_type: str) -> bool:
    """Check if changing from_type to to_type is a safe widening operation."""
    f = from_type.strip().lower()
    t = to_type.strip().lower()

    if f == t:
        return True

    # Primitive widenings (int hierarchy, float hierarchy)
    if f in _WIDENING_RULES and t in _WIDENING_RULES[f]:
        return True

    # varchar(n) -> varchar(m) where m > n
    f_match = _PARAM_VARCHAR_RE.match(f)
    t_match = _PARAM_VARCHAR_RE.match(t)
    if f_match and t_match:
        return int(t_match.group(1)) > int(f_match.group(1))

    # varchar(n) -> string, char(n) -> string
    if t == "string" and (_PARAM_VARCHAR_RE.match(f) or _CHAR_RE.match(f)):
        return True

    return False


# Mapping from Glue/catalog type strings to canonical Athena types
GLUE_TYPE_MAP = {
    "int": "int",
    "integer": "int",
    "tinyint": "tinyint",
    "smallint": "smallint",
    "bigint": "bigint",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "string": "string",
    "date": "date",
    "timestamp": "timestamp",
    "binary": "binary",
}


def normalize_type(type_str: str) -> str:
    """Normalize a type string to its canonical form."""
    t = type_str.strip().lower()
    return GLUE_TYPE_MAP.get(t, t)


# Supported table formats and their SerDe mappings
FORMAT_SERDE = {
    "parquet": {
        "input_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "serde": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    },
    "orc": {
        "input_format": "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
        "serde": "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
    },
    "csv": {
        "input_format": "org.apache.hadoop.mapred.TextInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "serde": "org.apache.hadoop.hive.serde2.OpenCSVSerde",
    },
    "json": {
        "input_format": "org.apache.hadoop.mapred.TextInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "serde": "org.openx.data.jsonserde.JsonSerDe",
    },
    "avro": {
        "input_format": "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        "output_format": "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
        "serde": "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
    },
}

# Reverse lookup: SerDe class -> format name
SERDE_TO_FORMAT = {v["serde"]: fmt for fmt, v in FORMAT_SERDE.items()}

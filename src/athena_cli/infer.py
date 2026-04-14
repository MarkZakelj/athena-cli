"""Infer table schema from S3 location."""

from __future__ import annotations

import re
from io import BytesIO

from athena_cli.athena_client import _get_session


def _s3_client():
    return _get_session().client("s3")


def _parse_s3_url(url: str) -> tuple[str, str]:
    """Parse s3://bucket/prefix into (bucket, prefix)."""
    if not url.startswith("s3://"):
        raise ValueError(f"Invalid S3 URL: {url}. Must start with s3://")
    path = url[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    if not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def scan_s3_location(s3_url: str) -> dict:
    """Scan an S3 location to detect format, partitions, and a sample file.

    Returns dict with keys: format, partitions, sample_key, file_count.
    """
    bucket, prefix = _parse_s3_url(s3_url)
    client = _s3_client()

    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, MaxKeys=1000):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
        # Stop after gathering enough to detect patterns
        if len(keys) >= 1000:
            break

    if not keys:
        raise ValueError(f"No objects found at {s3_url}")

    # Detect format from file extensions, fall back to magic bytes
    fmt = _detect_format(keys)
    if fmt == "unknown":
        fmt = _detect_format_by_magic(bucket, keys)

    # Detect partition structure from directory patterns
    partitions = _detect_partitions(keys, prefix)

    # Find a sample data file (not a directory marker)
    sample_key = _find_sample_file(keys, fmt)

    return {
        "format": fmt,
        "partitions": partitions,
        "sample_key": sample_key,
        "sample_bucket": bucket,
        "file_count": len(keys),
    }


def _detect_format(keys: list[str]) -> str:
    """Detect the data format from file extensions."""
    ext_map = {
        ".parquet": "parquet",
        ".snappy.parquet": "parquet",
        ".orc": "orc",
        ".csv": "csv",
        ".csv.gz": "csv",
        ".json": "json",
        ".json.gz": "json",
        ".jsonl": "json",
        ".jsonl.gz": "json",
        ".avro": "avro",
    }
    for key in keys:
        lower = key.lower()
        # Skip directory markers and metadata files
        if lower.endswith("/") or "/_" in lower or lower.endswith("_$folder$"):
            continue
        for ext, fmt in ext_map.items():
            if lower.endswith(ext):
                return fmt
    return "unknown"


# Magic bytes for format detection
_MAGIC_BYTES = {
    b"PAR1": "parquet",
    b"ORC": "orc",
    b"Obj\x01": "avro",
}


def _detect_format_by_magic(bucket: str, keys: list[str]) -> str:
    """Detect format by reading the first bytes of a sample file."""
    client = _s3_client()
    for key in keys:
        lower = key.lower()
        if lower.endswith("/") or "/_" in lower or lower.endswith("_$folder$"):
            continue
        try:
            response = client.get_object(Bucket=bucket, Key=key, Range="bytes=0-3")
            head = response["Body"].read()
            for magic, fmt in _MAGIC_BYTES.items():
                if head[: len(magic)] == magic:
                    return fmt
        except Exception:
            continue
    return "unknown"


_PARTITION_RE = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)=([^/]+)")


def _detect_partitions(keys: list[str], prefix: str) -> dict[str, str]:
    """Detect partition columns and infer types from S3 key patterns."""
    partition_values: dict[str, list[str]] = {}

    for key in keys:
        relative = key[len(prefix):]
        for match in _PARTITION_RE.finditer(relative):
            name = match.group(1).lower()
            value = match.group(2)
            partition_values.setdefault(name, []).append(value)

    # Infer types from sample values
    partitions: dict[str, str] = {}
    for name, values in partition_values.items():
        partitions[name] = _infer_partition_type(values)

    return partitions


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INT_RE = re.compile(r"^-?\d+$")


def _infer_partition_type(values: list[str]) -> str:
    """Infer the type of a partition column from sample values."""
    sample = values[:100]

    if all(_DATE_RE.match(v) for v in sample):
        return "date"
    if all(_INT_RE.match(v) for v in sample):
        return "int"
    return "string"


def _find_sample_file(keys: list[str], fmt: str) -> str | None:
    """Find a sample data file to read schema from."""
    ext_filter = {
        "parquet": (".parquet", ".snappy.parquet"),
        "orc": (".orc",),
        "csv": (".csv", ".csv.gz"),
        "json": (".json", ".json.gz", ".jsonl", ".jsonl.gz"),
        "avro": (".avro",),
    }
    exts = ext_filter.get(fmt, ())

    # First try matching by extension
    for key in keys:
        lower = key.lower()
        if lower.endswith("/") or "/_" in lower:
            continue
        for ext in exts:
            if lower.endswith(ext):
                return key

    # If format was detected by magic bytes, any non-marker file works
    if fmt != "unknown":
        for key in keys:
            lower = key.lower()
            if lower.endswith("/") or "/_" in lower or lower.endswith("_$folder$"):
                continue
            return key

    return None


def read_parquet_schema(bucket: str, key: str) -> dict[str, str]:
    """Read column names and types from a Parquet file using pyarrow."""
    try:
        import pyarrow.parquet as pq
    except ImportError:
        raise ImportError("pyarrow")

    client = _s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()

    pf = pq.ParquetFile(BytesIO(data))
    schema = pf.schema_arrow

    columns: dict[str, str] = {}
    for field in schema:
        columns[field.name.lower()] = _arrow_to_athena_type(field.type)

    return columns


def read_orc_schema(bucket: str, key: str) -> dict[str, str]:
    """Read column names and types from an ORC file using pyarrow."""
    try:
        import pyarrow.orc as orc
    except ImportError:
        raise ImportError("pyarrow")

    client = _s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    data = response["Body"].read()

    reader = orc.ORCFile(BytesIO(data))
    schema = reader.schema

    columns: dict[str, str] = {}
    for field in schema:
        columns[field.name.lower()] = _arrow_to_athena_type(field.type)

    return columns


def _arrow_to_athena_type(arrow_type) -> str:
    """Convert a PyArrow type to an Athena type string."""
    import pyarrow as pa

    if arrow_type == pa.bool_():
        return "boolean"
    if arrow_type == pa.int8():
        return "tinyint"
    if arrow_type == pa.int16():
        return "smallint"
    if arrow_type in (pa.int32(), pa.uint8(), pa.uint16()):
        return "int"
    if arrow_type in (pa.int64(), pa.uint32(), pa.uint64()):
        return "bigint"
    if arrow_type in (pa.float16(), pa.float32()):
        return "float"
    if arrow_type == pa.float64():
        return "double"
    if arrow_type == pa.utf8() or arrow_type == pa.large_utf8():
        return "string"
    if arrow_type == pa.binary() or arrow_type == pa.large_binary():
        return "binary"
    if arrow_type == pa.date32() or arrow_type == pa.date64():
        return "date"
    if isinstance(arrow_type, pa.TimestampType):
        return "timestamp"
    if isinstance(arrow_type, pa.Decimal128Type):
        return f"decimal({arrow_type.precision},{arrow_type.scale})"
    if isinstance(arrow_type, pa.ListType):
        inner = _arrow_to_athena_type(arrow_type.value_type)
        return f"array<{inner}>"
    if isinstance(arrow_type, pa.MapType):
        kt = _arrow_to_athena_type(arrow_type.key_type)
        vt = _arrow_to_athena_type(arrow_type.item_type)
        return f"map<{kt},{vt}>"
    if isinstance(arrow_type, pa.StructType):
        fields = []
        for i in range(arrow_type.num_fields):
            f = arrow_type.field(i)
            fields.append(f"{f.name.lower()}:{_arrow_to_athena_type(f.type)}")
        return f"struct<{','.join(fields)}>"

    return "string"

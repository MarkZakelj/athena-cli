"""AWS client — Glue for reads, Athena for DDL execution."""

from __future__ import annotations

import time

import boto3

from athena_cli.schema import SchemaConfig
from athena_cli.types import SERDE_TO_FORMAT, normalize_type


def _glue_client(catalog: str = "AwsDataCatalog"):
    return boto3.client("glue")


def _athena_client():
    return boto3.client("athena")


def get_glue_table(database: str, table_name: str, catalog: str = "AwsDataCatalog") -> dict:
    """Fetch table metadata from Glue Data Catalog.

    Returns a dict with keys: columns, partitions, location, format, description, properties.
    """
    client = _glue_client(catalog)
    response = client.get_table(DatabaseName=database, Name=table_name)
    table = response["Table"]
    sd = table.get("StorageDescriptor", {})

    # Columns
    columns = {}
    for col in sd.get("Columns", []):
        columns[col["Name"]] = normalize_type(col["Type"])

    # Partitions
    partitions = {}
    for part in table.get("PartitionKeys", []):
        partitions[part["Name"]] = normalize_type(part["Type"])

    # Format from SerDe
    serde_class = sd.get("SerdeInfo", {}).get("SerializationLibrary", "")
    fmt = SERDE_TO_FORMAT.get(serde_class, "unknown")

    # Location
    location = sd.get("Location", "")

    # Description
    description = table.get("Parameters", {}).get("comment", "") or table.get("Description", "")

    # Properties
    properties = dict(table.get("Parameters", {}))
    # Remove internal/classification keys
    for key in ("classification", "comment", "EXTERNAL", "transient_lastDdlTime"):
        properties.pop(key, None)

    return {
        "columns": columns,
        "partitions": partitions,
        "location": location,
        "format": fmt,
        "description": description,
        "properties": properties,
    }


def list_glue_tables(database: str, catalog: str = "AwsDataCatalog") -> list[str]:
    """List all table names in a Glue database."""
    client = _glue_client(catalog)
    paginator = client.get_paginator("get_tables")
    tables = []
    for page in paginator.paginate(DatabaseName=database):
        for table in page["TableList"]:
            tables.append(table["Name"])
    return sorted(tables)


def execute_ddl(ddl: str, config: SchemaConfig) -> str:
    """Execute a DDL statement via Athena and wait for completion.

    Returns the query execution ID.
    """
    client = _athena_client()

    params: dict = {
        "QueryString": ddl,
        "WorkGroup": config.workgroup,
    }
    if config.output_location:
        params["ResultConfiguration"] = {"OutputLocation": config.output_location}

    response = client.start_query_execution(**params)
    query_id = response["QueryExecutionId"]

    # Poll for completion
    while True:
        result = client.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]

        if state == "SUCCEEDED":
            return query_id
        elif state in ("FAILED", "CANCELLED"):
            reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
            raise RuntimeError(f"Query {state}: {reason}")

        time.sleep(0.5)

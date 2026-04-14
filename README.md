# athena-cli

Manage AWS Athena table schemas from a single YAML file. Define your tables locally, then push, pull, and diff against the live catalog.

## Install

```bash
uv tool install git+https://github.com/MarkZakelj/athena-cli
```

To also infer schemas from Parquet/ORC files on S3:

```bash
uv tool install "git+https://github.com/MarkZakelj/athena-cli[infer]"
```

## Quickstart

```bash
# Create a starter file in the current directory
athena-cli init

# Validate it
athena-cli validate

# See what's different from live Athena
athena-cli status

# Apply changes
athena-cli push
```

Auto-discovery looks for `table_definitions.yaml` in the current directory and up the tree. You can always point to a specific file with `--schema path/to/file.yaml`.

## The schema file

```yaml
_config:
  database: my_database          # default database for all tables
  workgroup: primary
  output_location: s3://my-bucket/query-results/

orders:
  location: s3://my-bucket/orders/
  format: parquet
  description: "Raw orders from the web shop"
  columns:
    order_id: bigint
    customer_id: bigint
    total_amount: decimal(18,2)
    created_at: timestamp
  partitions:
    dt: date                     # yyyy-mm-dd

events:
  database: other_database       # overrides _config.database for this table
  location: s3://my-bucket/events/
  format: orc
  columns:
    event_type: string
    payload: map<string,string>
    tags: array<string>
```

### Per-table fields

| Field | Required | Default | Notes |
|---|---|---|---|
| `columns` | yes | — | |
| `location` | for `push` | — | S3 path, required by Athena |
| `format` | no | `parquet` | `parquet`, `orc`, `csv`, `json`, `avro` |
| `database` | no | `_config.database` | overrides the global default |
| `description` | no | — | stored as table comment |
| `partitions` | no | — | partition columns (excluded from regular columns) |
| `properties` | no | — | arbitrary `TBLPROPERTIES` key-value pairs |

## Commands

### `init`

Creates a starter `table_definitions.yaml`:

```bash
athena-cli init
athena-cli init path/to/tables.yaml
```

### `validate`

Parses and validates the schema file, prints a summary table. No AWS calls.

```bash
athena-cli validate
```

### `status`

Compares local schema against live Glue/Athena tables and shows what's different.

```bash
athena-cli status                  # all tables
athena-cli status orders           # single table
```

### `push`

Applies local schema to Athena — creates new tables or alters existing ones.

```bash
athena-cli push                    # all tables
athena-cli push orders             # single table
athena-cli push --dry-run          # print DDL without executing
athena-cli push --force            # allow destructive changes (drop + recreate)
```

Safe changes (adding columns, widening types like `int` → `bigint`) are applied automatically. Destructive changes (removing columns, changing types incompatibly, altering partitions) require `--force`, which drops and recreates the table after confirmation.

### `pull`

Pulls table definitions from Athena into your local YAML.

```bash
athena-cli pull                    # pull all tables from _config.database
athena-cli pull --database logs    # pull from a specific database
athena-cli pull --table orders     # pull a single table
athena-cli pull --merge            # add new tables, skip existing
```

### `repair`

Runs `MSCK REPAIR TABLE` to discover new partitions and reports how many were found.

```bash
athena-cli repair orders
```

### `drop`

Drops a table in Athena (with confirmation).

```bash
athena-cli drop orders
athena-cli drop orders --yes       # skip confirmation
```

### `infer`

Scans an S3 location to detect format and partitions, reads a sample file to extract column types, and appends the result to your schema file.

```bash
athena-cli infer orders s3://my-bucket/orders/
```

Requires `athena-cli[infer]` for Parquet/ORC column detection. For other formats, a skeleton entry with placeholder columns is added and you fill in the columns manually.

## AWS credentials

Uses the standard boto3 credential chain (env vars, `~/.aws/credentials`, instance profile). To use a named profile:

```bash
athena-cli --profile my-profile status
```

## Supported types

Primitives: `tinyint`, `smallint`, `int`, `bigint`, `float`, `double`, `decimal(p,s)`, `boolean`, `string`, `varchar(n)`, `char(n)`, `date`, `timestamp`, `binary`

Complex: `array<type>`, `map<key,value>`, `struct<field:type,...>`

Types are validated on `validate` and `push` before any AWS call is made.
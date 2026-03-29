# sqllineage

Extract table-level and column-level data lineage from SQL statements.

`sqllineage` parses SQL (via [sqlparser](https://crates.io/crates/sqlparser)) and
produces a structured lineage result showing which tables are read/written and
which source columns each output column derives from.

**Schema-agnostic by default** — works without any catalog metadata. Supply a
`CatalogProvider` to resolve `SELECT *` and disambiguate unqualified columns.

## Features

- Table/column lineage for SELECT, INSERT, CREATE TABLE AS, UPDATE, DELETE, MERGE
- Multi-statement support — each statement analyzed independently
- Statement type classification (Query, Insert, CreateTable, Update, Delete, Merge)
- Identifier normalization — unquoted identifiers lowercased by default
- CTE support including `WITH RECURSIVE`
- UNION / INTERSECT / EXCEPT with positional column correspondence
- Subquery support (derived tables, scalar, correlated, EXISTS, IN)
- Window functions (PARTITION BY, ORDER BY tracked as ancestors)
- Optional `CatalogProvider` for `SELECT *` expansion and column disambiguation
- Rust library + CLI + Python bindings (via PyO3)

## Installation

### Rust

```sh
cargo add sqllineage
```

### Python

```sh
pip install sqllineage
```

### CLI

```sh
cargo install sqllineage
```

## Quick start (Rust)

```rust
use sqllineage::{analyze, AnalyzeOptions};

let results = analyze(
    "INSERT INTO summary SELECT user_id, SUM(score) AS total FROM events GROUP BY user_id",
    AnalyzeOptions::default(),
).unwrap();

let r = &results[0];
assert_eq!(r.statement_type, sqllineage::StatementType::Insert);
assert_eq!(r.tables.inputs[0].table, "events");
assert_eq!(r.tables.output.as_ref().unwrap().table, "summary");
assert_eq!(r.columns.mappings.len(), 2);
```

## Quick start (Python)

```python
from sqllineage import analyze

results = analyze("INSERT INTO summary SELECT user_id, SUM(score) FROM events GROUP BY user_id")
r = results[0]
print(r.statement_type)        # "Insert"
print(r.tables.inputs[0])      # "events"
print(r.tables.output)         # "summary"
print(r.columns[0].target.column, "←", r.columns[0].sources)
```

## CLI

```sh
sqllineage -e "INSERT INTO db1.t1 SELECT a, b FROM db2.t2" --columns -f table
```

```
Statement: Insert
Tables:
  inputs:  db2.t2
  output:  db1.t1

Columns:
  db1.t1.a  ←  db2.t2.a    (direct)
  db1.t1.b  ←  db2.t2.b    (direct)
```

Multi-statement:

```sh
sqllineage -e "INSERT INTO a SELECT x FROM s1; INSERT INTO b SELECT y FROM s2" -f table
```

```
Statement: Insert
Tables:
  inputs:  s1
  output:  a
...
Statement: Insert
Tables:
  inputs:  s2
  output:  b
...
```

### CLI reference

```
sqllineage [OPTIONS] --execute <SQL>

Options:
  -e, --execute <SQL>      SQL string to analyze
  -d, --dialect <DIALECT>  SQL dialect [default: generic]
  -f, --format <FORMAT>    Output format: json, table, dot [default: json]
      --columns            Include column-level lineage
  -h, --help               Print help
```

Supported dialects: `generic`, `ansi`, `postgresql`, `mysql`, `hive`,
`databricks`, `snowflake`, `bigquery`.

## CatalogProvider

Supply a `CatalogProvider` to resolve `SELECT *` and disambiguate unqualified
columns in multi-table queries:

```rust
use sqllineage::{analyze, AnalyzeOptions, CatalogProvider, TableRef};

struct MyCatalog;

impl CatalogProvider for MyCatalog {
    fn list_columns(&self, table: &TableRef) -> Option<Vec<String>> {
        match table.table.as_str() {
            "users" => Some(vec!["id".into(), "name".into(), "email".into()]),
            _ => None,
        }
    }

    fn resolve_column(&self, column: &str, candidates: &[TableRef]) -> Option<TableRef> {
        None
    }
}

let results = analyze(
    "SELECT * FROM users",
    AnalyzeOptions {
        catalog: Some(Box::new(MyCatalog)),
        ..Default::default()
    },
).unwrap();

// With catalog: * is expanded to id, name, email
assert_eq!(results[0].columns.mappings.len(), 3);
```

Python equivalent:

```python
class MyCatalog:
    def list_columns(self, table):
        if table.table == "users":
            return ["id", "name", "email"]
        return None

    def resolve_column(self, column, candidates):
        return None

results = analyze("SELECT * FROM users", catalog=MyCatalog())
assert len(results[0].columns) == 3
```

## License

MIT OR Apache-2.0

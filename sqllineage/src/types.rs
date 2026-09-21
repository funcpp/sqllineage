use serde::Serialize;
use std::fmt;

/// A reference to a table, possibly multi-part (catalog.schema.table).
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
pub struct TableRef {
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: String,
}

impl TableRef {
    /// Create a table reference with just a table name.
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            catalog: None,
            schema: None,
            table: table.into(),
        }
    }

    /// Create a table reference with schema and table name.
    pub fn with_schema(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            catalog: None,
            schema: Some(schema.into()),
            table: table.into(),
        }
    }
}

impl fmt::Display for TableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(c) = &self.catalog {
            write!(f, "{c}.")?;
        }
        if let Some(s) = &self.schema {
            write!(f, "{s}.")?;
        }
        write!(f, "{}", self.table)
    }
}

/// A reference to a column, optionally qualified with a table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct ColumnRef {
    pub table: Option<TableRef>,
    pub column: String,
}

/// The type of SQL statement that was analyzed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum StatementType {
    /// `SELECT` (no output table).
    Query,
    /// `INSERT INTO ... SELECT`.
    Insert,
    /// `CREATE TABLE ... AS SELECT`.
    CreateTable,
    /// `UPDATE ... SET`.
    Update,
    /// `DELETE`.
    Delete,
    /// `MERGE INTO ... USING`.
    Merge,
    /// DDL, DCL, or other statement with no lineage.
    Other,
}

/// The complete result of analyzing a single SQL statement.
#[derive(Debug, Clone, Serialize)]
pub struct AnalyzeResult {
    /// The kind of statement.
    pub statement_type: StatementType,
    /// Table-level lineage (inputs and output).
    pub tables: TableLineage,
    /// Column-level lineage mappings.
    pub columns: ColumnLineage,
    /// Warnings encountered during analysis.
    pub warnings: Vec<Warning>,
}

impl Default for AnalyzeResult {
    fn default() -> Self {
        Self {
            statement_type: StatementType::Other,
            tables: TableLineage::default(),
            columns: ColumnLineage::default(),
            warnings: Vec::new(),
        }
    }
}

/// Table-level lineage: which tables are read and which is written.
#[derive(Debug, Clone, Serialize, Default)]
pub struct TableLineage {
    /// Tables read by the statement (FROM, JOIN, USING, etc.).
    pub inputs: Vec<TableRef>,
    /// Table written by the statement (INSERT, UPDATE, MERGE target), if any.
    pub output: Option<TableRef>,
}

/// Column-level lineage: a collection of column derivation mappings.
#[derive(Debug, Clone, Serialize, Default)]
pub struct ColumnLineage {
    pub mappings: Vec<ColumnMapping>,
}

/// One output column and the source columns it derives from.
#[derive(Debug, Clone, Serialize)]
pub struct ColumnMapping {
    /// The output column.
    pub target: ColumnRef,
    /// Source columns contributing to this output.
    pub sources: Vec<ColumnOrigin>,
    /// The kind of transformation applied.
    pub transform: TransformKind,
}

/// Resolution state of a source column.
#[derive(Debug, Clone, Serialize)]
pub enum ColumnOrigin {
    /// Fully resolved to a specific table and column.
    Concrete { table: TableRef, column: String },
    /// Multiple candidate tables; catalog needed to disambiguate.
    Ambiguous {
        column: String,
        candidates: Vec<TableRef>,
    },
    /// `SELECT *` or `table.*`; catalog needed to expand.
    Wildcard { table: TableRef },
    /// Derived via recursive CTE; base case sources only.
    Recursive { base_sources: Vec<ColumnOrigin> },
}

/// What kind of transformation produced an output column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum TransformKind {
    /// Direct column reference (`SELECT col`).
    Direct,
    /// Expression or window function (`SELECT a + b`, `SUM(x) OVER (...)`).
    Expression,
    /// Aggregate function (`SELECT SUM(col)`).
    Aggregation,
    /// Conditional expression (`SELECT CASE WHEN ...`).
    Conditional,
    /// Reserved for future use.
    Window,
    /// Fallback for unhandled expressions.
    Unknown,
}

/// A warning produced during analysis.
#[derive(Debug, Clone, Serialize)]
pub struct Warning {
    pub kind: WarningKind,
    pub location: Option<SourceLocation>,
}

/// The kind of warning.
#[derive(Debug, Clone, Serialize)]
pub enum WarningKind {
    /// Cycle detected in lineage graph after back-edge removal.
    UnexpectedCycle,
}

/// Source location in the SQL string.
#[derive(Debug, Clone, Serialize)]
pub struct SourceLocation {
    pub line: usize,
    pub column: usize,
}

/// Options for the [`crate::analyze`] function.
pub struct AnalyzeOptions {
    /// SQL dialect for parsing.
    pub dialect: Dialect,
    /// Optional catalog for resolving `SELECT *` and unqualified columns.
    pub catalog: Option<Box<dyn CatalogProvider>>,
    /// Normalize unquoted identifiers to lowercase (default: `true`).
    ///
    /// Quoted identifiers (e.g., `"MyTable"`) always preserve their case.
    pub normalize_case: bool,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            dialect: Dialect::Generic,
            catalog: None,
            normalize_case: true,
        }
    }
}

/// Supported SQL dialects (maps to sqlparser dialects).
///
/// Every dialect that the pinned `sqlparser` release exposes has a variant
/// here. Parsing a statement with the closest dialect matters for lineage:
/// `generic` accepts a superset of most grammars, but it does not apply
/// dialect-specific rules such as `BigQuery`'s backtick quoting or T-SQL's
/// bracket quoting.
///
/// Marked `#[non_exhaustive]`: `sqlparser` gains dialects over time, and
/// adding one here should not be a breaking change for downstream matches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Dialect {
    #[default]
    Generic,
    Ansi,
    PostgreSql,
    MySql,
    Hive,
    Databricks,
    Snowflake,
    BigQuery,
    DuckDb,
    Redshift,
    Spark,
    ClickHouse,
    SQLite,
    /// Microsoft SQL Server (T-SQL).
    MsSql,
    Oracle,
    Teradata,
}

impl Dialect {
    /// Every supported dialect, in the order used for help and error text.
    pub const ALL: &'static [Self] = &[
        Self::Generic,
        Self::Ansi,
        Self::PostgreSql,
        Self::MySql,
        Self::Hive,
        Self::Databricks,
        Self::Snowflake,
        Self::BigQuery,
        Self::DuckDb,
        Self::Redshift,
        Self::Spark,
        Self::ClickHouse,
        Self::SQLite,
        Self::MsSql,
        Self::Oracle,
        Self::Teradata,
    ];

    /// The canonical lowercase name, as accepted by [`Dialect::from_str`] and
    /// printed by [`Display`].
    ///
    /// [`Display`]: std::fmt::Display
    pub const fn name(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::Ansi => "ansi",
            Self::PostgreSql => "postgresql",
            Self::MySql => "mysql",
            Self::Hive => "hive",
            Self::Databricks => "databricks",
            Self::Snowflake => "snowflake",
            Self::BigQuery => "bigquery",
            Self::DuckDb => "duckdb",
            Self::Redshift => "redshift",
            Self::Spark => "spark",
            Self::ClickHouse => "clickhouse",
            Self::SQLite => "sqlite",
            Self::MsSql => "mssql",
            Self::Oracle => "oracle",
            Self::Teradata => "teradata",
        }
    }

    /// A comma-separated list of every canonical name, for help and error text.
    pub fn names() -> String {
        Self::ALL
            .iter()
            .map(|d| d.name())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for Dialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

impl std::str::FromStr for Dialect {
    type Err = UnknownDialect;

    /// Parse a dialect name, case-insensitively.
    ///
    /// Accepts each canonical name from [`Dialect::name`] plus a few common
    /// spellings: `postgres`, `sparksql`, `tsql`, and `sqlserver`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "generic" => Ok(Self::Generic),
            "ansi" => Ok(Self::Ansi),
            "postgresql" | "postgres" => Ok(Self::PostgreSql),
            "mysql" => Ok(Self::MySql),
            "hive" => Ok(Self::Hive),
            "databricks" => Ok(Self::Databricks),
            "snowflake" => Ok(Self::Snowflake),
            "bigquery" => Ok(Self::BigQuery),
            "duckdb" => Ok(Self::DuckDb),
            "redshift" => Ok(Self::Redshift),
            "spark" | "sparksql" => Ok(Self::Spark),
            "clickhouse" => Ok(Self::ClickHouse),
            "sqlite" => Ok(Self::SQLite),
            "mssql" | "tsql" | "sqlserver" => Ok(Self::MsSql),
            "oracle" => Ok(Self::Oracle),
            "teradata" => Ok(Self::Teradata),
            _ => Err(UnknownDialect {
                name: s.to_string(),
            }),
        }
    }
}

/// Error returned when a dialect name is not recognized.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownDialect {
    pub name: String,
}

impl fmt::Display for UnknownDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "unknown dialect '{}'. valid: {}",
            self.name,
            Dialect::names()
        )
    }
}

impl std::error::Error for UnknownDialect {}

/// Error returned when SQL parsing fails.
#[derive(Debug, Clone)]
pub struct ParseError {
    pub message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SQL parse error: {}", self.message)
    }
}

impl std::error::Error for ParseError {}

/// Optional metadata source for resolving `SELECT *` and ambiguous columns.
pub trait CatalogProvider {
    /// Return the column names of a table. Used to expand `SELECT *`.
    fn list_columns(&self, table: &TableRef) -> Option<Vec<String>>;
    /// Given a column name and candidate tables, return the owning table.
    fn resolve_column(&self, column: &str, candidates: &[TableRef]) -> Option<TableRef>;
}

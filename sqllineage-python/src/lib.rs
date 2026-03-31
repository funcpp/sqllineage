use pyo3::prelude::*;

/// A reference to a SQL table.
#[pyclass(frozen, from_py_object, name = "TableRef")]
#[derive(Clone)]
struct PyTableRef {
    #[pyo3(get)]
    catalog: Option<String>,
    #[pyo3(get)]
    schema: Option<String>,
    #[pyo3(get)]
    table: String,
}

#[pymethods]
impl PyTableRef {
    #[new]
    #[pyo3(signature = (table, schema=None, catalog=None))]
    fn new(table: String, schema: Option<String>, catalog: Option<String>) -> Self {
        Self {
            catalog,
            schema,
            table,
        }
    }

    fn __repr__(&self) -> String {
        self.__str__()
    }

    fn __str__(&self) -> String {
        let mut s = String::new();
        if let Some(c) = &self.catalog {
            s.push_str(c);
            s.push('.');
        }
        if let Some(sc) = &self.schema {
            s.push_str(sc);
            s.push('.');
        }
        s.push_str(&self.table);
        s
    }
}

impl From<&sqllineage_core::TableRef> for PyTableRef {
    fn from(t: &sqllineage_core::TableRef) -> Self {
        Self {
            catalog: t.catalog.clone(),
            schema: t.schema.clone(),
            table: t.table.clone(),
        }
    }
}

impl From<&PyTableRef> for sqllineage_core::TableRef {
    fn from(t: &PyTableRef) -> Self {
        Self {
            catalog: t.catalog.clone(),
            schema: t.schema.clone(),
            table: t.table.clone(),
        }
    }
}

/// A reference to a column, optionally qualified with a table.
#[pyclass(frozen, from_py_object, name = "ColumnRef")]
#[derive(Clone)]
struct PyColumnRef {
    #[pyo3(get)]
    table: Option<PyTableRef>,
    #[pyo3(get)]
    column: String,
}

impl From<&sqllineage_core::ColumnRef> for PyColumnRef {
    fn from(c: &sqllineage_core::ColumnRef) -> Self {
        Self {
            table: c.table.as_ref().map(PyTableRef::from),
            column: c.column.clone(),
        }
    }
}

/// Resolution state of a source column.
#[pyclass(frozen, from_py_object, name = "ColumnOrigin")]
#[derive(Clone)]
struct PyColumnOrigin {
    #[pyo3(get)]
    kind: String,
    #[pyo3(get)]
    table: Option<PyTableRef>,
    #[pyo3(get)]
    column: Option<String>,
    #[pyo3(get)]
    candidates: Option<Vec<PyTableRef>>,
    #[pyo3(get)]
    base_sources: Option<Vec<PyColumnOrigin>>,
}

#[pymethods]
impl PyColumnOrigin {
    fn __repr__(&self) -> String {
        match self.kind.as_str() {
            "concrete" => format!(
                "ColumnOrigin.concrete({}.{})",
                self.table.as_ref().map_or("?", |t| &t.table),
                self.column.as_deref().unwrap_or("?"),
            ),
            "wildcard" => format!(
                "ColumnOrigin.wildcard({}.*)",
                self.table.as_ref().map_or("?", |t| &t.table),
            ),
            "ambiguous" => format!(
                "ColumnOrigin.ambiguous({})",
                self.column.as_deref().unwrap_or("?"),
            ),
            "recursive" => "ColumnOrigin.recursive(...)".to_string(),
            other => format!("ColumnOrigin.{other}(...)"),
        }
    }
}

fn convert_origin(o: &sqllineage_core::ColumnOrigin) -> PyColumnOrigin {
    match o {
        sqllineage_core::ColumnOrigin::Concrete { table, column } => PyColumnOrigin {
            kind: "concrete".into(),
            table: Some(PyTableRef::from(table)),
            column: Some(column.clone()),
            candidates: None,
            base_sources: None,
        },
        sqllineage_core::ColumnOrigin::Ambiguous { column, candidates } => PyColumnOrigin {
            kind: "ambiguous".into(),
            table: None,
            column: Some(column.clone()),
            candidates: Some(candidates.iter().map(PyTableRef::from).collect()),
            base_sources: None,
        },
        sqllineage_core::ColumnOrigin::Wildcard { table } => PyColumnOrigin {
            kind: "wildcard".into(),
            table: Some(PyTableRef::from(table)),
            column: None,
            candidates: None,
            base_sources: None,
        },
        sqllineage_core::ColumnOrigin::Recursive { base_sources } => PyColumnOrigin {
            kind: "recursive".into(),
            table: None,
            column: None,
            candidates: None,
            base_sources: Some(base_sources.iter().map(convert_origin).collect()),
        },
    }
}

/// One output column and its source columns.
#[pyclass(frozen, from_py_object, name = "ColumnMapping")]
#[derive(Clone)]
struct PyColumnMapping {
    #[pyo3(get)]
    target: PyColumnRef,
    #[pyo3(get)]
    sources: Vec<PyColumnOrigin>,
    #[pyo3(get)]
    transform: String,
}

#[pymethods]
impl PyColumnMapping {
    fn __repr__(&self) -> String {
        let tgt = &self.target.column;
        let srcs: Vec<String> = self.sources.iter().map(|s| s.__repr__()).collect();
        format!(
            "ColumnMapping({tgt} <- [{}], {})",
            srcs.join(", "),
            self.transform
        )
    }
}

fn convert_transform(t: &sqllineage_core::TransformKind) -> &'static str {
    match t {
        sqllineage_core::TransformKind::Direct => "direct",
        sqllineage_core::TransformKind::Expression => "expression",
        sqllineage_core::TransformKind::Aggregation => "aggregation",
        sqllineage_core::TransformKind::Conditional => "conditional",
        sqllineage_core::TransformKind::Window => "window",
        sqllineage_core::TransformKind::Unknown => "unknown",
    }
}

/// Table-level lineage result.
#[pyclass(frozen, from_py_object, name = "TableLineage")]
#[derive(Clone)]
struct PyTableLineage {
    #[pyo3(get)]
    inputs: Vec<PyTableRef>,
    #[pyo3(get)]
    output: Option<PyTableRef>,
}

/// The complete lineage analysis result.
#[pyclass(frozen, from_py_object, name = "LineageResult")]
#[derive(Clone)]
struct PyLineageResult {
    #[pyo3(get)]
    statement_type: String,
    #[pyo3(get)]
    tables: PyTableLineage,
    #[pyo3(get)]
    columns: Vec<PyColumnMapping>,
}

#[pymethods]
impl PyLineageResult {
    fn __repr__(&self) -> String {
        format!(
            "LineageResult(type={}, inputs={}, output={}, columns={})",
            self.statement_type,
            self.tables.inputs.len(),
            self.tables
                .output
                .as_ref()
                .map_or("None".into(), |t| t.__str__()),
            self.columns.len(),
        )
    }
}

/// Bridge: wraps a Python object implementing the catalog protocol into
/// the Rust `CatalogProvider` trait.
struct PyCatalog {
    obj: Py<PyAny>,
}

impl sqllineage_core::CatalogProvider for PyCatalog {
    fn list_columns(&self, table: &sqllineage_core::TableRef) -> Option<Vec<String>> {
        Python::attach(|py| {
            let py_table = PyTableRef::from(table);
            self.obj
                .call_method1(py, "list_columns", (py_table,))
                .ok()?
                .extract::<Option<Vec<String>>>(py)
                .ok()?
        })
    }

    fn resolve_column(
        &self,
        column: &str,
        candidates: &[sqllineage_core::TableRef],
    ) -> Option<sqllineage_core::TableRef> {
        Python::attach(|py| {
            let py_candidates: Vec<PyTableRef> = candidates.iter().map(PyTableRef::from).collect();
            let result = self
                .obj
                .call_method1(py, "resolve_column", (column, py_candidates))
                .ok()?;
            let py_ref: Option<PyTableRef> = result.extract(py).ok()?;
            py_ref.as_ref().map(sqllineage_core::TableRef::from)
        })
    }
}

/// Analyze a SQL string and extract lineage for each statement.
///
/// Args:
///     sql: One or more SQL statements (separated by `;`).
///     dialect: SQL dialect name (default: "generic").
///     catalog: Optional object with `list_columns(table) -> list[str] | None`
///              and `resolve_column(column, candidates) -> TableRef | None`.
///     normalize_case: Lowercase unquoted identifiers (default: True).
///
/// Returns:
///     List of `LineageResult`, one per statement.
#[pyfunction]
#[pyo3(signature = (sql, dialect="generic", catalog=None, normalize_case=true))]
fn analyze(
    sql: &str,
    dialect: &str,
    catalog: Option<Py<PyAny>>,
    normalize_case: bool,
) -> PyResult<Vec<PyLineageResult>> {
    let d = match dialect.to_lowercase().as_str() {
        "generic" => sqllineage_core::Dialect::Generic,
        "ansi" => sqllineage_core::Dialect::Ansi,
        "postgresql" | "postgres" => sqllineage_core::Dialect::PostgreSql,
        "mysql" => sqllineage_core::Dialect::MySql,
        "hive" => sqllineage_core::Dialect::Hive,
        "databricks" => sqllineage_core::Dialect::Databricks,
        "snowflake" => sqllineage_core::Dialect::Snowflake,
        "bigquery" => sqllineage_core::Dialect::BigQuery,
        other => {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "unknown dialect: '{other}'"
            )));
        }
    };

    let catalog_box: Option<Box<dyn sqllineage_core::CatalogProvider>> =
        catalog.map(|obj| Box::new(PyCatalog { obj }) as Box<dyn sqllineage_core::CatalogProvider>);

    let results = sqllineage_core::analyze(
        sql,
        sqllineage_core::AnalyzeOptions {
            dialect: d,
            catalog: catalog_box,
            normalize_case,
        },
    )
    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.message))?;

    Ok(results
        .iter()
        .map(|result| PyLineageResult {
            statement_type: format!("{:?}", result.statement_type),
            tables: PyTableLineage {
                inputs: result.tables.inputs.iter().map(PyTableRef::from).collect(),
                output: result.tables.output.as_ref().map(PyTableRef::from),
            },
            columns: result
                .columns
                .mappings
                .iter()
                .map(|m| PyColumnMapping {
                    target: PyColumnRef::from(&m.target),
                    sources: m.sources.iter().map(convert_origin).collect(),
                    transform: convert_transform(&m.transform).into(),
                })
                .collect(),
        })
        .collect())
}

#[pymodule]
mod sqllineage {
    #[pymodule_export]
    use super::PyColumnMapping;
    #[pymodule_export]
    use super::PyColumnOrigin;
    #[pymodule_export]
    use super::PyColumnRef;
    #[pymodule_export]
    use super::PyLineageResult;
    #[pymodule_export]
    use super::PyTableLineage;
    #[pymodule_export]
    use super::PyTableRef;
    #[pymodule_export]
    use super::analyze;
}

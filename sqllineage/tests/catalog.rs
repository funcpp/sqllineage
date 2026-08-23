mod common;

use common::find_mapping;
use sqllineage::{AnalyzeOptions, CatalogProvider, ColumnOrigin, Dialect, TableRef, analyze};

struct MockCatalog;

impl CatalogProvider for MockCatalog {
    fn list_columns(&self, table: &TableRef) -> Option<Vec<String>> {
        match table.table.as_str() {
            "users" => Some(vec!["id".into(), "name".into(), "email".into()]),
            "orders" => Some(vec!["id".into(), "user_id".into(), "amount".into()]),
            _ => None,
        }
    }

    fn resolve_column(&self, column: &str, candidates: &[TableRef]) -> Option<TableRef> {
        match column {
            "name" | "email" => candidates.iter().find(|t| t.table == "users").cloned(),
            "amount" => candidates.iter().find(|t| t.table == "orders").cloned(),
            _ => None,
        }
    }
}

struct EagerCatalog;

impl CatalogProvider for EagerCatalog {
    fn list_columns(&self, _table: &TableRef) -> Option<Vec<String>> {
        None
    }

    fn resolve_column(&self, _column: &str, _candidates: &[TableRef]) -> Option<TableRef> {
        Some(TableRef::new("fabricated"))
    }
}

struct AliasCatalog;

impl CatalogProvider for AliasCatalog {
    fn list_columns(&self, table: &TableRef) -> Option<Vec<String>> {
        (table.table == "actual_table").then(|| vec!["id".into(), "event".into()])
    }

    fn resolve_column(&self, _column: &str, _candidates: &[TableRef]) -> Option<TableRef> {
        None
    }
}

fn opts_with_catalog() -> AnalyzeOptions {
    AnalyzeOptions {
        catalog: Some(Box::new(MockCatalog)),
        ..AnalyzeOptions::default()
    }
}

fn concrete_sources(mapping: &sqllineage::ColumnMapping) -> Vec<(String, String)> {
    let mut v: Vec<(String, String)> = mapping
        .sources
        .iter()
        .map(|s| match s {
            ColumnOrigin::Concrete { table, column } => (table.table.clone(), column.clone()),
            other => panic!("expected Concrete, got {other:?}"),
        })
        .collect();
    v.sort();
    v
}

#[test]
fn select_star_with_catalog_expands() {
    let result = analyze("SELECT * FROM users", opts_with_catalog())
        .expect("parse")
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(result.columns.mappings.len(), 3);

    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "id")),
        vec![("users".into(), "id".into())]
    );
    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "name")),
        vec![("users".into(), "name".into())]
    );
    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "email")),
        vec![("users".into(), "email".into())]
    );
}

fn assert_qualified_alias_star_expands(sql: &str) {
    let result = analyze(
        sql,
        AnalyzeOptions {
            catalog: Some(Box::new(AliasCatalog)),
            dialect: Dialect::Generic,
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 2);
    assert!(result.columns.mappings.iter().all(|mapping| {
        mapping
            .sources
            .iter()
            .all(|source| matches!(source, ColumnOrigin::Concrete { .. }))
    }));
    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "id")),
        vec![("actual_table".into(), "id".into())]
    );
    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "event")),
        vec![("actual_table".into(), "event".into())]
    );
}

#[test]
fn qualified_alias_star_uses_catalog_table_binding() {
    for sql in [
        "SELECT a.* FROM actual_table AS a",
        "WITH x AS (SELECT a.* FROM actual_table AS a) SELECT * FROM x",
    ] {
        assert_qualified_alias_star_expands(sql);
    }
}

#[test]
fn qualified_alias_star_without_catalog_keeps_actual_table_wildcard() {
    let result = analyze(
        "SELECT a.* FROM actual_table AS a",
        AnalyzeOptions {
            dialect: Dialect::Generic,
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 1);
    match &result.columns.mappings[0].sources[0] {
        ColumnOrigin::Wildcard { table } => assert_eq!(table.table, "actual_table"),
        other => panic!("expected Wildcard, got {other:?}"),
    }
}

#[test]
fn select_star_without_catalog_preserved() {
    let result = analyze("SELECT * FROM users", AnalyzeOptions::default())
        .expect("parse")
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(result.columns.mappings.len(), 1);
    match &result.columns.mappings[0].sources[0] {
        ColumnOrigin::Wildcard { table } => assert_eq!(table.table, "users"),
        other => panic!("expected Wildcard, got {other:?}"),
    }
}

#[test]
fn ambiguous_column_resolved_by_catalog() {
    let sql = "SELECT name FROM users JOIN orders ON users.id = orders.user_id";
    let result = analyze(sql, opts_with_catalog())
        .expect("parse")
        .into_iter()
        .next()
        .unwrap();
    let m = find_mapping(&result.columns.mappings, "name");
    assert_eq!(concrete_sources(m), vec![("users".into(), "name".into())]);
}

#[test]
fn ambiguous_column_without_catalog() {
    let sql = "SELECT name FROM users JOIN orders ON users.id = orders.user_id";
    let result = analyze(sql, AnalyzeOptions::default())
        .expect("parse")
        .into_iter()
        .next()
        .unwrap();
    let m = find_mapping(&result.columns.mappings, "name");
    match &m.sources[0] {
        ColumnOrigin::Ambiguous { column, candidates } => {
            assert_eq!(column, "name");
            assert!(candidates.len() >= 2);
        }
        other => panic!("expected Ambiguous, got {other:?}"),
    }
}

#[test]
fn catalog_does_not_fabricate_unresolved_column_owner() {
    let result = analyze(
        "SELECT missing",
        AnalyzeOptions {
            catalog: Some(Box::new(EagerCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();
    let m = find_mapping(&result.columns.mappings, "missing");
    match &m.sources[0] {
        ColumnOrigin::Ambiguous { column, candidates } => {
            assert_eq!(column, "missing");
            assert!(candidates.is_empty());
        }
        other => panic!("expected Ambiguous, got {other:?}"),
    }
}

#[test]
fn catalog_preserves_qualified_columns() {
    let sql =
        "SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id";
    let result = analyze(sql, opts_with_catalog())
        .expect("parse")
        .into_iter()
        .next()
        .unwrap();
    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "name")),
        vec![("users".into(), "name".into())]
    );
    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "amount")),
        vec![("orders".into(), "amount".into())]
    );
}

struct SetOperationCatalog;

impl CatalogProvider for SetOperationCatalog {
    fn list_columns(&self, table: &TableRef) -> Option<Vec<String>> {
        match table.table.as_str() {
            "users" => Some(vec!["id".into(), "name".into(), "email".into()]),
            "other" => Some(vec!["a".into(), "b".into(), "c".into(), "d".into()]),
            "ext_a" => Some(vec!["col_x".into(), "col_y".into()]),
            _ => None,
        }
    }

    fn resolve_column(&self, _column: &str, _candidates: &[TableRef]) -> Option<TableRef> {
        None
    }
}

#[test]
fn set_operation_expands_leading_star_before_positional_merge() {
    let sql = "SELECT * FROM users UNION ALL SELECT a, b, c FROM other";
    let result = analyze(
        sql,
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 3);
    assert_eq!(result.columns.mappings[0].target.column, "id");
    assert_eq!(result.columns.mappings[1].target.column, "name");
    assert_eq!(result.columns.mappings[2].target.column, "email");
    assert_eq!(
        concrete_sources(&result.columns.mappings[0]),
        vec![("other".into(), "a".into()), ("users".into(), "id".into())]
    );
    assert_eq!(
        concrete_sources(&result.columns.mappings[1]),
        vec![
            ("other".into(), "b".into()),
            ("users".into(), "name".into())
        ]
    );
}

#[test]
fn set_operation_preserves_non_leading_star_contribution() {
    let sql = "SELECT id, * FROM users UNION ALL SELECT a, b, c, d FROM other";
    let result = analyze(
        sql,
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 4);
    assert_eq!(
        concrete_sources(&result.columns.mappings[1]),
        vec![("other".into(), "b".into()), ("users".into(), "id".into())]
    );
    assert_eq!(
        concrete_sources(&result.columns.mappings[3]),
        vec![
            ("other".into(), "d".into()),
            ("users".into(), "email".into())
        ]
    );
}

#[test]
fn set_operation_branches_survive_cte_and_derived_boundaries() {
    let sql = "WITH combined AS (SELECT * FROM users UNION ALL SELECT a, b, c FROM other) \
               SELECT * FROM (SELECT * FROM combined) derived";
    let result = analyze(
        sql,
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 3);
    assert_eq!(
        concrete_sources(&result.columns.mappings[1]),
        vec![
            ("other".into(), "b".into()),
            ("users".into(), "name".into())
        ]
    );
}

#[test]
fn non_leading_star_in_right_set_branch_contributes_to_named_output() {
    let sql = "WITH lit AS (SELECT 1 AS col_a), \
               u AS (SELECT col_a FROM lit UNION ALL SELECT * FROM ext_a) \
               SELECT col_a FROM u";
    let result = analyze(
        sql,
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 1);
    assert!(matches!(
        result.columns.mappings[0].sources.as_slice(),
        [ColumnOrigin::Concrete { table, column }, ColumnOrigin::SourceFree { column: marker }]
            if table.table == "ext_a" && column == "col_x" && marker == "col_a"
    ));
}

#[test]
fn named_lookup_through_set_operation_cte_keeps_all_branches() {
    let sql = "WITH combined AS (SELECT * FROM users UNION ALL SELECT a, b, c FROM other) \
               SELECT name FROM combined";
    let result = analyze(
        sql,
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(
        concrete_sources(&result.columns.mappings[0]),
        vec![
            ("other".into(), "b".into()),
            ("users".into(), "name".into())
        ]
    );
}

#[test]
fn named_lookup_through_projection_cte_and_derived_star_chain() {
    let sql = "WITH base AS (SELECT * FROM users), wrapped AS (SELECT * FROM base) \
               SELECT name FROM (SELECT * FROM wrapped) derived";
    let result = analyze(
        sql,
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 1);
    assert_eq!(
        concrete_sources(&result.columns.mappings[0]),
        vec![("users".into(), "name".into())]
    );
}

#[test]
fn named_lookup_through_unknown_projection_star_is_indeterminate() {
    let result = analyze(
        "WITH base AS (SELECT * FROM unknown) SELECT name FROM base",
        AnalyzeOptions::default(),
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 1);
    assert!(matches!(
        result.columns.mappings[0].sources.as_slice(),
        [ColumnOrigin::NamedWildcard { table, column }]
            if table.table == "unknown" && column == "name"
    ));
}

#[test]
fn named_lookup_through_projection_preserves_inner_transform() {
    let result = analyze(
        "WITH aggregated AS (SELECT SUM(amount) AS total FROM orders) \
         SELECT total FROM aggregated",
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 1);
    assert_eq!(
        concrete_sources(&result.columns.mappings[0]),
        vec![("orders".into(), "amount".into())]
    );
    assert_eq!(
        result.columns.mappings[0].transform,
        sqllineage::TransformKind::Aggregation
    );
}

#[test]
fn named_lookup_through_projection_preserves_inner_expression_transform() {
    let result = analyze(
        "WITH transformed AS (SELECT amount + 1 AS adjusted FROM orders) \
         SELECT adjusted FROM transformed",
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("parse")
    .into_iter()
    .next()
    .unwrap();

    assert_eq!(result.columns.mappings.len(), 1);
    assert_eq!(
        concrete_sources(&result.columns.mappings[0]),
        vec![("orders".into(), "amount".into())]
    );
    assert_eq!(
        result.columns.mappings[0].transform,
        sqllineage::TransformKind::Expression
    );
}

#[test]
fn catalog_known_set_arity_mismatch_is_an_analysis_error() {
    let result = analyze(
        "SELECT * FROM users UNION ALL SELECT a, b, c, d FROM other",
        AnalyzeOptions {
            catalog: Some(Box::new(SetOperationCatalog)),
            ..AnalyzeOptions::default()
        },
    );
    let error = match result {
        Ok(_) => panic!("catalog-known arity mismatch should not be truncated"),
        Err(error) => error,
    };
    assert_eq!(
        error.message,
        "set operation arity mismatch: left has 3 columns, right has 4 columns"
    );
}

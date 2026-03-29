mod common;

use common::find_mapping;
use sqllineage::{AnalyzeOptions, CatalogProvider, ColumnOrigin, TableRef, analyze};

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

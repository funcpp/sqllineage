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

/// A star written on an alias names the relation the alias stands for. The
/// alias itself appears in no catalog and in no `tables.inputs`, so naming it
/// would both block expansion and claim a relation that does not exist.
#[test]
fn qualified_alias_star_names_the_aliased_relation() {
    let result = analyze("SELECT u.* FROM users AS u", AnalyzeOptions::default())
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
fn qualified_alias_star_expands_from_catalog() {
    for sql in [
        "SELECT u.* FROM users AS u",
        "WITH x AS (SELECT u.* FROM users AS u) SELECT * FROM x",
    ] {
        let result = analyze(sql, opts_with_catalog())
            .expect("parse")
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(result.columns.mappings.len(), 3, "{sql}");
        assert_eq!(
            concrete_sources(find_mapping(&result.columns.mappings, "email")),
            vec![("users".into(), "email".into())],
            "{sql}"
        );
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

/// Answers `resolve_column` for any name, the way a catalog that looks columns
/// up globally would.
struct ByNameCatalog;

impl CatalogProvider for ByNameCatalog {
    fn list_columns(&self, _table: &TableRef) -> Option<Vec<String>> {
        None
    }

    fn resolve_column(&self, _column: &str, _candidates: &[TableRef]) -> Option<TableRef> {
        Some(TableRef::new("guessed"))
    }
}

/// An unresolved column has no candidate relations, so there is nothing for a
/// catalog to choose between. A catalog that answers by name alone must not be
/// able to turn it into a concrete origin: a column existing somewhere in the
/// catalog is not evidence that its table takes part in this query.
#[test]
fn catalog_cannot_give_an_unresolved_column_an_owner() {
    for sql in [
        "SELECT bare_col",
        "WITH cte AS (SELECT present FROM source) SELECT bare_col FROM cte",
    ] {
        let result = analyze(
            sql,
            AnalyzeOptions {
                catalog: Some(Box::new(ByNameCatalog)),
                ..AnalyzeOptions::default()
            },
        )
        .expect("parse")
        .remove(0);

        let m = find_mapping(&result.columns.mappings, "bare_col");
        assert!(
            matches!(&m.sources[..], [ColumnOrigin::Unresolved { .. }]),
            "{sql}: got {:?}",
            m.sources
        );
    }
}

/// The other side of the same boundary: genuine ambiguity between known tables
/// is still the catalog's to resolve.
#[test]
fn catalog_still_resolves_genuine_ambiguity() {
    let result = analyze(
        "SELECT name FROM users JOIN orders ON users.id = orders.user_id",
        opts_with_catalog(),
    )
    .expect("parse")
    .remove(0);

    let m = find_mapping(&result.columns.mappings, "name");
    assert!(
        matches!(&m.sources[..], [ColumnOrigin::Concrete { table, .. }] if table.table == "users"),
        "got {:?}",
        m.sources
    );
}

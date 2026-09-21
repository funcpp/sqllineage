mod common;

use common::{analyze_one, concrete_sources, find_mapping, table};
use sqllineage::{AnalyzeOptions, CatalogProvider, ColumnOrigin, TableRef, TransformKind, analyze};

#[test]
fn merge_when_matched_update_set() {
    let sql = "\
        MERGE INTO target t \
        USING source s ON t.id = s.id \
        WHEN MATCHED THEN UPDATE SET t.col = s.val";
    let result = analyze_one(sql);
    assert_eq!(result.tables.output, Some(table("target")));
    assert_eq!(result.tables.inputs, vec![table("source")]);

    let m = find_mapping(&result.columns.mappings, "col");
    assert_eq!(concrete_sources(m), vec![("source".into(), "val".into())]);
    assert_eq!(m.transform, TransformKind::Direct);
}

#[test]
fn merge_when_not_matched_insert() {
    let sql = "\
        MERGE INTO target t \
        USING source s ON t.id = s.id \
        WHEN NOT MATCHED THEN INSERT (id, name) VALUES (s.id, s.name)";
    let result = analyze_one(sql);
    assert_eq!(result.tables.output, Some(table("target")));

    let m_id = find_mapping(&result.columns.mappings, "id");
    assert_eq!(concrete_sources(m_id), vec![("source".into(), "id".into())]);

    let m_name = find_mapping(&result.columns.mappings, "name");
    assert_eq!(
        concrete_sources(m_name),
        vec![("source".into(), "name".into())]
    );
}

#[test]
fn merge_both_clauses() {
    let sql = "\
        MERGE INTO target t \
        USING source s ON t.id = s.id \
        WHEN MATCHED THEN UPDATE SET t.val = s.val \
        WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)";
    let result = analyze_one(sql);

    // UPDATE SET produces "val", INSERT VALUES produces "id" and "val"
    let val_mappings: Vec<_> = result
        .columns
        .mappings
        .iter()
        .filter(|m| m.target.column == "val")
        .collect();
    assert!(
        val_mappings.len() >= 2,
        "expected val from both UPDATE SET and INSERT"
    );

    let id_mappings: Vec<_> = result
        .columns
        .mappings
        .iter()
        .filter(|m| m.target.column == "id")
        .collect();
    assert!(!id_mappings.is_empty(), "expected id from INSERT clause");
}

/// `UPDATE SET *` copies every source column; without a catalog the shape stays
/// an unexpanded star rather than a guess at the column list.
#[test]
fn merge_update_set_wildcard_is_an_unexpanded_star() {
    let sql = "\
        MERGE INTO target t \
        USING source s ON t.id = s.id \
        WHEN MATCHED THEN UPDATE SET *";
    let result = analyze_one(sql);
    assert_eq!(result.tables.output, Some(table("target")));
    assert_eq!(result.tables.inputs, vec![table("source")]);

    let m = find_mapping(&result.columns.mappings, "*");
    assert!(
        matches!(&m.sources[..], [ColumnOrigin::Wildcard { table }] if table.table == "source"),
        "expected a wildcard on source, got {:?}",
        m.sources
    );
}

struct SourceCatalog;

impl CatalogProvider for SourceCatalog {
    fn list_columns(&self, table: &TableRef) -> Option<Vec<String>> {
        (table.table == "source").then(|| vec!["id".into(), "val".into()])
    }

    fn resolve_column(&self, _column: &str, _candidates: &[TableRef]) -> Option<TableRef> {
        None
    }
}

/// `INSERT *` resolves to the source's real columns once a catalog can name them.
#[test]
fn merge_insert_wildcard_expands_from_catalog() {
    let sql = "\
        MERGE INTO target t \
        USING source s ON t.id = s.id \
        WHEN NOT MATCHED THEN INSERT *";
    let result = analyze(
        sql,
        AnalyzeOptions {
            catalog: Some(Box::new(SourceCatalog)),
            ..AnalyzeOptions::default()
        },
    )
    .expect("MERGE should parse")
    .remove(0);

    let m_id = find_mapping(&result.columns.mappings, "id");
    assert_eq!(concrete_sources(m_id), vec![("source".into(), "id".into())]);

    let m_val = find_mapping(&result.columns.mappings, "val");
    assert_eq!(
        concrete_sources(m_val),
        vec![("source".into(), "val".into())]
    );
}

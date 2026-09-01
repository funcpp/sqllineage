mod common;

use common::{analyze_one, concrete_sources, find_mapping, table};
use sqllineage::{ColumnOrigin, TransformKind};

#[test]
fn select_columns() {
    let result = analyze_one("SELECT a, b FROM t");
    assert_eq!(result.columns.mappings.len(), 2);

    let m_a = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m_a), vec![("t".into(), "a".into())]);
    assert_eq!(m_a.transform, TransformKind::Direct);
    assert_eq!(m_a.target.table, None);

    let m_b = find_mapping(&result.columns.mappings, "b");
    assert_eq!(concrete_sources(m_b), vec![("t".into(), "b".into())]);
    assert_eq!(m_b.transform, TransformKind::Direct);
}

#[test]
fn unresolved_column_has_empty_ambiguous_candidates() {
    let result = analyze_one("SELECT missing");
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
fn select_expression() {
    let result = analyze_one("SELECT a + b AS c FROM t");
    assert_eq!(result.columns.mappings.len(), 1);

    let m = find_mapping(&result.columns.mappings, "c");
    assert_eq!(
        concrete_sources(m),
        vec![("t".into(), "a".into()), ("t".into(), "b".into())]
    );
    assert_eq!(m.transform, TransformKind::Expression);
}

#[test]
fn select_qualified_column() {
    let result = analyze_one("SELECT t.a FROM t");
    assert_eq!(result.columns.mappings.len(), 1);

    let m = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
    assert_eq!(m.transform, TransformKind::Direct);
}

#[test]
fn insert_select_columns() {
    let result = analyze_one("INSERT INTO out SELECT a, b FROM t");
    assert_eq!(result.tables.output, Some(table("out")));
    assert_eq!(result.columns.mappings.len(), 2);

    let m_a = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m_a), vec![("t".into(), "a".into())]);
    assert_eq!(m_a.target.table, Some(table("out")));
}

#[test]
fn select_alias() {
    let result = analyze_one("SELECT a AS x FROM t");
    let m = find_mapping(&result.columns.mappings, "x");
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
    assert_eq!(m.transform, TransformKind::Direct);
}

#[test]
fn select_aggregate() {
    let result = analyze_one("SELECT SUM(a) FROM t");
    let m = &result.columns.mappings[0];
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
    assert_eq!(m.transform, TransformKind::Aggregation);
}

#[test]
fn select_count_star_is_aggregation_without_sources() {
    let result = analyze_one("SELECT COUNT(*) AS c FROM t");
    let m = find_mapping(&result.columns.mappings, "c");

    assert!(m.sources.is_empty());
    assert_eq!(m.transform, TransformKind::Aggregation);
}

#[test]
fn select_multiple_tables_qualified() {
    let result = analyze_one("SELECT t1.a, t2.b FROM t1 JOIN t2 ON t1.id = t2.id");
    assert_eq!(result.columns.mappings.len(), 2);

    let m_a = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m_a), vec![("t1".into(), "a".into())]);

    let m_b = find_mapping(&result.columns.mappings, "b");
    assert_eq!(concrete_sources(m_b), vec![("t2".into(), "b".into())]);
}

#[test]
fn duplicate_output_names_preserve_projection_order() {
    let result = analyze_one("SELECT a.id, b.id FROM a JOIN b ON a.id = b.bid");
    let sources: Vec<_> = result
        .columns
        .mappings
        .iter()
        .map(concrete_sources)
        .collect();

    assert_eq!(
        sources,
        vec![
            vec![("a".into(), "id".into())],
            vec![("b".into(), "id".into())]
        ]
    );
}

#[test]
fn three_duplicate_output_names_preserve_projection_order() {
    let result =
        analyze_one("SELECT a.id, b.id, c.id FROM a JOIN b ON a.id = b.bid JOIN c ON a.id = c.cid");
    let sources: Vec<_> = result
        .columns
        .mappings
        .iter()
        .map(concrete_sources)
        .collect();

    assert_eq!(
        sources,
        vec![
            vec![("a".into(), "id".into())],
            vec![("b".into(), "id".into())],
            vec![("c".into(), "id".into())],
        ]
    );
}

#[test]
fn select_case_expression() {
    let result = analyze_one("SELECT CASE WHEN a > 0 THEN b ELSE c END AS d FROM t");
    let m = find_mapping(&result.columns.mappings, "d");
    assert_eq!(m.transform, TransformKind::Conditional);
    assert_eq!(
        concrete_sources(m),
        vec![
            ("t".into(), "a".into()),
            ("t".into(), "b".into()),
            ("t".into(), "c".into()),
        ]
    );
}

#[test]
fn select_cast_passthrough() {
    let result = analyze_one("SELECT CAST(a AS INT) AS a_int FROM t");
    let m = find_mapping(&result.columns.mappings, "a_int");
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
    assert_eq!(m.transform, TransformKind::Direct);
}

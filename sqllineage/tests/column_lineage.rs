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

/// With no relation in scope there is nothing that could own the column, so
/// the origin says so instead of naming a table.
#[test]
fn bare_column_without_any_relation_is_unresolved() {
    let result = analyze_one("SELECT bare_col");
    let m = find_mapping(&result.columns.mappings, "bare_col");
    assert!(
        matches!(&m.sources[..], [ColumnOrigin::Unresolved { column }] if column == "bare_col"),
        "got {:?}",
        m.sources
    );
}

/// A set operation can mix a proven origin with an unresolved one in the same
/// mapping, so completeness is a per-source question.
#[test]
fn proven_and_unresolved_sources_coexist_in_one_mapping() {
    let result = analyze_one("SELECT a FROM t UNION ALL SELECT bare");
    let m = find_mapping(&result.columns.mappings, "a");
    assert!(
        matches!(
            &m.sources[..],
            [
                ColumnOrigin::Concrete { table, .. },
                ColumnOrigin::Unresolved { column },
            ] if table.table == "t" && column == "bare"
        ),
        "got {:?}",
        m.sources
    );
}

/// A source-free projection is classified by what the expression is, not by
/// the fact that it has no ancestors. A literal really is a direct value; a
/// function call or an operator is not.
#[test]
fn source_free_projections_are_classified_by_their_own_kind() {
    for (sql, expected) in [
        ("SELECT 1 AS c FROM t", TransformKind::Direct),
        ("SELECT NULL AS c FROM t", TransformKind::Direct),
        ("SELECT CAST(1 AS INT) AS c FROM t", TransformKind::Direct),
        ("SELECT 1 + 2 AS c FROM t", TransformKind::Expression),
        ("SELECT NOW() AS c FROM t", TransformKind::Expression),
        ("SELECT COUNT(1) AS c FROM t", TransformKind::Aggregation),
        (
            "SELECT CASE WHEN 1 = 1 THEN 2 ELSE 3 END AS c FROM t",
            TransformKind::Conditional,
        ),
    ] {
        let result = analyze_one(sql);
        let m = find_mapping(&result.columns.mappings, "c");
        assert!(m.sources.is_empty(), "{sql}: expected no sources");
        assert_eq!(m.transform, expected, "{sql}");
    }
}

/// A set operation classifies the column from every branch, including one that
/// reached no source and so left no edge behind — the branch's own kind still
/// counts.
#[test]
fn a_set_operation_is_classified_by_every_branch() {
    for (sql, expected) in [
        (
            "SELECT COUNT(*) AS c FROM t UNION ALL SELECT a FROM u",
            TransformKind::Aggregation,
        ),
        (
            "SELECT a AS c FROM t UNION ALL SELECT SUM(b) FROM u",
            TransformKind::Aggregation,
        ),
        (
            "SELECT a AS c FROM t UNION ALL SELECT b + 1 FROM u",
            TransformKind::Expression,
        ),
        (
            "SELECT a AS c FROM t UNION ALL SELECT b FROM u",
            TransformKind::Direct,
        ),
    ] {
        let result = analyze_one(sql);
        let m = find_mapping(&result.columns.mappings, "c");
        assert_eq!(m.transform, expected, "{sql}");
    }
}

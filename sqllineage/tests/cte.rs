mod common;

use common::{analyze_one, concrete_sources, find_mapping, table};
use sqllineage::{ColumnOrigin, TableRef, TransformKind};

#[test]
fn single_cte() {
    let sql = "WITH cte AS (SELECT a FROM t) SELECT a FROM cte";
    let result = analyze_one(sql);
    assert_eq!(result.tables.inputs, vec![table("t")]);
    assert_eq!(result.tables.output, None);
    assert_eq!(result.columns.mappings.len(), 1);
    let m = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
    assert_eq!(m.transform, TransformKind::Direct);
}

#[test]
fn cte_chain() {
    let sql = "WITH a AS (SELECT x FROM t), b AS (SELECT x FROM a) SELECT x FROM b";
    let result = analyze_one(sql);
    assert_eq!(result.tables.inputs, vec![table("t")]);
    assert_eq!(result.columns.mappings.len(), 1);
    let m = find_mapping(&result.columns.mappings, "x");
    assert_eq!(concrete_sources(m), vec![("t".into(), "x".into())]);
}

#[test]
fn cte_multiple_refs() {
    let sql = "WITH cte AS (SELECT a FROM t) SELECT c1.a AS a1, c2.a AS a2 FROM cte c1, cte c2";
    let result = analyze_one(sql);
    assert_eq!(result.tables.inputs, vec![table("t")]);
    assert_eq!(result.columns.mappings.len(), 2);
    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "a1")),
        vec![("t".into(), "a".into())]
    );
    assert_eq!(
        concrete_sources(find_mapping(&result.columns.mappings, "a2")),
        vec![("t".into(), "a".into())]
    );
}

#[test]
fn cte_shadowing() {
    let sql = "\
        WITH x AS (SELECT a FROM t1) \
        SELECT b FROM (WITH x AS (SELECT b FROM t2) SELECT b FROM x) sub";
    let result = analyze_one(sql);
    let mut inputs = result.tables.inputs.clone();
    inputs.sort();
    assert_eq!(inputs, vec![table("t1"), table("t2")]);
    let m = find_mapping(&result.columns.mappings, "b");
    assert_eq!(concrete_sources(m), vec![("t2".into(), "b".into())]);
}

#[test]
fn recursive_cte_base_case() {
    let sql = "\
        WITH RECURSIVE cte AS (\
            SELECT a FROM t \
            UNION ALL \
            SELECT a FROM cte\
        ) SELECT a FROM cte";
    let result = analyze_one(sql);
    assert_eq!(result.tables.inputs, vec![table("t")]);
    assert_eq!(result.columns.mappings.len(), 1);

    let m = &result.columns.mappings[0];
    assert_eq!(m.target.column, "a");
    match &m.sources[0] {
        ColumnOrigin::Recursive { base_sources } => {
            assert!(!base_sources.is_empty());
            match &base_sources[0] {
                ColumnOrigin::Concrete { table, column } => {
                    assert_eq!(table.table, "t");
                    assert_eq!(column, "a");
                }
                other => panic!("expected Concrete in base_sources, got: {other:?}"),
            }
        }
        other => panic!("expected Recursive origin, got: {other:?}"),
    }
}

#[test]
fn recursive_cte_no_cycle_warning() {
    let sql = "\
        WITH RECURSIVE cte AS (\
            SELECT 1 AS n \
            UNION ALL \
            SELECT n + 1 FROM cte WHERE n < 10\
        ) SELECT n FROM cte";
    let result = analyze_one(sql);
    assert!(
        !result
            .warnings
            .iter()
            .any(|w| matches!(w.kind, sqllineage::WarningKind::UnexpectedCycle)),
        "topological sort should succeed after back-edge removal"
    );
}

#[test]
fn derived_table() {
    let sql = "SELECT x FROM (SELECT a AS x FROM t) sub";
    let result = analyze_one(sql);
    assert_eq!(result.tables.inputs, vec![table("t")]);
    let m = find_mapping(&result.columns.mappings, "x");
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
}

#[test]
fn scalar_subquery_table_input() {
    let sql = "SELECT a, (SELECT MAX(b) FROM t2) AS m FROM t1";
    let result = analyze_one(sql);
    let mut inputs = result.tables.inputs.clone();
    inputs.sort();
    assert_eq!(inputs, vec![table("t1"), table("t2")]);
    let m_a = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m_a), vec![("t1".into(), "a".into())]);
}

#[test]
fn correlated_subquery_table_input() {
    let sql = "SELECT a, (SELECT MAX(b) FROM t2 WHERE t2.id = t1.id) AS mb FROM t1";
    let result = analyze_one(sql);
    let mut inputs = result.tables.inputs.clone();
    inputs.sort();
    assert_eq!(inputs, vec![table("t1"), table("t2")]);
}

#[test]
fn union_all_columns() {
    let sql = "SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t2";
    let result = analyze_one(sql);
    let mut inputs = result.tables.inputs.clone();
    inputs.sort();
    assert_eq!(inputs, vec![table("t1"), table("t2")]);
    assert_eq!(result.columns.mappings.len(), 2);

    let m_a = find_mapping(&result.columns.mappings, "a");
    assert_eq!(
        concrete_sources(m_a),
        vec![("t1".into(), "a".into()), ("t2".into(), "c".into())]
    );
    let m_b = find_mapping(&result.columns.mappings, "b");
    assert_eq!(
        concrete_sources(m_b),
        vec![("t1".into(), "b".into()), ("t2".into(), "d".into())]
    );
}

#[test]
fn union_inside_cte() {
    let sql = "\
        WITH cte AS (SELECT a FROM t1 UNION ALL SELECT b FROM t2) \
        SELECT a FROM cte";
    let result = analyze_one(sql);
    let mut inputs = result.tables.inputs.clone();
    inputs.sort();
    assert_eq!(inputs, vec![table("t1"), table("t2")]);
    let m = find_mapping(&result.columns.mappings, "a");
    assert_eq!(
        concrete_sources(m),
        vec![("t1".into(), "a".into()), ("t2".into(), "b".into())]
    );
}

#[test]
fn select_star_from_derived_table() {
    let sql = "\
        SELECT * FROM (\
            SELECT base_date, money_code, action_type_code \
            FROM core.cos_dw.some_table\
        )";
    let result = analyze_one(sql);
    assert_eq!(
        result.tables.inputs,
        vec![TableRef {
            catalog: Some("core".into()),
            schema: Some("cos_dw".into()),
            table: "some_table".into(),
        }]
    );
    assert_eq!(result.columns.mappings.len(), 3);
    let m = find_mapping(&result.columns.mappings, "base_date");
    assert_eq!(
        concrete_sources(m),
        vec![("some_table".into(), "base_date".into())]
    );
    assert_eq!(m.transform, TransformKind::Direct);

    let m = find_mapping(&result.columns.mappings, "money_code");
    assert_eq!(
        concrete_sources(m),
        vec![("some_table".into(), "money_code".into())]
    );

    let m = find_mapping(&result.columns.mappings, "action_type_code");
    assert_eq!(
        concrete_sources(m),
        vec![("some_table".into(), "action_type_code".into())]
    );
}

#[test]
fn select_star_from_cte() {
    let sql = "WITH cte AS (SELECT a, b FROM t) SELECT * FROM cte";
    let result = analyze_one(sql);
    assert_eq!(result.tables.inputs, vec![table("t")]);
    assert_eq!(result.columns.mappings.len(), 2);
    let m = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
    let m = find_mapping(&result.columns.mappings, "b");
    assert_eq!(concrete_sources(m), vec![("t".into(), "b".into())]);
}

#[test]
fn cte_join_unqualified_column() {
    let sql = "\
        WITH cte AS (SELECT a FROM t1) \
        SELECT a FROM cte JOIN t2 ON cte.id = t2.id";
    let result = analyze_one(sql);
    let m = find_mapping(&result.columns.mappings, "a");
    // "a" should come from CTE (which traces to t1.a), not from t2
    assert_eq!(concrete_sources(m), vec![("t1".into(), "a".into())]);
}

#[test]
fn qualified_wildcard_on_cte() {
    let sql = "WITH cte AS (SELECT a, b FROM t) SELECT cte.* FROM cte";
    let result = analyze_one(sql);
    assert_eq!(result.columns.mappings.len(), 2);
    let m = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
    let m = find_mapping(&result.columns.mappings, "b");
    assert_eq!(concrete_sources(m), vec![("t".into(), "b".into())]);
}

#[test]
fn nested_select_star_derived() {
    let sql = "SELECT * FROM (SELECT * FROM (SELECT a FROM t) inner_q) outer_q";
    let result = analyze_one(sql);
    assert_eq!(result.columns.mappings.len(), 1);
    let m = find_mapping(&result.columns.mappings, "a");
    assert_eq!(concrete_sources(m), vec![("t".into(), "a".into())]);
}

#[test]
fn cte_chain_select_star() {
    let sql = "WITH a AS (SELECT x FROM t), b AS (SELECT * FROM a) SELECT * FROM b";
    let result = analyze_one(sql);
    assert_eq!(result.columns.mappings.len(), 1);
    let m = find_mapping(&result.columns.mappings, "x");
    assert_eq!(concrete_sources(m), vec![("t".into(), "x".into())]);
}

fn only_source(sql: &str, column: &str) -> ColumnOrigin {
    let result = analyze_one(sql);
    let m = find_mapping(&result.columns.mappings, column);
    assert_eq!(
        m.sources.len(),
        1,
        "expected one source, got {:?}",
        m.sources
    );
    m.sources[0].clone()
}

/// A CTE that has no output column of this name cannot be said to provide it.
/// Three shapes reach the same state, and all three used to claim a fabricated
/// `?cte?` table.
#[test]
fn column_absent_from_a_cte_is_unresolved() {
    // The CTE genuinely has no such column.
    let missing = only_source(
        "WITH cte AS (SELECT present FROM source) SELECT missing FROM cte",
        "missing",
    );
    assert!(
        matches!(&missing, ColumnOrigin::Unresolved { column } if column == "missing"),
        "got {missing:?}"
    );

    // Every binding is a CTE and none of them has the column, so no physical
    // relation is left to attribute it to.
    let two_ctes = only_source(
        "WITH a AS (SELECT p FROM s1), b AS (SELECT q FROM s2) \
         SELECT missing FROM a JOIN b ON a.p = b.q",
        "missing",
    );
    assert!(
        matches!(&two_ctes, ColumnOrigin::Unresolved { column } if column == "missing"),
        "got {two_ctes:?}"
    );
}

/// A column hidden behind an unexpanded `SELECT *` reads as unresolved too.
///
/// Here `p` very likely exists — the star just was not expanded. Reporting it
/// as unresolved is honest but coarse; naming the relation it came from needs
/// an origin that can carry one. Pinned so that change is visible when it lands.
#[test]
fn column_behind_an_unexpanded_star_is_unresolved() {
    for sql in [
        "WITH cte AS (SELECT * FROM t) SELECT p FROM cte",
        "SELECT p FROM (SELECT * FROM t) d",
    ] {
        let origin = only_source(sql, "p");
        assert!(
            matches!(&origin, ColumnOrigin::Unresolved { column } if column == "p"),
            "{sql}: got {origin:?}"
        );
    }
}

/// Known limitation: only the last hop classifies the column.
///
/// `collect_output_sources` records the kind of the *immediate* incoming edge
/// and drops every kind met deeper in the walk, so an aggregate below a CTE or
/// a derived table reads as `Direct`. The column's own kind does not rescue
/// this — it describes the outer projection, which really is a plain
/// reference; the aggregation is a hop further down and never travels.
///
/// Pinned so the change is visible when that lands.
#[test]
fn only_the_last_hop_classifies_the_column() {
    for sql in [
        "WITH x AS (SELECT COUNT(*) AS c FROM t) SELECT c FROM x",
        "SELECT c FROM (SELECT COUNT(*) AS c FROM t) d",
        // Not a question of missing sources: `t.x` survives the hop, the
        // aggregation does not.
        "SELECT c FROM (SELECT SUM(x) AS c FROM t) d",
    ] {
        let result = analyze_one(sql);
        let m = find_mapping(&result.columns.mappings, "c");
        assert_eq!(m.transform, TransformKind::Direct, "{sql}");
    }

    // Expanding a star takes no second hop — it classifies each inner column
    // from that column's own node — so the same query disagrees with itself
    // depending on how the column is selected.
    let result = analyze_one("SELECT * FROM (SELECT COUNT(*) AS c FROM t) d");
    let m = find_mapping(&result.columns.mappings, "c");
    assert_eq!(m.transform, TransformKind::Aggregation);
}

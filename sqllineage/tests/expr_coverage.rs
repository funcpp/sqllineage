mod common;

use common::{analyze_one, concrete_sources, find_mapping};
use sqllineage::TransformKind;

#[test]
fn extract_year() {
    let result = analyze_one("SELECT EXTRACT(YEAR FROM hire_date) AS yr FROM t");
    let m = find_mapping(&result.columns.mappings, "yr");
    assert_eq!(concrete_sources(m), vec![("t".into(), "hire_date".into())]);
    assert_eq!(m.transform, TransformKind::Expression);
}

#[test]
fn ceil_expr() {
    let result = analyze_one("SELECT CEIL(price) AS p FROM t");
    let m = find_mapping(&result.columns.mappings, "p");
    assert_eq!(concrete_sources(m), vec![("t".into(), "price".into())]);
}

#[test]
fn floor_expr() {
    let result = analyze_one("SELECT FLOOR(price) AS p FROM t");
    let m = find_mapping(&result.columns.mappings, "p");
    assert_eq!(concrete_sources(m), vec![("t".into(), "price".into())]);
}

#[test]
fn substring_expr() {
    let result = analyze_one("SELECT SUBSTRING(name FROM 1 FOR 3) AS sub FROM t");
    let m = find_mapping(&result.columns.mappings, "sub");
    assert_eq!(concrete_sources(m), vec![("t".into(), "name".into())]);
}

#[test]
fn trim_expr() {
    let result = analyze_one("SELECT TRIM(name) AS n FROM t");
    let m = find_mapping(&result.columns.mappings, "n");
    assert_eq!(concrete_sources(m), vec![("t".into(), "name".into())]);
}

#[test]
fn position_expr() {
    let result = analyze_one("SELECT POSITION('x' IN name) AS pos FROM t");
    let m = find_mapping(&result.columns.mappings, "pos");
    assert_eq!(concrete_sources(m), vec![("t".into(), "name".into())]);
}

#[test]
fn overlay_expr() {
    let result = analyze_one("SELECT OVERLAY(name PLACING 'X' FROM 1 FOR 1) AS o FROM t");
    let m = find_mapping(&result.columns.mappings, "o");
    assert_eq!(concrete_sources(m), vec![("t".into(), "name".into())]);
}

#[test]
fn at_time_zone() {
    let result = analyze_one("SELECT ts AT TIME ZONE 'UTC' AS utc FROM t");
    let m = find_mapping(&result.columns.mappings, "utc");
    assert_eq!(concrete_sources(m), vec![("t".into(), "ts".into())]);
}

#[test]
fn collate_expr() {
    let result = analyze_one("SELECT name COLLATE \"en_US\" AS n FROM t");
    let m = find_mapping(&result.columns.mappings, "n");
    assert_eq!(concrete_sources(m), vec![("t".into(), "name".into())]);
}

#[test]
fn is_true_expr() {
    let result = analyze_one("SELECT active IS TRUE AS flag FROM t");
    let m = find_mapping(&result.columns.mappings, "flag");
    assert_eq!(concrete_sources(m), vec![("t".into(), "active".into())]);
}

#[test]
fn is_distinct_from() {
    let result = analyze_one("SELECT a IS DISTINCT FROM b AS diff FROM t");
    let m = find_mapping(&result.columns.mappings, "diff");
    assert_eq!(
        concrete_sources(m),
        vec![("t".into(), "a".into()), ("t".into(), "b".into())]
    );
}

#[test]
fn like_expr() {
    let result = analyze_one("SELECT name LIKE '%test%' AS matched FROM t");
    let m = find_mapping(&result.columns.mappings, "matched");
    assert_eq!(concrete_sources(m), vec![("t".into(), "name".into())]);
}

#[test]
fn array_expr() {
    let result = analyze_one("SELECT ARRAY[a, b] AS arr FROM t");
    let m = find_mapping(&result.columns.mappings, "arr");
    assert_eq!(
        concrete_sources(m),
        vec![("t".into(), "a".into()), ("t".into(), "b".into())]
    );
}

#[test]
fn json_access() {
    let result = analyze_one("SELECT data->>'key' AS val FROM t");
    let m = find_mapping(&result.columns.mappings, "val");
    assert_eq!(concrete_sources(m), vec![("t".into(), "data".into())]);
}

use sqllineage::{AnalyzeOptions, Dialect, TableRef, analyze};

#[test]
fn every_dialect_parses_a_basic_query() {
    for &dialect in Dialect::ALL {
        let results = analyze(
            "SELECT a FROM t",
            AnalyzeOptions {
                dialect,
                ..AnalyzeOptions::default()
            },
        )
        .unwrap_or_else(|e| panic!("{dialect} failed to parse: {e}"));

        assert_eq!(
            results[0].tables.inputs,
            vec![TableRef::new("t")],
            "{dialect}"
        );
    }
}

/// `name` and `from_str` are separate tables; this keeps them agreeing.
#[test]
fn canonical_names_round_trip() {
    for &dialect in Dialect::ALL {
        assert_eq!(dialect.name().parse::<Dialect>(), Ok(dialect));
        assert_eq!(dialect.to_string(), dialect.name());
    }
}

#[test]
fn aliases_and_mixed_case_resolve() {
    assert_eq!("postgres".parse(), Ok(Dialect::PostgreSql));
    assert_eq!("sparksql".parse(), Ok(Dialect::Spark));
    assert_eq!("tsql".parse(), Ok(Dialect::MsSql));
    assert_eq!("sqlserver".parse(), Ok(Dialect::MsSql));
    assert_eq!("DuckDB".parse(), Ok(Dialect::DuckDb));
}

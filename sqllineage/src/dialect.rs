use crate::types::Dialect;
use sqlparser::dialect::{
    self, AnsiDialect, BigQueryDialect, DatabricksDialect, GenericDialect, HiveDialect,
    MySqlDialect, PostgreSqlDialect, SnowflakeDialect,
};

impl Dialect {
    pub fn to_sqlparser_dialect(&self) -> Box<dyn dialect::Dialect> {
        match self {
            Dialect::Generic => Box::new(GenericDialect),
            Dialect::Ansi => Box::new(AnsiDialect {}),
            Dialect::PostgreSql => Box::new(PostgreSqlDialect {}),
            Dialect::MySql => Box::new(MySqlDialect {}),
            Dialect::Hive => Box::new(HiveDialect {}),
            Dialect::Databricks => Box::new(DatabricksDialect),
            Dialect::Snowflake => Box::new(SnowflakeDialect),
            Dialect::BigQuery => Box::new(BigQueryDialect),
        }
    }
}

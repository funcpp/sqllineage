from typing import Optional

class TableRef:
    catalog: Optional[str]
    schema: Optional[str]
    table: str
    def __init__(self, table: str, schema: Optional[str] = None, catalog: Optional[str] = None) -> None: ...
    def __str__(self) -> str: ...
    def __repr__(self) -> str: ...

class ColumnRef:
    table: Optional[TableRef]
    column: str

class ColumnOrigin:
    """Resolution state of a source column.

    Check ``kind`` to determine the variant:
    - ``"concrete"``: ``table`` and ``column`` are set.
    - ``"ambiguous"``: ``column`` and ``candidates`` are set.
    - ``"wildcard"``: ``table`` is set.
    - ``"recursive"``: ``base_sources`` is set.
    """
    kind: str
    table: Optional[TableRef]
    column: Optional[str]
    candidates: Optional[list[TableRef]]
    base_sources: Optional[list["ColumnOrigin"]]

class ColumnMapping:
    target: ColumnRef
    sources: list[ColumnOrigin]
    transform: str

class TableLineage:
    inputs: list[TableRef]
    output: Optional[TableRef]

class LineageResult:
    statement_type: str
    tables: TableLineage
    columns: list[ColumnMapping]
    def __repr__(self) -> str: ...

def analyze(
    sql: str,
    dialect: str = "generic",
    catalog: Optional[object] = None,
    normalize_case: bool = True,
) -> list[LineageResult]:
    """Analyze SQL and extract lineage for each statement.

    Args:
        sql: One or more SQL statements separated by ``;``.
        dialect: SQL dialect (generic, ansi, postgresql, mysql, hive,
                 databricks, snowflake, bigquery).
        catalog: Optional object implementing ``list_columns(table: TableRef)
                 -> list[str] | None`` and ``resolve_column(column: str,
                 candidates: list[TableRef]) -> TableRef | None``.
        normalize_case: Lowercase unquoted identifiers (default True).

    Returns:
        One ``LineageResult`` per statement.

    Raises:
        ValueError: If SQL parsing fails.
    """
    ...

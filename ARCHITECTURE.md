# Architecture

This document describes how sqllineage is structured and how to navigate
the codebase. It is intended for contributors and anyone reading the source.

## Overview

sqllineage takes a SQL string and produces, for each statement, a list of
input/output tables and a mapping from each output column to the source
columns it derives from. The analysis is purely syntactic — it does not
execute SQL or connect to a database. An optional `CatalogProvider` can be
supplied to resolve `SELECT *` and disambiguate unqualified columns.

The work happens in two phases:

1. **Build** (`build/`) — walk the AST produced by `sqlparser` and construct
   a dependency graph (`RawGraph`).
2. **Resolve** (`resolve/`) — traverse the graph, resolve column references
   through scope chains, and produce the final `AnalyzeResult`.

Each statement is processed independently with its own graph and scope tree.

## Build phase

`build/statement.rs` is the entry point. It pattern-matches on the sqlparser
`Statement` enum to identify the output table and delegate to the appropriate
handler. INSERT, CTAS, UPDATE, DELETE, and MERGE each have specific logic for
extracting column-level assignments.

`build/query.rs` handles `Query` — first processing any CTEs (each gets its
own scope with bindings registered in the parent), then the query body. UNION
is handled by giving each side its own scope and merging output columns
positionally. `SetExpr::Update` / `SetExpr::Insert` (for CTE-wrapped DML
like `WITH ... UPDATE`) delegates back to statement dispatch.

`build/select.rs` processes a `SELECT`: FROM first (registering bindings),
then projection (creating Output nodes). When a FROM item is a CTE reference,
the existing CTE binding is reused instead of creating a new Table binding.
Anonymous (alias-less) derived tables are tracked on the scope separately.

`build/expr.rs` recursively walks expressions to collect leaf column
references. Window function PARTITION BY / ORDER BY clauses are included
as ancestors.

### Graph and scope structures

`graph/node.rs` defines four node types: Output (projection result), Ref
(qualified column like `t.col`), Unqualified (bare column name), and Star
(`SELECT *`).

`graph/scope.rs` models SQL name resolution. Each scope holds a map of
bindings (Table, Cte, or DerivedTable) and a list of output columns. Lookup
walks the parent chain with nearest-scope-wins shadowing. CTE body scopes
are separated from CTE registration scopes to prevent false ambiguity.

## Resolve phase

`resolve/mod.rs` iterates the root scope's output columns and resolves each
one. Named columns resolve through scope lookup: Table bindings produce
`Concrete` origins, CTE/DerivedTable bindings follow through `output_columns`
to the base table recursively. Star nodes go through the same chain via
`expand_star()`.

Every resolve path must handle all three binding types (Table, Cte,
DerivedTable) uniformly. This is the most important invariant in the
codebase — past bugs came from paths that only handled `Binding::Table`.

`resolve/topo.rs` validates that the graph is a DAG after removing recursive
CTE back-edges. `resolve/catalog.rs` applies the optional `CatalogProvider`
as a post-processing step.

## Upgrading sqlparser

When sqlparser adds new `Statement` or `Expr` variants, the build layer
may need updating:

- New `Statement` variant → add a match arm in `statement.rs` (or let
  it fall through to `StatementType::Other` if it carries no lineage).
- New `Expr` variant → add a match arm in `expr.rs` (the catch-all
  emits `WarningKind::UnhandledExpression`).
- Renamed/restructured fields → follow compiler errors.

If sqlparser already parses a construct, sqllineage generally handles it
automatically through the existing AST traversal. Changes to sqllineage
itself should only be needed when sqlparser's representation changes.

## Module map

| Path | Responsibility |
|------|---------------|
| `lib.rs` | `pub fn analyze()` — parses SQL, runs build+resolve per statement |
| `types.rs` | All public types: `AnalyzeResult`, `TableRef`, `ColumnMapping`, etc. |
| `dialect.rs` | Maps `Dialect` enum to sqlparser dialect implementations |
| `build/statement.rs` | Statement-level dispatch and column mapping for DML |
| `build/query.rs` | CTE registration, UNION merge, CTE-wrapped DML |
| `build/select.rs` | FROM binding registration, projection processing |
| `build/expr.rs` | Recursive expression traversal for ancestor collection |
| `graph/node.rs` | `RawNode` enum (Output, Ref, Unqualified, Star) |
| `graph/edge.rs` | `RawEdge` with `EdgeKind` (Direct, ViaExpression, etc.) |
| `graph/scope.rs` | `ScopeTree` with bindings, output columns, parent chain |
| `resolve/mod.rs` | Scope-chain resolution, Star expansion, `ColumnMapping` assembly |
| `resolve/topo.rs` | Kahn's algorithm for DAG validation |
| `resolve/catalog.rs` | `CatalogProvider` application (Wildcard/Ambiguous refinement) |

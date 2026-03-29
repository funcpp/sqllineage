mod catalog;
mod topo;

use std::collections::{HashMap, HashSet};

use crate::graph::RawGraph;
use crate::graph::edge::EdgeKind;
use crate::graph::node::{NodeId, RawNode};
use crate::graph::scope::{Binding, ScopeTree};
use crate::types::{
    AnalyzeResult, CatalogProvider, ColumnLineage, ColumnMapping, ColumnOrigin, ColumnRef,
    StatementType, TableRef, TransformKind, Warning, WarningKind,
};

/// Resolve `RawGraph` into `AnalyzeResult`.
pub(crate) fn resolve(
    mut graph: RawGraph,
    catalog: Option<&dyn CatalogProvider>,
    mut warnings: Vec<Warning>,
    statement_type: StatementType,
) -> AnalyzeResult {
    graph.tables.inputs.sort();
    graph.tables.inputs.dedup();

    if graph.nodes.is_empty() {
        return AnalyzeResult {
            statement_type,
            tables: graph.tables,
            columns: ColumnLineage::default(),
            warnings,
        };
    }

    if topo::topological_sort(&graph.nodes, &graph.edges).is_err() {
        warnings.push(Warning {
            kind: WarningKind::UnexpectedCycle,
            location: None,
        });
        return AnalyzeResult {
            statement_type,
            tables: graph.tables,
            columns: ColumnLineage::default(),
            warnings,
        };
    }

    let mut incoming: Vec<Vec<usize>> = vec![vec![]; graph.nodes.len()];
    for (idx, edge) in graph.edges.iter().enumerate() {
        incoming[edge.to].push(idx);
    }

    let mut resolved: Vec<Option<ColumnOrigin>> = vec![None; graph.nodes.len()];

    let root = ScopeTree::root();
    let ordered_cols = graph.scopes.output_columns(root).to_vec();
    let final_ids: HashSet<NodeId> = ordered_cols.iter().map(|c| c.node_id).collect();

    let output_table = graph.tables.output.clone();
    let mut mappings = Vec::new();

    for &node_id in &final_ids {
        match &graph.nodes[node_id] {
            RawNode::Output { name, .. } => {
                let mut visited = HashSet::new();
                let (sources, edge_kinds, has_back) =
                    collect_output_sources(node_id, &graph, &mut resolved, &incoming, &mut visited);
                let transform = derive_transform(&edge_kinds);

                if has_back {
                    mappings.push(ColumnMapping {
                        target: ColumnRef { table: output_table.clone(), column: name.clone() },
                        sources: vec![ColumnOrigin::Recursive { base_sources: sources }],
                        transform,
                    });
                } else {
                    mappings.push(ColumnMapping {
                        target: ColumnRef { table: output_table.clone(), column: name.clone() },
                        sources,
                        transform,
                    });
                }
            }
            RawNode::Star { table, scope } => {
                expand_star(
                    table.as_ref(),
                    *scope,
                    &graph,
                    &mut resolved,
                    &incoming,
                    output_table.as_ref(),
                    &mut mappings,
                    &mut HashSet::new(),
                );
            }
            _ => {}
        }
    }

    // Sort by original output_column order (O(k + m log m))
    let name_order: HashMap<String, usize> = ordered_cols
        .iter()
        .enumerate()
        .filter_map(|(i, c)| match &graph.nodes[c.node_id] {
            RawNode::Output { name, .. } => Some((name.clone(), i)),
            RawNode::Star { .. } => Some(("*".to_string(), i)),
            _ => None,
        })
        .collect();
    mappings.sort_by_key(|m| name_order.get(&m.target.column).copied().unwrap_or(usize::MAX));

    if let Some(cat) = catalog {
        catalog::apply_catalog(&mut mappings, cat);
    }

    AnalyzeResult {
        statement_type,
        tables: graph.tables,
        columns: ColumnLineage { mappings },
        warnings,
    }
}

fn effective_bindings(scope: usize, graph: &RawGraph) -> Vec<(String, Binding)> {
    let immediate = graph.scopes.immediate_bindings(scope);
    if immediate.is_empty() {
        graph.scopes.visible_bindings(scope)
    } else {
        immediate
    }
}

fn wildcard_mapping(output_table: Option<&TableRef>, source_table: TableRef) -> ColumnMapping {
    ColumnMapping {
        target: ColumnRef {
            table: output_table.cloned(),
            column: "*".to_string(),
        },
        sources: vec![ColumnOrigin::Wildcard {
            table: source_table,
        }],
        transform: TransformKind::Direct,
    }
}

/// Expand a Star node (qualified or unqualified) into `ColumnMapping`s.
#[allow(clippy::too_many_arguments)]
fn expand_star(
    table: Option<&TableRef>,
    scope: usize,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    output_table: Option<&TableRef>,
    mappings: &mut Vec<ColumnMapping>,
    visited_scopes: &mut HashSet<usize>,
) {
    if let Some(t) = table {
        let binding = graph.scopes.lookup(scope, &t.table).cloned();
        if let Some(Binding::Cte(s) | Binding::DerivedTable(s)) = binding {
            expand_scope_columns(s, graph, resolved, incoming, output_table, mappings, visited_scopes);
        } else {
            mappings.push(wildcard_mapping(output_table, t.clone()));
        }
    } else {
        for (_, binding) in effective_bindings(scope, graph) {
            match binding {
                Binding::Table(tref) => mappings.push(wildcard_mapping(output_table, tref)),
                Binding::Cte(s) | Binding::DerivedTable(s) => {
                    expand_scope_columns(s, graph, resolved, incoming, output_table, mappings, visited_scopes);
                }
            }
        }
        for &child in graph.scopes.anonymous_derived(scope) {
            expand_scope_columns(child, graph, resolved, incoming, output_table, mappings, visited_scopes);
        }
    }
}

/// Recursively expand a scope's output columns into `ColumnMapping`s.
fn expand_scope_columns(
    scope_id: usize,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    output_table: Option<&TableRef>,
    mappings: &mut Vec<ColumnMapping>,
    visited_scopes: &mut HashSet<usize>,
) {
    if !visited_scopes.insert(scope_id) {
        return;
    }
    for col in graph.scopes.output_columns(scope_id) {
        if let RawNode::Star { table, scope } = &graph.nodes[col.node_id] {
            expand_star(table.as_ref(), *scope, graph, resolved, incoming, output_table, mappings, visited_scopes);
        } else {
            let mut visited = HashSet::new();
            let (sources, edge_kinds, _) =
                collect_output_sources(col.node_id, graph, resolved, incoming, &mut visited);
            let transform = derive_transform(&edge_kinds);
            mappings.push(ColumnMapping {
                target: ColumnRef {
                    table: output_table.cloned(),
                    column: col.name.clone(),
                },
                sources,
                transform,
            });
        }
    }
}

fn collect_output_sources(
    node_id: NodeId,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
) -> (Vec<ColumnOrigin>, Vec<EdgeKind>, bool) {
    if !visited.insert(node_id) {
        return (vec![], vec![], false);
    }

    let mut sources = Vec::new();
    let mut kinds = Vec::new();
    let mut has_back = false;

    for &edge_idx in &incoming[node_id] {
        let edge = &graph.edges[edge_idx];
        if edge.is_recursive_back_edge {
            has_back = true;
            continue;
        }
        let (sub_sources, sub_back) =
            collect_leaf_origins(edge.from, graph, resolved, incoming, visited);
        for _ in &sub_sources {
            kinds.push(edge.kind.clone());
        }
        sources.extend(sub_sources);
        has_back |= sub_back;
    }

    (sources, kinds, has_back)
}

fn collect_leaf_origins(
    node_id: NodeId,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
) -> (Vec<ColumnOrigin>, bool) {
    if let Some((target_output, _)) = find_cte_redirect(node_id, graph) {
        let (sources, _, has_back) =
            collect_output_sources(target_output, graph, resolved, incoming, visited);
        return (sources, has_back);
    }

    if let RawNode::Output { .. } = &graph.nodes[node_id] {
        let (sources, _, has_back) =
            collect_output_sources(node_id, graph, resolved, incoming, visited);
        (sources, has_back)
    } else {
        let origin = resolve_node(node_id, graph, resolved, incoming, visited);
        match origin {
            Some(o) => (vec![o], false),
            None => (vec![], false),
        }
    }
}

fn resolve_node(
    node_id: NodeId,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
) -> Option<ColumnOrigin> {
    if let Some(ref origin) = resolved[node_id] {
        return Some(origin.clone());
    }

    let origin = match &graph.nodes[node_id] {
        RawNode::Ref {
            name,
            qualifier,
            scope,
        } => {
            if let Some(qual) = qualifier {
                let binding = graph.scopes.lookup(*scope, qual).cloned();
                match binding {
                    Some(Binding::Table(table_ref)) => Some(ColumnOrigin::Concrete {
                        table: table_ref,
                        column: name.clone(),
                    }),
                    Some(Binding::Cte(cte_scope) | Binding::DerivedTable(cte_scope)) => {
                        resolve_through_scope(name, cte_scope, graph, resolved, incoming, visited)
                    }
                    None => Some(ColumnOrigin::Concrete {
                        table: TableRef::new(qual.as_str()),
                        column: name.clone(),
                    }),
                }
            } else {
                resolve_unqualified(name, *scope, graph, resolved, incoming, visited)
            }
        }

        RawNode::Unqualified { name, scope } => {
            resolve_unqualified(name, *scope, graph, resolved, incoming, visited)
        }

        RawNode::Star { table, .. } => table
            .as_ref()
            .map(|t| ColumnOrigin::Wildcard { table: t.clone() }),

        RawNode::Output { .. } => None,
    };

    resolved[node_id].clone_from(&origin);
    origin
}

fn find_cte_redirect(node_id: NodeId, graph: &RawGraph) -> Option<(NodeId, usize)> {
    match &graph.nodes[node_id] {
        RawNode::Ref {
            name,
            qualifier,
            scope,
        } => {
            let binding = if let Some(qual) = qualifier {
                graph.scopes.lookup(*scope, qual).cloned()
            } else {
                find_single_binding(*scope, graph)
            };
            match binding {
                Some(Binding::Cte(s) | Binding::DerivedTable(s)) => graph
                    .scopes
                    .output_columns(s)
                    .iter()
                    .find(|c| c.name == *name)
                    .map(|c| (c.node_id, s)),
                _ => None,
            }
        }
        RawNode::Unqualified { name, scope } => {
            let binding = find_single_binding(*scope, graph);
            match binding {
                Some(Binding::Cte(s) | Binding::DerivedTable(s)) => graph
                    .scopes
                    .output_columns(s)
                    .iter()
                    .find(|c| c.name == *name)
                    .map(|c| (c.node_id, s)),
                _ => None,
            }
        }
        _ => None,
    }
}

fn find_single_binding(scope: usize, graph: &RawGraph) -> Option<Binding> {
    let bindings = effective_bindings(scope, graph);
    if bindings.len() == 1 {
        Some(bindings[0].1.clone())
    } else {
        None
    }
}

fn resolve_unqualified(
    name: &str,
    scope: usize,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
) -> Option<ColumnOrigin> {
    resolve_from_bindings(name, &effective_bindings(scope, graph), graph, resolved, incoming, visited)
}

fn resolve_from_bindings(
    name: &str,
    bindings: &[(String, Binding)],
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
) -> Option<ColumnOrigin> {
    if bindings.len() == 1 {
        let (_, binding) = &bindings[0];
        match binding {
            Binding::Table(table_ref) => Some(ColumnOrigin::Concrete {
                table: table_ref.clone(),
                column: name.to_string(),
            }),
            Binding::Cte(cte_scope) | Binding::DerivedTable(cte_scope) => {
                resolve_through_scope(name, *cte_scope, graph, resolved, incoming, visited)
            }
        }
    } else if bindings.is_empty() {
        Some(ColumnOrigin::Concrete {
            table: TableRef::new("?unknown?"),
            column: name.to_string(),
        })
    } else {
        // Single pass: check CTE matches and collect Table candidates together
        let mut table_candidates = Vec::new();
        for (_, binding) in bindings {
            match binding {
                Binding::Cte(s) | Binding::DerivedTable(s) => {
                    if graph.scopes.output_columns(*s).iter().any(|c| c.name == name) {
                        return resolve_through_scope(name, *s, graph, resolved, incoming, visited);
                    }
                }
                Binding::Table(t) => table_candidates.push(t.clone()),
            }
        }
        if table_candidates.len() == 1 {
            Some(ColumnOrigin::Concrete {
                table: table_candidates.into_iter().next().unwrap(),
                column: name.to_string(),
            })
        } else {
            Some(ColumnOrigin::Ambiguous {
                column: name.to_string(),
                candidates: table_candidates,
            })
        }
    }
}

fn resolve_through_scope(
    column_name: &str,
    target_scope: usize,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
) -> Option<ColumnOrigin> {
    if let Some(col) = graph
        .scopes
        .output_columns(target_scope)
        .iter()
        .find(|c| c.name == column_name)
    {
        let (origins, _, has_back) =
            collect_output_sources(col.node_id, graph, resolved, incoming, visited);
        if has_back {
            Some(ColumnOrigin::Recursive {
                base_sources: origins,
            })
        } else if origins.len() == 1 {
            Some(origins.into_iter().next().unwrap())
        } else {
            // Multi-source CTE output (e.g., UNION inside CTE). Returns the first
            // origin here; the full list is collected transitively by
            // collect_output_sources when building the final ColumnMapping.
            origins.into_iter().next()
        }
    } else {
        Some(ColumnOrigin::Concrete {
            table: TableRef::new("?cte?"),
            column: column_name.to_string(),
        })
    }
}

fn derive_transform(kinds: &[EdgeKind]) -> TransformKind {
    if kinds.iter().any(|k| matches!(k, EdgeKind::ViaAggregation)) {
        TransformKind::Aggregation
    } else if kinds.iter().any(|k| matches!(k, EdgeKind::ViaConditional)) {
        TransformKind::Conditional
    } else if kinds.iter().any(|k| matches!(k, EdgeKind::ViaExpression)) {
        TransformKind::Expression
    } else {
        TransformKind::Direct
    }
}

mod catalog;
mod topo;

use std::collections::HashSet;

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

    // Validate: topological sort must succeed after removing back-edges (invariant §13.3)
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

    // Build incoming-edges index (includes back-edges so we can detect them)
    let mut incoming: Vec<Vec<usize>> = vec![vec![]; graph.nodes.len()];
    for (idx, edge) in graph.edges.iter().enumerate() {
        incoming[edge.to].push(idx);
    }

    // Memoized resolution cache for leaf nodes
    let mut resolved: Vec<Option<ColumnOrigin>> = vec![None; graph.nodes.len()];

    // Determine final output node IDs from root scope
    let root = ScopeTree::root();
    let final_ids: HashSet<NodeId> = graph
        .scopes
        .output_columns(root)
        .iter()
        .map(|c| c.node_id)
        .collect();

    // Build ColumnMappings for final Output nodes
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
                        target: ColumnRef {
                            table: output_table.clone(),
                            column: name.clone(),
                        },
                        sources: vec![ColumnOrigin::Recursive {
                            base_sources: sources,
                        }],
                        transform,
                    });
                } else {
                    mappings.push(ColumnMapping {
                        target: ColumnRef {
                            table: output_table.clone(),
                            column: name.clone(),
                        },
                        sources,
                        transform,
                    });
                }
            }
            RawNode::Star { table, .. } => {
                // Generate Wildcard mapping (catalog can expand later)
                if let Some(t) = table {
                    mappings.push(ColumnMapping {
                        target: ColumnRef {
                            table: output_table.clone(),
                            column: "*".to_string(),
                        },
                        sources: vec![ColumnOrigin::Wildcard { table: t.clone() }],
                        transform: TransformKind::Direct,
                    });
                } else {
                    // Unqualified * — expand per visible binding
                    let scope_id = if let RawNode::Star { scope, .. } = &graph.nodes[node_id] {
                        Some(*scope)
                    } else {
                        None
                    };
                    if let Some(sid) = scope_id {
                        for (_, binding) in graph.scopes.visible_bindings(sid) {
                            match binding {
                                Binding::Table(tref) => {
                                    mappings.push(ColumnMapping {
                                        target: ColumnRef {
                                            table: output_table.clone(),
                                            column: "*".to_string(),
                                        },
                                        sources: vec![ColumnOrigin::Wildcard { table: tref }],
                                        transform: TransformKind::Direct,
                                    });
                                }
                                Binding::Cte(s) | Binding::DerivedTable(s) => {
                                    for col in graph.scopes.output_columns(s) {
                                        let mut visited = HashSet::new();
                                        let (sources, edge_kinds, _) = collect_output_sources(
                                            col.node_id,
                                            &graph,
                                            &mut resolved,
                                            &incoming,
                                            &mut visited,
                                        );
                                        let transform = derive_transform(&edge_kinds);
                                        mappings.push(ColumnMapping {
                                            target: ColumnRef {
                                                table: output_table.clone(),
                                                column: col.name.clone(),
                                            },
                                            sources,
                                            transform,
                                        });
                                    }
                                }
                            }
                        }
                        // Expand anonymous (alias-less) derived tables
                        for &child in graph.scopes.anonymous_derived(sid) {
                            for col in graph.scopes.output_columns(child) {
                                let mut visited = HashSet::new();
                                let (sources, edge_kinds, _) = collect_output_sources(
                                    col.node_id,
                                    &graph,
                                    &mut resolved,
                                    &incoming,
                                    &mut visited,
                                );
                                let transform = derive_transform(&edge_kinds);
                                mappings.push(ColumnMapping {
                                    target: ColumnRef {
                                        table: output_table.clone(),
                                        column: col.name.clone(),
                                    },
                                    sources,
                                    transform,
                                });
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    // Preserve original output_column order from root scope
    let ordered_ids: Vec<NodeId> = graph
        .scopes
        .output_columns(root)
        .iter()
        .map(|c| c.node_id)
        .collect();
    mappings.sort_by_key(|m| {
        ordered_ids
            .iter()
            .position(|&id| {
                if let RawNode::Output { ref name, .. } = graph.nodes[id] {
                    *name == m.target.column
                } else {
                    false
                }
            })
            .unwrap_or(usize::MAX)
    });

    // Apply catalog if provided
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

/// Collect all leaf-level `ColumnOrigins` for an Output node, transitively following
/// through CTE/DerivedTable redirects and intermediate Output nodes.
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
        let from = edge.from;
        let (sub_sources, sub_back) =
            collect_leaf_origins(from, graph, resolved, incoming, visited);
        for _ in &sub_sources {
            kinds.push(edge.kind.clone());
        }
        sources.extend(sub_sources);
        has_back |= sub_back;
    }

    (sources, kinds, has_back)
}

/// Recursively collect leaf `ColumnOrigins` from a node. If the node is an Output
/// (e.g., CTE output), dig through its incoming edges. Otherwise resolve it.
fn collect_leaf_origins(
    node_id: NodeId,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
) -> (Vec<ColumnOrigin>, bool) {
    // Check for CTE/DerivedTable redirect first
    if let Some((target_output, _)) = find_cte_redirect(node_id, graph) {
        // Follow through the CTE output node
        let (sources, _, has_back) =
            collect_output_sources(target_output, graph, resolved, incoming, visited);
        return (sources, has_back);
    }

    if let RawNode::Output { .. } = &graph.nodes[node_id] {
        // Intermediate Output (e.g., from UNION merge) — dig through
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

/// Resolve a single node to a `ColumnOrigin` (with memoization).
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

/// Check if a Ref/Unqualified node redirects through a CTE/DerivedTable.
/// Returns (`target_output_node_id`, `cte_scope`) if so.
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

/// If a scope has exactly one binding (preferring immediate scope), return it.
fn find_single_binding(scope: usize, graph: &RawGraph) -> Option<Binding> {
    let immediate = graph.scopes.immediate_bindings(scope);
    let bindings = if immediate.is_empty() {
        graph.scopes.visible_bindings(scope)
    } else {
        immediate
    };
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
    // Prefer immediate scope bindings (FROM clause) over parent scope (CTE registrations)
    let immediate = graph.scopes.immediate_bindings(scope);
    let bindings = if immediate.is_empty() {
        graph.scopes.visible_bindings(scope)
    } else {
        immediate
    };

    resolve_from_bindings(name, &bindings, graph, resolved, incoming, visited)
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
        let candidates: Vec<TableRef> = bindings
            .iter()
            .filter_map(|(_, b)| match b {
                Binding::Table(t) => Some(t.clone()),
                _ => None,
            })
            .collect();
        if candidates.len() == 1 {
            Some(ColumnOrigin::Concrete {
                table: candidates.into_iter().next().unwrap(),
                column: name.to_string(),
            })
        } else {
            Some(ColumnOrigin::Ambiguous {
                column: name.to_string(),
                candidates,
            })
        }
    }
}

/// Resolve a column name through a CTE/DerivedTable scope's output columns.
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
        // Collect the output node's sources transitively
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
        // Column not found in CTE output — use placeholder
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

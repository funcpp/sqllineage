mod catalog;
mod topo;

use std::collections::HashSet;

use crate::graph::RawGraph;
use crate::graph::edge::EdgeKind;
use crate::graph::node::{NodeId, RawNode};
use crate::graph::scope::{Binding, OutputPlan, ScopeTree};
use crate::types::{
    AnalyzeResult, CatalogProvider, ColumnLineage, ColumnMapping, ColumnOrigin, ColumnRef,
    ParseError, StatementType, TableRef, TransformKind, Warning, WarningKind,
};

/// Resolve `RawGraph` into `AnalyzeResult`.
#[allow(clippy::if_not_else, clippy::useless_let_if_seq)]
pub(crate) fn resolve(
    mut graph: RawGraph,
    catalog: Option<&dyn CatalogProvider>,
    mut warnings: Vec<Warning>,
    statement_type: StatementType,
) -> Result<AnalyzeResult, ParseError> {
    graph.tables.inputs.sort();
    graph.tables.inputs.dedup();

    if graph.nodes.is_empty() {
        return Ok(AnalyzeResult {
            statement_type,
            tables: graph.tables,
            columns: ColumnLineage::default(),
            warnings,
        });
    }

    if topo::topological_sort(&graph.nodes, &graph.edges).is_err() {
        warnings.push(Warning {
            kind: WarningKind::UnexpectedCycle,
            location: None,
        });
        return Ok(AnalyzeResult {
            statement_type,
            tables: graph.tables,
            columns: ColumnLineage::default(),
            warnings,
        });
    }

    validate_set_arities(&graph, catalog)?;

    let mut incoming: Vec<Vec<usize>> = vec![vec![]; graph.nodes.len()];
    for (idx, edge) in graph.edges.iter().enumerate() {
        incoming[edge.to].push(idx);
    }

    let mut resolved: Vec<Option<ColumnOrigin>> = vec![None; graph.nodes.len()];

    let root = ScopeTree::root();
    let output_table = graph.tables.output.clone();
    let mappings = resolve_scope_mappings(
        root,
        &graph,
        &mut resolved,
        &incoming,
        output_table.as_ref(),
        catalog,
    );

    Ok(AnalyzeResult {
        statement_type,
        tables: graph.tables,
        columns: ColumnLineage { mappings },
        warnings,
    })
}

const SET_ARITY_ERROR_PREFIX: &str = "set operation arity mismatch";

fn validate_set_arities(
    graph: &RawGraph,
    catalog: Option<&dyn CatalogProvider>,
) -> Result<(), ParseError> {
    let mut active = HashSet::new();
    let _ = scope_arity(ScopeTree::root(), graph, catalog, &mut active)?;
    Ok(())
}

/// Return an exact output width when every star in a scope can be expanded;
/// otherwise return `None` and leave the eventual merge conservative.
fn scope_arity(
    scope: usize,
    graph: &RawGraph,
    catalog: Option<&dyn CatalogProvider>,
    active: &mut HashSet<usize>,
) -> Result<Option<usize>, ParseError> {
    if !active.insert(scope) {
        return Ok(None);
    }
    let result = match graph.scopes.output_plan(scope).clone() {
        OutputPlan::Projection => {
            let mut width = 0;
            let mut exact = true;
            for col in graph.scopes.output_columns(scope) {
                if let RawNode::Star {
                    table,
                    scope: star_scope,
                } = &graph.nodes[col.node_id]
                {
                    match star_arity(table.as_ref(), *star_scope, graph, catalog, active)? {
                        Some(star_width) => width += star_width,
                        None => exact = false,
                    }
                } else {
                    width += 1;
                }
            }
            exact.then_some(width)
        }
        OutputPlan::Delegate(child) => scope_arity(child, graph, catalog, active)?,
        OutputPlan::SetOperation { left, right, .. } => {
            let left_width = scope_arity(left, graph, catalog, active)?;
            let right_width = scope_arity(right, graph, catalog, active)?;
            match (left_width, right_width) {
                (Some(left), Some(right)) if left != right => {
                    return Err(ParseError {
                        message: format!(
                            "{SET_ARITY_ERROR_PREFIX}: left has {left} columns, right has {right} columns"
                        ),
                    });
                }
                (Some(width), Some(_)) => Some(width),
                _ => None,
            }
        }
    };
    active.remove(&scope);
    Ok(result)
}

fn star_arity(
    table: Option<&TableRef>,
    scope: usize,
    graph: &RawGraph,
    catalog: Option<&dyn CatalogProvider>,
    active: &mut HashSet<usize>,
) -> Result<Option<usize>, ParseError> {
    if let Some(table) = table {
        let binding = graph.scopes.lookup(scope, &table.table).cloned();
        return match binding {
            Some(Binding::Cte(child) | Binding::DerivedTable(child)) => {
                scope_arity(child, graph, catalog, active)
            }
            _ => Ok(catalog
                .and_then(|catalog| catalog.list_columns(table))
                .map(|columns| columns.len())),
        };
    }

    let bindings = effective_bindings(scope, graph);
    let mut width = 0;
    for (_, binding) in bindings {
        let binding_width = match binding {
            Binding::Table(table) => catalog
                .and_then(|catalog| catalog.list_columns(&table))
                .map(|columns| columns.len()),
            Binding::Cte(child) | Binding::DerivedTable(child) => {
                scope_arity(child, graph, catalog, active)?
            }
        };
        match binding_width {
            Some(binding_width) => width += binding_width,
            None => return Ok(None),
        }
    }
    for &child in graph.scopes.anonymous_derived(scope) {
        match scope_arity(child, graph, catalog, active)? {
            Some(child_width) => width += child_width,
            None => return Ok(None),
        }
    }
    Ok(Some(width))
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

/// Resolve a scope's output plan into ordered mappings. Set-operation
/// branches are resolved independently so catalog expansion happens before
/// their positional merge.
#[allow(clippy::too_many_arguments)]
fn resolve_scope_mappings(
    scope: usize,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    output_table: Option<&TableRef>,
    catalog: Option<&dyn CatalogProvider>,
) -> Vec<ColumnMapping> {
    match graph.scopes.output_plan(scope).clone() {
        OutputPlan::Projection => {
            let mut mappings = Vec::new();
            for col in graph.scopes.output_columns(scope) {
                match &graph.nodes[col.node_id] {
                    RawNode::Output { name, .. } => {
                        let mut visited = HashSet::new();
                        let (sources, edge_kinds, has_back, inherited_transform) =
                            collect_output_sources(
                                col.node_id,
                                graph,
                                resolved,
                                incoming,
                                &mut visited,
                                catalog,
                            );
                        let transform = merge_transform(
                            &derive_transform(&graph.nodes[col.node_id], &edge_kinds),
                            &inherited_transform,
                        );
                        mappings.push(ColumnMapping {
                            target: ColumnRef {
                                table: output_table.cloned(),
                                column: name.clone(),
                            },
                            sources: if has_back {
                                vec![ColumnOrigin::Recursive {
                                    base_sources: sources,
                                }]
                            } else {
                                sources
                            },
                            transform,
                        });
                    }
                    RawNode::Star { table, scope } => expand_star(
                        table.as_ref(),
                        *scope,
                        graph,
                        resolved,
                        incoming,
                        catalog,
                        output_table,
                        &mut mappings,
                        &mut HashSet::new(),
                    ),
                    _ => {}
                }
            }
            if let Some(cat) = catalog {
                catalog::apply_catalog(&mut mappings, cat);
            }
            mappings
        }
        OutputPlan::Delegate(child) => {
            resolve_scope_mappings(child, graph, resolved, incoming, output_table, catalog)
        }
        OutputPlan::SetOperation {
            left,
            right,
            recursive,
        } => {
            let left_mappings =
                resolve_scope_mappings(left, graph, resolved, incoming, output_table, catalog);
            let right_mappings = if recursive {
                Vec::new()
            } else {
                resolve_scope_mappings(right, graph, resolved, incoming, output_table, catalog)
            };
            // A wildcard without catalog metadata is a variable-width slot.
            // Positional alignment at or after it would fabricate an ordinal
            // and lose the remaining branch columns. Merge only the exact
            // prefix before the first wildcard, then retain both tails and
            // expose the Wildcard origin.
            if !recursive
                && (mappings_have_unknown_shape(&left_mappings)
                    || mappings_have_unknown_shape(&right_mappings))
            {
                return merge_unknown_shape_mappings(left_mappings, right_mappings);
            }
            // The branch resolvers expand stars independently. Preserve the
            // left branch's names and order, as SQL set operations do.
            let mut merged = Vec::with_capacity(left_mappings.len());
            for (idx, left_mapping) in left_mappings.into_iter().enumerate() {
                let mut sources = left_mapping.sources;
                let mut transform = left_mapping.transform.clone();
                if let Some(right_mapping) = right_mappings.get(idx) {
                    sources.extend(right_mapping.sources.clone());
                    transform = merge_transform(&transform, &right_mapping.transform);
                }
                if recursive {
                    merged.push(ColumnMapping {
                        target: left_mapping.target,
                        sources: vec![ColumnOrigin::Recursive {
                            base_sources: sources,
                        }],
                        transform,
                    });
                } else {
                    merged.push(ColumnMapping {
                        target: left_mapping.target,
                        sources,
                        transform,
                    });
                }
            }
            merged
        }
    }
}

fn mappings_have_unknown_shape(mappings: &[ColumnMapping]) -> bool {
    mappings.iter().any(|mapping| {
        mapping.sources.iter().any(|source| match source {
            ColumnOrigin::Wildcard { .. } => true,
            ColumnOrigin::Recursive { base_sources } => base_sources
                .iter()
                .any(|source| matches!(source, ColumnOrigin::Wildcard { .. })),
            _ => false,
        })
    })
}

fn merge_unknown_shape_mappings(
    left: Vec<ColumnMapping>,
    right: Vec<ColumnMapping>,
) -> Vec<ColumnMapping> {
    let left_barrier = first_unknown_mapping(&left).unwrap_or(left.len());
    let right_barrier = first_unknown_mapping(&right).unwrap_or(right.len());

    // A leading unknown star determines the output names for the set
    // operation. When the left side is already a merged set (and therefore
    // contains named slots contributed by an earlier operand), a right-only
    // tail would publish names that the leading operand never declared. Keep
    // the left candidates and their existing wildcard provenance; a direct
    // two-branch `SELECT * UNION SELECT a, b` still retains the right branch
    // names because there are no prior named slots to preserve.
    if left_barrier == 0 && left.len() > 1 && right_barrier == right.len() {
        return left;
    }

    let prefix_len = left_barrier.min(right_barrier);
    let left_unknown = wildcard_sources(&left);
    let right_unknown = wildcard_sources(&right);
    let mut merged = Vec::with_capacity(left.len() + right.len() - prefix_len);

    for (left_mapping, right_mapping) in left.iter().zip(right.iter()).take(prefix_len) {
        let mut sources = left_mapping.sources.clone();
        sources.extend(right_mapping.sources.clone());
        merged.push(ColumnMapping {
            target: left_mapping.target.clone(),
            sources,
            transform: merge_transform(&left_mapping.transform, &right_mapping.transform),
        });
    }
    merged.extend(
        left.into_iter()
            .skip(prefix_len)
            .map(|mapping| append_unknown_sources(mapping, &right_unknown)),
    );
    merged.extend(
        right
            .into_iter()
            .skip(prefix_len)
            .map(|mapping| append_unknown_sources(mapping, &left_unknown)),
    );
    merged
}

fn first_unknown_mapping(mappings: &[ColumnMapping]) -> Option<usize> {
    mappings
        .iter()
        .position(|mapping| mappings_have_unknown_shape(std::slice::from_ref(mapping)))
}

fn wildcard_sources(mappings: &[ColumnMapping]) -> Vec<ColumnOrigin> {
    let mut sources = Vec::new();
    for mapping in mappings {
        for source in &mapping.sources {
            match source {
                ColumnOrigin::Wildcard { .. } => sources.push(source.clone()),
                ColumnOrigin::Recursive { base_sources } => sources.extend(
                    base_sources
                        .iter()
                        .filter(|source| matches!(source, ColumnOrigin::Wildcard { .. }))
                        .cloned(),
                ),
                _ => {}
            }
        }
    }
    sources
}

fn append_unknown_sources(
    mut mapping: ColumnMapping,
    unknown_sources: &[ColumnOrigin],
) -> ColumnMapping {
    mapping.sources.extend(unknown_sources.iter().cloned());
    mapping
}

fn merge_transform(left: &TransformKind, right: &TransformKind) -> TransformKind {
    if matches!(left, TransformKind::Aggregation) || matches!(right, TransformKind::Aggregation) {
        TransformKind::Aggregation
    } else if matches!(left, TransformKind::Conditional)
        || matches!(right, TransformKind::Conditional)
    {
        TransformKind::Conditional
    } else if matches!(left, TransformKind::Expression)
        || matches!(right, TransformKind::Expression)
    {
        TransformKind::Expression
    } else if matches!(left, TransformKind::Window) || matches!(right, TransformKind::Window) {
        TransformKind::Window
    } else if matches!(left, TransformKind::Unknown) || matches!(right, TransformKind::Unknown) {
        TransformKind::Unknown
    } else {
        TransformKind::Direct
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
    catalog: Option<&dyn CatalogProvider>,
    output_table: Option<&TableRef>,
    mappings: &mut Vec<ColumnMapping>,
    visited_scopes: &mut HashSet<usize>,
) {
    if let Some(t) = table {
        let binding = graph.scopes.lookup(scope, &t.table).cloned();
        if let Some(Binding::Cte(s) | Binding::DerivedTable(s)) = binding {
            expand_scope_columns(
                s,
                graph,
                resolved,
                incoming,
                catalog,
                output_table,
                mappings,
                visited_scopes,
            );
        } else {
            mappings.push(wildcard_mapping(output_table, t.clone()));
        }
    } else {
        for (_, binding) in effective_bindings(scope, graph) {
            match binding {
                Binding::Table(tref) => mappings.push(wildcard_mapping(output_table, tref)),
                Binding::Cte(s) | Binding::DerivedTable(s) => {
                    expand_scope_columns(
                        s,
                        graph,
                        resolved,
                        incoming,
                        catalog,
                        output_table,
                        mappings,
                        visited_scopes,
                    );
                }
            }
        }
        for &child in graph.scopes.anonymous_derived(scope) {
            expand_scope_columns(
                child,
                graph,
                resolved,
                incoming,
                catalog,
                output_table,
                mappings,
                visited_scopes,
            );
        }
    }
}

/// Recursively expand a scope's output columns into `ColumnMapping`s.
#[allow(clippy::too_many_arguments)]
fn expand_scope_columns(
    scope_id: usize,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    catalog: Option<&dyn CatalogProvider>,
    output_table: Option<&TableRef>,
    mappings: &mut Vec<ColumnMapping>,
    visited_scopes: &mut HashSet<usize>,
) {
    if !visited_scopes.insert(scope_id) {
        return;
    }
    if !matches!(graph.scopes.output_plan(scope_id), OutputPlan::Projection) {
        let mut nested =
            resolve_scope_mappings(scope_id, graph, resolved, incoming, output_table, catalog);
        mappings.append(&mut nested);
        return;
    }
    for col in graph.scopes.output_columns(scope_id) {
        if let RawNode::Star { table, scope } = &graph.nodes[col.node_id] {
            expand_star(
                table.as_ref(),
                *scope,
                graph,
                resolved,
                incoming,
                catalog,
                output_table,
                mappings,
                visited_scopes,
            );
        } else {
            let mut visited = HashSet::new();
            let (sources, edge_kinds, _, inherited_transform) = collect_output_sources(
                col.node_id,
                graph,
                resolved,
                incoming,
                &mut visited,
                catalog,
            );
            let transform = merge_transform(
                &derive_transform(&graph.nodes[col.node_id], &edge_kinds),
                &inherited_transform,
            );
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

/// Collect source origins for one logical output slot, retaining both sides
/// of a set operation. Unlike the public mapping path this returns origins so
/// a later CTE/derived-table reference can continue through the slot.
fn scope_column_sources(
    scope: usize,
    index: usize,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    catalog: Option<&dyn CatalogProvider>,
) -> (Vec<ColumnOrigin>, Vec<EdgeKind>, bool, TransformKind) {
    let mappings = resolve_scope_mappings(scope, graph, resolved, incoming, None, catalog);
    let Some(mapping) = mappings.get(index) else {
        return (vec![], vec![], false, TransformKind::Direct);
    };
    let mut sources = Vec::new();
    let mut has_back = false;
    for source in &mapping.sources {
        match source {
            ColumnOrigin::Recursive { base_sources } => {
                sources.extend(base_sources.clone());
                has_back = true;
            }
            source => sources.push(source.clone()),
        }
    }
    (sources, vec![], has_back, mapping.transform.clone())
}

fn collect_output_sources(
    node_id: NodeId,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
    catalog: Option<&dyn CatalogProvider>,
) -> (Vec<ColumnOrigin>, Vec<EdgeKind>, bool, TransformKind) {
    if !visited.insert(node_id) {
        return (vec![], vec![], false, TransformKind::Direct);
    }

    let mut sources = Vec::new();
    let mut kinds = Vec::new();
    let mut has_back = false;
    let mut inherited_transform = TransformKind::Direct;

    for &edge_idx in &incoming[node_id] {
        let edge = &graph.edges[edge_idx];
        if edge.is_recursive_back_edge {
            has_back = true;
            continue;
        }
        let (sub_sources, sub_back, sub_transform) =
            collect_leaf_origins(edge.from, graph, resolved, incoming, visited, catalog);
        for _ in &sub_sources {
            kinds.push(edge.kind.clone());
        }
        sources.extend(sub_sources);
        has_back |= sub_back;
        inherited_transform = merge_transform(&inherited_transform, &sub_transform);
    }

    (sources, kinds, has_back, inherited_transform)
}

fn collect_leaf_origins(
    node_id: NodeId,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
    catalog: Option<&dyn CatalogProvider>,
) -> (Vec<ColumnOrigin>, bool, TransformKind) {
    if let Some((sources, has_back, transform)) =
        resolve_named_scope_reference(node_id, graph, resolved, incoming, catalog)
    {
        return (sources, has_back, transform);
    }

    if let Some((target_output, scope)) = find_cte_redirect(node_id, graph) {
        if let Some(index) = graph
            .scopes
            .output_columns(scope)
            .iter()
            .position(|c| c.node_id == target_output)
            && !matches!(graph.scopes.output_plan(scope), OutputPlan::Projection)
        {
            let (sources, _, has_back, transform) =
                scope_column_sources(scope, index, graph, resolved, incoming, catalog);
            return (sources, has_back, transform);
        }
        let (sources, _, has_back, transform) =
            collect_output_sources(target_output, graph, resolved, incoming, visited, catalog);
        return (sources, has_back, transform);
    }

    if let RawNode::Output { .. } = &graph.nodes[node_id] {
        let (sources, _, has_back, transform) =
            collect_output_sources(node_id, graph, resolved, incoming, visited, catalog);
        (sources, has_back, transform)
    } else {
        let origin = resolve_node(node_id, graph, resolved, incoming, visited, catalog);
        match origin {
            Some(o) => (vec![o], false, TransformKind::Direct),
            None => (vec![], false, TransformKind::Direct),
        }
    }
}

/// Resolve a named reference through a set-operation/Delegate scope using the
/// expanded mappings. Raw scope columns intentionally do not contain names
/// for individual catalog-expanded star outputs, so lookup must happen here.
fn resolve_named_scope_reference(
    node_id: NodeId,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    catalog: Option<&dyn CatalogProvider>,
) -> Option<(Vec<ColumnOrigin>, bool, TransformKind)> {
    let (name, qualifier, scope) = match &graph.nodes[node_id] {
        RawNode::Ref {
            name,
            qualifier,
            scope,
        } => (name, qualifier.as_ref(), *scope),
        RawNode::Unqualified { name, scope } => (name, None, *scope),
        _ => return None,
    };
    let binding = qualifier
        .and_then(|qualifier| graph.scopes.lookup(scope, qualifier).cloned())
        .or_else(|| find_single_binding(scope, graph));
    let Some(Binding::Cte(target_scope) | Binding::DerivedTable(target_scope)) = binding else {
        return None;
    };
    let mappings = resolve_scope_mappings(target_scope, graph, resolved, incoming, None, catalog);
    if let Some(mapping) = mappings
        .iter()
        .find(|mapping| mapping.target.column == *name)
    {
        let (sources, has_back) = flatten_mapping_sources(&mapping.sources);
        return Some((sources, has_back, mapping.transform.clone()));
    }
    // A catalog-less star has no individual named mapping. Returning its
    // wildcard origin is safer than falling through to a fabricated concrete
    // source for a named reference through the CTE/derived scope.
    let wildcard_sources = mappings
        .iter()
        .flat_map(|mapping| mapping.sources.iter())
        .filter_map(|source| match source {
            ColumnOrigin::Wildcard { .. } => Some(source.clone()),
            ColumnOrigin::Recursive { base_sources } => base_sources
                .iter()
                .find(|source| matches!(source, ColumnOrigin::Wildcard { .. }))
                .cloned(),
            _ => None,
        })
        .collect::<Vec<_>>();
    (!wildcard_sources.is_empty()).then_some((wildcard_sources, false, TransformKind::Direct))
}

fn flatten_mapping_sources(sources: &[ColumnOrigin]) -> (Vec<ColumnOrigin>, bool) {
    let mut flattened = Vec::new();
    let mut has_back = false;
    for source in sources {
        match source {
            ColumnOrigin::Recursive { base_sources } => {
                flattened.extend(base_sources.clone());
                has_back = true;
            }
            source => flattened.push(source.clone()),
        }
    }
    (flattened, has_back)
}

fn resolve_node(
    node_id: NodeId,
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
    catalog: Option<&dyn CatalogProvider>,
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
                        resolve_through_scope(
                            name, cte_scope, graph, resolved, incoming, visited, catalog,
                        )
                    }
                    None => Some(ColumnOrigin::Concrete {
                        table: TableRef::new(qual.as_str()),
                        column: name.clone(),
                    }),
                }
            } else {
                resolve_unqualified(name, *scope, graph, resolved, incoming, visited, catalog)
            }
        }

        RawNode::Unqualified { name, scope } => {
            resolve_unqualified(name, *scope, graph, resolved, incoming, visited, catalog)
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
    catalog: Option<&dyn CatalogProvider>,
) -> Option<ColumnOrigin> {
    resolve_from_bindings(
        name,
        &effective_bindings(scope, graph),
        graph,
        resolved,
        incoming,
        visited,
        catalog,
    )
}

fn resolve_from_bindings(
    name: &str,
    bindings: &[(String, Binding)],
    graph: &RawGraph,
    resolved: &mut Vec<Option<ColumnOrigin>>,
    incoming: &[Vec<usize>],
    visited: &mut HashSet<NodeId>,
    catalog: Option<&dyn CatalogProvider>,
) -> Option<ColumnOrigin> {
    if bindings.len() == 1 {
        let (_, binding) = &bindings[0];
        match binding {
            Binding::Table(table_ref) => Some(ColumnOrigin::Concrete {
                table: table_ref.clone(),
                column: name.to_string(),
            }),
            Binding::Cte(cte_scope) | Binding::DerivedTable(cte_scope) => resolve_through_scope(
                name, *cte_scope, graph, resolved, incoming, visited, catalog,
            ),
        }
    } else if bindings.is_empty() {
        Some(ColumnOrigin::Ambiguous {
            column: name.to_string(),
            candidates: Vec::new(),
        })
    } else {
        let mut table_candidates = Vec::new();
        for (_, binding) in bindings {
            match binding {
                Binding::Cte(s) | Binding::DerivedTable(s) => {
                    if graph
                        .scopes
                        .output_columns(*s)
                        .iter()
                        .any(|c| c.name == name)
                    {
                        return resolve_through_scope(
                            name, *s, graph, resolved, incoming, visited, catalog,
                        );
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
    catalog: Option<&dyn CatalogProvider>,
) -> Option<ColumnOrigin> {
    // Resolve through the same expanded output mappings used by the public
    // projection path. This is important for a qualified CTE/derived
    // reference whose name was introduced by a catalog-expanded star.
    let mappings = resolve_scope_mappings(target_scope, graph, resolved, incoming, None, catalog);
    if let Some(mapping) = mappings
        .iter()
        .find(|mapping| mapping.target.column == column_name)
    {
        let (origins, has_back) = flatten_mapping_sources(&mapping.sources);
        return if has_back {
            Some(ColumnOrigin::Recursive {
                base_sources: origins,
            })
        } else {
            origins.into_iter().next()
        };
    }
    if let Some(source) = mappings
        .iter()
        .flat_map(|mapping| mapping.sources.iter())
        .find_map(|source| match source {
            ColumnOrigin::Wildcard { .. } => Some(source.clone()),
            ColumnOrigin::Recursive { base_sources } => base_sources
                .iter()
                .find(|source| matches!(source, ColumnOrigin::Wildcard { .. }))
                .cloned(),
            _ => None,
        })
    {
        return Some(source);
    }

    if let Some(col) = graph
        .scopes
        .output_columns(target_scope)
        .iter()
        .find(|c| c.name == column_name)
    {
        let (origins, _, has_back, _) =
            collect_output_sources(col.node_id, graph, resolved, incoming, visited, catalog);
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
        Some(ColumnOrigin::Ambiguous {
            column: column_name.to_string(),
            candidates: Vec::new(),
        })
    }
}

fn derive_transform(node: &RawNode, edge_kinds: &[EdgeKind]) -> TransformKind {
    let kinds = if edge_kinds.is_empty() {
        match node {
            RawNode::Output { intrinsic_kind, .. } => std::slice::from_ref(intrinsic_kind),
            _ => edge_kinds,
        }
    } else {
        edge_kinds
    };

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

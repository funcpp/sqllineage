use sqlparser::ast::{Query, SetExpr};

use crate::build::LineageBuilder;
use crate::graph::edge::RawEdge;
use crate::graph::node::NodeId;
use crate::graph::scope::{Binding, ScopeKind};
use crate::types::WarningKind;

impl LineageBuilder {
    pub(crate) fn visit_query(&mut self, query: &Query) {
        let has_ctes = query.with.is_some();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let cte_scope = self.push_scope(ScopeKind::Cte);

                if with.recursive {
                    self.recursive_cte_name = Some(cte.alias.name.value.clone());
                }

                self.visit_query(&cte.query);

                self.recursive_cte_name = None;
                self.pop_scope();

                self.add_binding(cte.alias.name.value.clone(), Binding::Cte(cte_scope));
            }
        }

        // Push a child scope for the body when CTEs exist, so FROM bindings
        // don't mix with CTE registration bindings (avoids false ambiguity).
        if has_ctes {
            self.push_scope(ScopeKind::Root);
        }

        self.visit_set_expr(&query.body);

        if has_ctes {
            // Propagate output columns to parent (CTE-registration) scope
            let body_outputs: Vec<_> = self
                .graph
                .scopes
                .output_columns(self.current_scope)
                .to_vec();
            self.pop_scope();
            for col in body_outputs {
                self.graph.scopes.add_output_column(self.current_scope, col);
            }
        }
    }

    pub(crate) fn visit_set_expr(&mut self, body: &SetExpr) {
        match body {
            SetExpr::Select(select) => {
                self.visit_select(select);
            }
            SetExpr::SetOperation { left, right, .. } => {
                // Process left side in its own scope
                let left_scope = self.push_scope(ScopeKind::SetOperation);
                self.visit_set_expr(left);
                let left_outputs: Vec<(String, NodeId)> = self
                    .graph
                    .scopes
                    .output_columns(left_scope)
                    .iter()
                    .map(|c| (c.name.clone(), c.node_id))
                    .collect();
                self.pop_scope();

                // Process right side in its own scope
                let right_scope = self.push_scope(ScopeKind::SetOperation);
                self.visit_set_expr(right);
                let right_outputs: Vec<(String, NodeId)> = self
                    .graph
                    .scopes
                    .output_columns(right_scope)
                    .iter()
                    .map(|c| (c.name.clone(), c.node_id))
                    .collect();
                self.pop_scope();

                let is_recursive = self.recursive_cte_name.is_some();

                // Positional merge: redirect right's incoming edges to left's output nodes
                let pair_count = left_outputs.len().min(right_outputs.len());
                for i in 0..pair_count {
                    let left_out = left_outputs[i].1;
                    let right_out = right_outputs[i].1;

                    let redirected: Vec<(NodeId, _)> = self
                        .graph
                        .edges
                        .iter()
                        .filter(|e| e.to == right_out)
                        .map(|e| (e.from, e.kind.clone()))
                        .collect();

                    for (from, kind) in redirected {
                        self.graph.edges.push(RawEdge {
                            from,
                            to: left_out,
                            kind,
                            is_recursive_back_edge: is_recursive,
                        });
                    }
                }

                // Register left's outputs as this scope's output columns
                for (name, node_id) in &left_outputs {
                    self.graph.scopes.add_output_column(
                        self.current_scope,
                        crate::graph::scope::ScopeColumn {
                            name: name.clone(),
                            node_id: *node_id,
                        },
                    );
                }
            }
            SetExpr::Query(q) => {
                self.visit_query(q);
            }
            SetExpr::Values(_) => {}
            SetExpr::Update(stmt) | SetExpr::Insert(stmt) => {
                let st = self.visit_statement(stmt);
                self.inner_statement_type = Some(st);
            }
            other => {
                self.warn(WarningKind::UnhandledExpression(format!("{other:?}")));
            }
        }
    }
}

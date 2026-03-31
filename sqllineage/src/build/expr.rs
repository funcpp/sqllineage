use sqlparser::ast::{self, Expr, FunctionArguments, WindowType};

use crate::build::LineageBuilder;
use crate::build::select::split_compound;
use crate::graph::edge::EdgeKind;
use crate::graph::node::NodeId;
use crate::graph::scope::ScopeKind;
use crate::types::WarningKind;

impl LineageBuilder {
    /// Collect all ancestor (source) nodes from an expression.
    pub(crate) fn collect_ancestors(&mut self, expr: &Expr) -> Vec<NodeId> {
        match expr {
            Expr::Identifier(ident) => {
                let node = self
                    .graph
                    .add_unqualified(ident.value.clone(), self.current_scope);
                vec![node]
            }

            Expr::CompoundIdentifier(parts) => {
                let (qualifier, column) = split_compound(parts);
                let node = self
                    .graph
                    .add_ref(column, Some(qualifier), self.current_scope);
                vec![node]
            }

            Expr::Value(_) => vec![],

            Expr::BinaryOp { left, right, .. } => {
                let mut v = self.collect_ancestors(left);
                v.extend(self.collect_ancestors(right));
                v
            }

            Expr::UnaryOp { expr, .. } => self.collect_ancestors(expr),

            Expr::Function(func) => {
                let mut ancestors = Vec::new();
                if let FunctionArguments::List(list) = &func.args {
                    for arg in &list.args {
                        match arg {
                            ast::FunctionArg::Unnamed(arg_expr)
                            | ast::FunctionArg::Named { arg: arg_expr, .. }
                            | ast::FunctionArg::ExprNamed { arg: arg_expr, .. } => {
                                if let ast::FunctionArgExpr::Expr(e) = arg_expr {
                                    ancestors.extend(self.collect_ancestors(e));
                                }
                            }
                        }
                    }
                }
                if let FunctionArguments::Subquery(q) = &func.args {
                    let _sub = self.push_scope(ScopeKind::Subquery);
                    self.visit_query(q);
                    self.pop_scope();
                }

                // Window specification — PARTITION BY and ORDER BY are ancestors
                if let Some(WindowType::WindowSpec(spec)) = &func.over {
                    for expr in &spec.partition_by {
                        ancestors.extend(self.collect_ancestors(expr));
                    }
                    for order in &spec.order_by {
                        ancestors.extend(self.collect_ancestors(&order.expr));
                    }
                }

                ancestors
            }

            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                let mut v = Vec::new();
                if let Some(op) = operand {
                    v.extend(self.collect_ancestors(op));
                }
                for cw in conditions {
                    v.extend(self.collect_ancestors(&cw.condition));
                    v.extend(self.collect_ancestors(&cw.result));
                }
                if let Some(el) = else_result {
                    v.extend(self.collect_ancestors(el));
                }
                v
            }

            Expr::Cast { expr, .. } => self.collect_ancestors(expr),

            Expr::Nested(inner) => self.collect_ancestors(inner),

            Expr::Subquery(query) => {
                let _sub = self.push_scope(ScopeKind::Subquery);
                self.visit_query(query);
                self.pop_scope();
                vec![]
            }

            Expr::InSubquery { expr, subquery, .. } => {
                let v = self.collect_ancestors(expr);
                let _sub = self.push_scope(ScopeKind::Subquery);
                self.visit_query(subquery);
                self.pop_scope();
                v
            }

            Expr::Exists { subquery, .. } => {
                let _sub = self.push_scope(ScopeKind::Subquery);
                self.visit_query(subquery);
                self.pop_scope();
                vec![]
            }

            Expr::Between {
                expr, low, high, ..
            } => {
                let mut v = self.collect_ancestors(expr);
                v.extend(self.collect_ancestors(low));
                v.extend(self.collect_ancestors(high));
                v
            }

            Expr::InList { expr, list, .. } => {
                let mut v = self.collect_ancestors(expr);
                for item in list {
                    v.extend(self.collect_ancestors(item));
                }
                v
            }

            Expr::IsNull(e) | Expr::IsNotNull(e) => self.collect_ancestors(e),

            other => {
                self.warn(WarningKind::UnhandledExpression(format!("{other:?}")));
                vec![]
            }
        }
    }
}

/// Determine the `EdgeKind` for the outermost expression.
pub(crate) fn determine_edge_kind(expr: &Expr) -> EdgeKind {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => EdgeKind::Direct,
        Expr::Function(f) => {
            if f.over.is_some() {
                EdgeKind::ViaExpression // window functions → Expression (includes partition/order)
            } else if is_aggregate(&f.name.to_string()) {
                EdgeKind::ViaAggregation
            } else {
                EdgeKind::ViaExpression
            }
        }
        Expr::Case { .. } => EdgeKind::ViaConditional,
        Expr::Cast { expr, .. } | Expr::Nested(expr) => determine_edge_kind(expr),
        _ => EdgeKind::ViaExpression,
    }
}

fn is_aggregate(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "SUM"
            | "COUNT"
            | "AVG"
            | "MIN"
            | "MAX"
            | "ARRAY_AGG"
            | "STRING_AGG"
            | "GROUP_CONCAT"
            | "LISTAGG"
            | "STDDEV"
            | "VARIANCE"
            | "MEDIAN"
    )
}

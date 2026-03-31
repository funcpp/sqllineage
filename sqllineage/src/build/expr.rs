use sqlparser::ast::{self, Expr, FunctionArguments, WindowType};

use crate::build::LineageBuilder;
use crate::build::select::split_compound;
use crate::graph::edge::EdgeKind;
use crate::graph::node::NodeId;
use crate::graph::scope::ScopeKind;

impl LineageBuilder {
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

            Expr::Value(_) | Expr::TypedString { .. } | Expr::Wildcard(..) | Expr::QualifiedWildcard(..) => vec![],

            Expr::Cast { expr, .. }
            | Expr::Nested(expr)
            | Expr::UnaryOp { expr, .. }
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::IsNormalized { expr, .. }
            | Expr::Collate { expr, .. }
            | Expr::Convert { expr, .. }
            | Expr::Ceil { expr, .. }
            | Expr::Floor { expr, .. }
            | Expr::Prefixed { value: expr, .. }
            | Expr::Prior(expr)
            | Expr::OuterJoin(expr)
            | Expr::Named { expr, .. } => self.collect_ancestors(expr),

            Expr::Extract { expr, .. } => self.collect_ancestors(expr),

            Expr::Trim { expr, trim_what, trim_characters, .. } => {
                let mut v = self.collect_ancestors(expr);
                if let Some(what) = trim_what {
                    v.extend(self.collect_ancestors(what));
                }
                if let Some(chars) = trim_characters {
                    for c in chars {
                        v.extend(self.collect_ancestors(c));
                    }
                }
                v
            }

            Expr::Substring { expr, substring_from, substring_for, .. } => {
                let mut v = self.collect_ancestors(expr);
                if let Some(from) = substring_from {
                    v.extend(self.collect_ancestors(from));
                }
                if let Some(for_expr) = substring_for {
                    v.extend(self.collect_ancestors(for_expr));
                }
                v
            }

            Expr::Overlay { expr, overlay_what, overlay_from, overlay_for, .. } => {
                let mut v = self.collect_ancestors(expr);
                v.extend(self.collect_ancestors(overlay_what));
                v.extend(self.collect_ancestors(overlay_from));
                if let Some(for_expr) = overlay_for {
                    v.extend(self.collect_ancestors(for_expr));
                }
                v
            }

            Expr::Position { expr, r#in } => {
                let mut v = self.collect_ancestors(expr);
                v.extend(self.collect_ancestors(r#in));
                v
            }

            Expr::AtTimeZone { timestamp, time_zone } => {
                let mut v = self.collect_ancestors(timestamp);
                v.extend(self.collect_ancestors(time_zone));
                v
            }

            Expr::BinaryOp { left, right, .. }
            | Expr::Like { expr: left, pattern: right, .. }
            | Expr::ILike { expr: left, pattern: right, .. }
            | Expr::SimilarTo { expr: left, pattern: right, .. }
            | Expr::RLike { expr: left, pattern: right, .. }
            | Expr::IsDistinctFrom(left, right)
            | Expr::IsNotDistinctFrom(left, right) => {
                let mut v = self.collect_ancestors(left);
                v.extend(self.collect_ancestors(right));
                v
            }

            Expr::AnyOp { left, right, .. } | Expr::AllOp { left, right, .. } => {
                let mut v = self.collect_ancestors(left);
                v.extend(self.collect_ancestors(right));
                v
            }

            Expr::InUnnest { expr, array_expr, .. } => {
                let mut v = self.collect_ancestors(expr);
                v.extend(self.collect_ancestors(array_expr));
                v
            }

            Expr::MemberOf(m) => {
                let mut v = self.collect_ancestors(&m.value);
                v.extend(self.collect_ancestors(&m.array));
                v
            }

            Expr::Tuple(exprs) => {
                let mut v = Vec::new();
                for e in exprs {
                    v.extend(self.collect_ancestors(e));
                }
                v
            }

            Expr::Array(arr) => {
                let mut v = Vec::new();
                for e in &arr.elem {
                    v.extend(self.collect_ancestors(e));
                }
                v
            }

            Expr::Struct { values, .. } => {
                let mut v = Vec::new();
                for e in values {
                    v.extend(self.collect_ancestors(e));
                }
                v
            }

            Expr::Map(map) => {
                let mut v = Vec::new();
                for entry in &map.entries {
                    v.extend(self.collect_ancestors(&entry.key));
                    v.extend(self.collect_ancestors(&entry.value));
                }
                v
            }

            Expr::CompoundFieldAccess { root, .. } => self.collect_ancestors(root),
            Expr::JsonAccess { value, .. } => self.collect_ancestors(value),

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

            Expr::Case { operand, conditions, else_result, .. } => {
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

            Expr::Between { expr, low, high, .. } => {
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

            Expr::GroupingSets(_)
            | Expr::Cube(_)
            | Expr::Rollup(_)
            | Expr::Dictionary(_)
            | Expr::Interval(_)
            | Expr::Lambda(_)
            | Expr::MatchAgainst { .. } => vec![],

        }
    }
}

pub(crate) fn determine_edge_kind(expr: &Expr) -> EdgeKind {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) | Expr::Value(_) => EdgeKind::Direct,
        Expr::Function(f) => {
            if f.over.is_some() {
                EdgeKind::ViaExpression
            } else if is_aggregate(&f.name.to_string()) {
                EdgeKind::ViaAggregation
            } else {
                EdgeKind::ViaExpression
            }
        }
        Expr::Case { .. } => EdgeKind::ViaConditional,
        Expr::Cast { expr, .. } | Expr::Nested(expr) | Expr::Collate { expr, .. } => {
            determine_edge_kind(expr)
        }
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

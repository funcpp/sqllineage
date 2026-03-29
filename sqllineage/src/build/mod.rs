pub(crate) mod expr;
pub(crate) mod query;
pub(crate) mod select;
pub(crate) mod statement;

use crate::graph::scope::{Binding, ScopeId, ScopeKind, ScopeTree};
use crate::graph::RawGraph;
use crate::types::{StatementType, Warning, WarningKind};
use sqlparser::ast::Statement;

pub(crate) struct LineageBuilder {
    pub(crate) graph: RawGraph,
    pub(crate) current_scope: ScopeId,
    pub(crate) recursive_cte_name: Option<String>,
    pub(crate) warnings: Vec<Warning>,
    pub(crate) normalize_case: bool,
}

impl LineageBuilder {
    pub fn new(normalize_case: bool) -> Self {
        let graph = RawGraph::new();
        let root = ScopeTree::root();
        Self {
            graph,
            current_scope: root,
            recursive_cte_name: None,
            warnings: Vec::new(),
            normalize_case,
        }
    }

    /// Build lineage for a single statement. Returns the graph, warnings, and statement type.
    pub fn build(mut self, stmt: &Statement) -> (RawGraph, Vec<Warning>, StatementType) {
        let st = self.visit_statement(stmt);
        (self.graph, self.warnings, st)
    }

    pub(crate) fn push_scope(&mut self, kind: ScopeKind) -> ScopeId {
        let new_scope = self.graph.scopes.push(self.current_scope, kind);
        self.current_scope = new_scope;
        new_scope
    }

    pub(crate) fn pop_scope(&mut self) {
        if let Some(parent) = self.graph.scopes.parent(self.current_scope) {
            self.current_scope = parent;
        }
    }

    pub(crate) fn add_binding(&mut self, name: String, binding: Binding) {
        self.graph
            .scopes
            .add_binding(self.current_scope, name, binding);
    }

    pub(crate) fn warn(&mut self, kind: WarningKind) {
        self.warnings.push(Warning {
            kind,
            location: None,
        });
    }
}

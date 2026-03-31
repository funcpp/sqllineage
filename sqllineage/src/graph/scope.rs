use std::collections::{HashMap, HashSet};

use crate::graph::node::NodeId;
use crate::types::TableRef;

pub(crate) type ScopeId = usize;

pub(crate) struct ScopeTree {
    scopes: Vec<Scope>,
}

struct Scope {
    parent: Option<ScopeId>,
    bindings: HashMap<String, Binding>,
    anonymous_derived: Vec<ScopeId>,
    output_columns: Vec<ScopeColumn>,
}

#[derive(Debug, Clone)]
pub(crate) enum ScopeKind {
    Root,
    Cte,
    Subquery,
    DerivedTable,
    SetOperation,
}

#[derive(Debug, Clone)]
pub(crate) enum Binding {
    Table(TableRef),
    Cte(ScopeId),
    DerivedTable(ScopeId),
}

#[derive(Debug, Clone)]
pub(crate) struct ScopeColumn {
    pub name: String,
    pub node_id: NodeId,
}

impl ScopeTree {
    pub fn new() -> Self {
        Self {
            scopes: vec![Scope {
                parent: None,
                bindings: HashMap::new(),
                anonymous_derived: Vec::new(),
                output_columns: Vec::new(),
            }],
        }
    }

    pub fn root() -> ScopeId {
        0
    }

    pub fn push(&mut self, parent: ScopeId, _kind: ScopeKind) -> ScopeId {
        let id = self.scopes.len();
        self.scopes.push(Scope {
            parent: Some(parent),
            bindings: HashMap::new(),
            anonymous_derived: Vec::new(),
            output_columns: Vec::new(),
        });
        id
    }

    pub fn parent(&self, scope: ScopeId) -> Option<ScopeId> {
        self.scopes[scope].parent
    }

    pub fn add_binding(&mut self, scope: ScopeId, name: String, binding: Binding) {
        self.scopes[scope].bindings.insert(name, binding);
    }

    pub fn add_output_column(&mut self, scope: ScopeId, col: ScopeColumn) {
        self.scopes[scope].output_columns.push(col);
    }

    /// Lookup a name starting from `scope`, walking the parent chain.
    /// Returns the first match (nearest scope wins = shadowing).
    pub fn lookup(&self, scope: ScopeId, name: &str) -> Option<&Binding> {
        let s = &self.scopes[scope];
        if let Some(b) = s.bindings.get(name) {
            return Some(b);
        }
        s.parent.and_then(|p| self.lookup(p, name))
    }

    pub fn output_columns(&self, scope: ScopeId) -> &[ScopeColumn] {
        &self.scopes[scope].output_columns
    }

    pub fn add_anonymous_derived(&mut self, parent: ScopeId, child: ScopeId) {
        self.scopes[parent].anonymous_derived.push(child);
    }

    pub fn anonymous_derived(&self, scope: ScopeId) -> &[ScopeId] {
        &self.scopes[scope].anonymous_derived
    }

    /// Bindings in the immediate scope only (no parent chain walk).
    pub fn immediate_bindings(&self, scope: ScopeId) -> Vec<(String, Binding)> {
        self.scopes[scope]
            .bindings
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// All bindings visible from `scope` (nearest scope wins for shadowing).
    pub fn visible_bindings(&self, scope: ScopeId) -> Vec<(String, Binding)> {
        let mut result = Vec::new();
        let mut seen = HashSet::new();
        let mut current = Some(scope);
        while let Some(id) = current {
            for (name, binding) in &self.scopes[id].bindings {
                if seen.insert(name.clone()) {
                    result.push((name.clone(), binding.clone()));
                }
            }
            current = self.scopes[id].parent;
        }
        result
    }
}

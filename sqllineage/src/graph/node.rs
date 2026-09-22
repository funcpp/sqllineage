use crate::graph::edge::EdgeKind;
use crate::graph::scope::ScopeId;
use crate::types::TableRef;

pub(crate) type NodeId = usize;

#[derive(Debug, Clone)]
pub(crate) enum RawNode {
    /// Output column — produced by a projection or assignment.
    Output {
        name: String,
        /// What the defining expression does, independent of what it draws
        /// from. An expression that reaches no column still has one —
        /// `COUNT(*)` has no column ancestor but is still an aggregate — so
        /// this is where the classification survives when no edge carries it.
        kind: EdgeKind,
    },
    /// Named reference — alias, CTE reference, derived table column.
    Ref {
        name: String,
        qualifier: Option<String>,
        scope: ScopeId,
    },
    /// SELECT * or table.* — expandable with catalog.
    Star {
        table: Option<TableRef>,
        scope: ScopeId,
    },
    /// Unqualified column in multi-table scope.
    Unqualified { name: String, scope: ScopeId },
}

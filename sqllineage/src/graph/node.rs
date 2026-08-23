use crate::graph::edge::EdgeKind;
use crate::graph::scope::{Binding, ScopeId};
use crate::types::TableRef;

pub(crate) type NodeId = usize;

#[derive(Debug, Clone)]
pub(crate) enum RawNode {
    /// Output column — produced by a projection or assignment.
    Output {
        name: String,
        /// The edge kind the defining expression would carry to its own
        /// ancestors, kept even when it has none (e.g. `COUNT(*)` has no
        /// column ancestor but is still an aggregate). Used as a fallback
        /// classification when no ancestor edge exists to classify from.
        intrinsic_kind: EdgeKind,
    },
    /// Named reference — alias, CTE reference, derived table column.
    Ref {
        name: String,
        qualifier: Option<String>,
        scope: ScopeId,
        /// Binding captured while building a FROM expression. This prevents
        /// a later table alias from changing the meaning of a lateral
        /// dependency (for example `base, UNNEST(base.items) AS base`).
        binding: Option<Binding>,
    },
    /// SELECT * or table.* — expandable with catalog.
    Star {
        table: Option<TableRef>,
        scope: ScopeId,
    },
    /// Unqualified column in multi-table scope.
    Unqualified {
        name: String,
        scope: ScopeId,
        /// Binding visible while the expression was built. This keeps a
        /// lateral FROM dependency attached to the preceding relation even
        /// when a later range variable shadows its name.
        binding: Option<Binding>,
    },
}
